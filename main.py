import os
import json
import base64
import re
import traceback
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse
import asyncpg

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

db_pool = None

async def get_db():
    global db_pool
    if db_pool is None:
        db_pool = await asyncpg.create_pool(
            DATABASE_URL,
            statement_cache_size=0,
            min_size=1,
            max_size=7
        )
    return db_pool

@app.on_event("startup")
async def startup():
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            ALTER TABLE tasks ADD COLUMN IF NOT EXISTS priority VARCHAR(30) DEFAULT 'NORMAL';
            ALTER TABLE tasks ADD COLUMN IF NOT EXISTS deadline TIMESTAMPTZ;

            CREATE TABLE IF NOT EXISTS task_messages (
                id SERIAL PRIMARY KEY,
                task_id INT REFERENCES tasks(id) ON DELETE CASCADE,
                sender_id INT,
                sender_role VARCHAR(30),
                sender_name VARCHAR(100),
                message_type VARCHAR(20) DEFAULT 'TEXT',
                content TEXT,
                media_url TEXT,
                is_read BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

SYSTEM_PROMPT = """
Ты — операционный директор компании. Преврати поручение владельца в четкое техническое задание для Замдиректора.
Верни ТОЛЬКО чистый JSON без markdown:
{
  "title": "Краткий заголовок (до 6 слов)",
  "ai_summary": "Суть задачи в 2 предложениях",
  "definition_of_done": "1. Первый результат\\n2. Второй результат",
  "task_type": "SOLO",
  "priority": "URGENT"
}
"""

def query_gemini_direct(parts_list: list) -> dict:
    models = ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-flash"]
    for model_name in models:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
        payload = {
            "contents": [{"parts": parts_list}],
            "generationConfig": {"temperature": 0.2}
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                res_json = json.loads(resp.read().decode("utf-8"))
                raw_text = res_json["candidates"][0]["content"]["parts"][0]["text"]
                match = re.search(r'\{.*\}', raw_text, re.DOTALL)
                if match:
                    return json.loads(match.group(0))
        except Exception:
            continue

    return {
        "title": "Новое поручение",
        "ai_summary": "Поручение принято и передано Замдиректора",
        "definition_of_done": "1. Выполнить поручение в срок",
        "task_type": "SOLO",
        "priority": "URGENT"
    }

@app.get("/api/users")
async def get_users():
    pool = await get_db()
    async with pool.acquire() as conn:
        users = await conn.fetch("SELECT id, full_name, role, department FROM users WHERE is_active = TRUE ORDER BY id ASC")
        return [dict(u) for u in users]

@app.get("/api/tasks")
async def get_tasks():
    pool = await get_db()
    async with pool.acquire() as conn:
        tasks = await conn.fetch("""
            SELECT t.*, m.file_url as voice_url, u.full_name as lead_name,
                   (SELECT COUNT(*) FROM task_messages msg WHERE msg.task_id = t.id AND msg.is_read = FALSE) as unread_count
            FROM tasks t
            LEFT JOIN media_attachments m ON m.task_id = t.id AND m.attachment_type = 'VOICE_ORIGINAL'
            LEFT JOIN users u ON u.id = t.lead_user_id
            ORDER BY 
                CASE WHEN t.status = 'ARCHIVED' THEN 2 ELSE 1 END ASC,
                CASE 
                    WHEN t.priority = 'URGENT' THEN 1 
                    WHEN t.priority = 'NORMAL' THEN 2 
                    WHEN t.priority = 'FUTURE' THEN 3 
                    ELSE 4 
                END ASC,
                t.id DESC
        """)
        return [dict(t) for t in tasks]

@app.post("/api/tasks/create-voice")
async def create_task_voice(audio: UploadFile = File(...), user_id: int = Form(1)):
    try:
        audio_bytes = await audio.read()
        mime = audio.content_type or "audio/webm"
        if "octet-stream" in mime:
            mime = "audio/webm"

        b64_audio = base64.b64encode(audio_bytes).decode("utf-8")
        parts = [
            {"inlineData": {"mimeType": mime, "data": b64_audio}},
            {"text": SYSTEM_PROMPT}
        ]
        parsed = query_gemini_direct(parts)
        audio_b64 = f"data:{mime};base64," + b64_audio

        pool = await get_db()
        async with pool.acquire() as conn:
            task_id = await conn.fetchval("""
                INSERT INTO tasks (title, raw_input_text, ai_summary, definition_of_done, task_type, status, priority, created_by, is_urgent)
                VALUES ($1, $2, $3, $4, $5, 'DRAFT', $6, $7, $8)
                RETURNING id
            """, parsed.get("title", "Голосовое поручение"), "Голосовая аудиозапись Шефа", parsed.get("ai_summary", ""), parsed.get("definition_of_done", ""), parsed.get("task_type", "SOLO"), parsed.get("priority", "URGENT"), user_id, True)

            await conn.execute("""
                INSERT INTO media_attachments (task_id, sender_id, attachment_type, file_url, transcript)
                VALUES ($1, $2, 'VOICE_ORIGINAL', $3, $4)
            """, task_id, user_id, audio_b64, parsed.get("ai_summary", ""))

        return {"status": "ok", "task_id": task_id, "data": parsed}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")

@app.post("/api/tasks/create-text")
async def create_task_text(text: str = Form(...), user_id: int = Form(1)):
    try:
        parts = [{"text": f"Поручение шефа: {text}\n{SYSTEM_PROMPT}"}]
        parsed = query_gemini_direct(parts)

        pool = await get_db()
        async with pool.acquire() as conn:
            task_id = await conn.fetchval("""
                INSERT INTO tasks (title, raw_input_text, ai_summary, definition_of_done, task_type, status, priority, created_by, is_urgent)
                VALUES ($1, $2, $3, $4, $5, 'DRAFT', $6, $7, $8)
                RETURNING id
            """, parsed.get("title", text[:30]), text, parsed.get("ai_summary", text), parsed.get("definition_of_done", "1. Выполнить задачу"), "SOLO", parsed.get("priority", "URGENT"), user_id, True)

        return {"status": "ok", "task_id": task_id, "data": parsed}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")

@app.post("/api/tasks/{task_id}/assign")
async def assign_task(
    task_id: int, 
    lead_id: int = Form(...), 
    priority: str = Form("URGENT"),
    deadline: str = Form(...),
    title: str = Form(None),
    ai_summary: str = Form(None),
    definition_of_done: str = Form(None)
):
    try:
        # Корректное преобразование строки в объект datetime для PostgreSQL
        dt_deadline = None
        if deadline:
            try:
                clean_dl = deadline.replace("Z", "+00:00")
                dt_deadline = datetime.fromisoformat(clean_dl)
            except Exception:
                dt_deadline = datetime.now(timezone.utc) + timedelta(days=1)
        else:
            dt_deadline = datetime.now(timezone.utc) + timedelta(days=1)

        pool = await get_db()
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE tasks 
                SET lead_user_id = $1, 
                    priority = $2,
                    deadline = $3,
                    title = COALESCE($4, title),
                    ai_summary = COALESCE($5, ai_summary),
                    definition_of_done = COALESCE($6, definition_of_done),
                    status = 'IN_PROGRESS'
                WHERE id = $7
            """, lead_id, priority, dt_deadline, title, ai_summary, definition_of_done, task_id)

            await conn.execute("""
                INSERT INTO task_messages (task_id, sender_id, sender_role, sender_name, message_type, content)
                VALUES ($1, 2, 'DEPUTY', 'Замдиректора', 'SYSTEM', '🚀 Задача утверждена и передана в работу')
            """, task_id)

        return {"status": "ok"}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")

@app.get("/api/tasks/{task_id}/messages")
async def get_messages(task_id: int, viewer_role: str = "OWNER"):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE task_messages 
            SET is_read = TRUE 
            WHERE task_id = $1 AND sender_role != $2
        """, task_id, viewer_role)

        rows = await conn.fetch("""
            SELECT id, task_id, sender_id, sender_role, sender_name, message_type, content, media_url, is_read,
                   to_char(created_at, 'HH24:MI') as time_str
            FROM task_messages 
            WHERE task_id = $1 
            ORDER BY id ASC
        """, task_id)
        return [dict(r) for r in rows]

@app.post("/api/tasks/{task_id}/messages/text")
async def send_text_msg(task_id: int, sender_role: str = Form(...), sender_name: str = Form(...), content: str = Form(...)):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO task_messages (task_id, sender_role, sender_name, message_type, content)
            VALUES ($1, $2, $3, 'TEXT', $4)
        """, task_id, sender_role, sender_name, content)
    return {"status": "ok"}

@app.post("/api/tasks/{task_id}/messages/voice")
async def send_voice_msg(task_id: int, sender_role: str = Form(...), sender_name: str = Form(...), audio: UploadFile = File(...)):
    audio_bytes = await audio.read()
    mime = audio.content_type or "audio/webm"
    audio_b64 = f"data:{mime};base64," + base64.b64encode(audio_bytes).decode('utf-8')
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO task_messages (task_id, sender_role, sender_name, message_type, media_url, content)
            VALUES ($1, $2, $3, 'VOICE', $4, 'Голосовое сообщение')
        """, task_id, sender_role, sender_name, audio_b64)
    return {"status": "ok"}

@app.post("/api/tasks/{task_id}/messages/image")
async def send_image_msg(task_id: int, sender_role: str = Form(...), sender_name: str = Form(...), file: UploadFile = File(...)):
    file_bytes = await file.read()
    mime = file.content_type or "image/jpeg"
    file_b64 = f"data:{mime};base64," + base64.b64encode(file_bytes).decode('utf-8')
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO task_messages (task_id, sender_role, sender_name, message_type, media_url, content)
            VALUES ($1, $2, $3, 'IMAGE', $4, 'Прикрепленное фото')
        """, task_id, sender_role, sender_name, file_b64)
    return {"status": "ok"}

@app.post("/api/tasks/{task_id}/red-flag")
async def red_flag(task_id: int, reason: str = Form(...), sender_name: str = Form("Исполнитель")):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE tasks SET priority = 'URGENT', is_urgent = TRUE, risks_notes = $1 WHERE id = $2
        """, f"🚨 RED FLAG: {reason}", task_id)
        
        await conn.execute("""
            INSERT INTO task_messages (task_id, sender_role, sender_name, message_type, content)
            VALUES ($1, 'EMPLOYEE', $2, 'REDFLAG', $3)
        """, task_id, sender_name, f"🚨 RED FLAG (БЛОКЕР): {reason}")
    return {"status": "flagged"}

@app.post("/api/tasks/{task_id}/complete")
async def complete_task(task_id: int):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE tasks SET status = 'ARCHIVED', completed_at = NOW() WHERE id = $1", task_id)
        await conn.execute("""
            INSERT INTO task_messages (task_id, sender_role, sender_name, message_type, content)
            VALUES ($1, 'DEPUTY', 'Замдиректора', 'SYSTEM', '🏁 Задача успешно принята и закрыта в архив')
        """, task_id)
    return {"status": "completed"}

@app.get("/", response_class=HTMLResponse)
async def index():
    return """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <title>Task OS Core</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://unpkg.com/vue@3.4.21/dist/vue.global.prod.js"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
  <style>[v-cloak] { display: none !important; }</style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen font-sans">
  <div id="app" v-cloak class="max-w-md mx-auto p-3.5 pb-24">
    
    <!-- ХЕДЕР -->
    <header class="bg-slate-900 border border-slate-800 p-3 rounded-2xl mb-3.5 shadow-md">
      <div class="flex justify-between items-center mb-2.5">
        <span class="text-[11px] font-black tracking-wider text-slate-400">TASK OS PLATFORM</span>
        <span class="text-[10px] font-bold px-2 py-0.5 rounded bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">{{ roleTitle }}</span>
      </div>
      <div class="grid grid-cols-3 gap-1 p-1 bg-slate-950 rounded-xl border border-slate-800">
        <button @click="switchRole('OWNER')" :class="role === 'OWNER' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="py-1.5 text-xs rounded-lg transition">Шеф</button>
        <button @click="switchRole('DEPUTY')" :class="role === 'DEPUTY' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="py-1.5 text-xs rounded-lg transition">Зам</button>
        <button @click="switchRole('EMPLOYEE')" :class="role === 'EMPLOYEE' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="py-1.5 text-xs rounded-lg transition">Сотрудник</button>
      </div>
    </header>

    <!-- 1. ЭКРАН ШЕФА -->
    <div v-if="role === 'OWNER'" class="space-y-4">
      <div class="bg-slate-900 border border-slate-800 p-5 rounded-3xl text-center space-y-3.5 shadow-xl">
        <h2 class="text-base font-bold text-white">Голосовое поручение</h2>
        
        <div class="flex justify-center py-1">
          <button @click="toggleRecord" :class="isRecording ? 'bg-red-500 animate-pulse scale-105' : 'bg-indigo-600 active:scale-95'" class="w-20 h-20 rounded-full flex flex-col items-center justify-center text-white shadow-2xl transition duration-200">
            <i :class="isRecording ? 'fa-solid fa-stop text-xl' : 'fa-solid fa-microphone text-2xl'"></i>
            <span v-if="isRecording" class="text-[10px] font-mono font-bold mt-1">{{ formatTime(recordSeconds) }}</span>
          </button>
        </div>

        <div v-if="isProcessing" class="text-xs text-indigo-400 font-semibold animate-pulse">
          <i class="fa-solid fa-circle-notch fa-spin mr-1"></i> ИИ обрабатывает задачу...
        </div>
        <p v-else class="text-[11px] font-semibold" :class="isRecording ? 'text-red-400' : 'text-slate-400'">
          {{ isRecording ? 'Идет запись... Нажмите для завершения' : 'Нажмите микрофон и говорите' }}
        </p>

        <div class="pt-2.5 border-t border-slate-800 text-left space-y-1.5">
          <div class="flex gap-1.5">
            <input v-model="textInput" @keyup.enter="sendTextTask" placeholder="Или напишите поручение текстом..." class="flex-1 bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-white">
            <button @click="sendTextTask" class="bg-indigo-600 hover:bg-indigo-500 px-3 rounded-xl text-white font-bold text-xs">
              <i class="fa-solid fa-paper-plane"></i>
            </button>
          </div>
        </div>
      </div>

      <!-- ЛЕНТА ЗАДАЧ ШЕФА -->
      <div class="space-y-3">
        <h3 class="text-xs font-bold text-slate-400 uppercase px-1">Задачи (по приоритету):</h3>
        
        <div v-for="t in tasks" :key="t.id" class="bg-slate-900 border border-slate-800 rounded-2xl p-3.5 space-y-2.5 shadow">
          
          <div class="flex justify-between items-start gap-2">
            <div class="flex flex-wrap items-center gap-1.5">
              <span class="text-[10px] font-mono font-bold bg-slate-800 text-indigo-300 px-1.5 py-0.5 rounded">#{{ t.id }}</span>
              <span class="text-[10px] font-bold px-2 py-0.5 rounded" :class="priorityBadge(t.priority)">{{ priorityLabel(t.priority) }}</span>
              <span v-if="t.status === 'IN_PROGRESS'" class="text-[10px] font-mono px-2 py-0.5 rounded border" :class="getDeadlineBadge(t.deadline)">
                ⏰ {{ getDeadlineCountdown(t.deadline) }}
              </span>
            </div>
            <span class="text-[10px] font-bold px-2 py-0.5 rounded" :class="statusBadge(t.status)">{{ statusLabel(t.status) }}</span>
          </div>

          <h4 class="text-sm font-bold text-white leading-snug">{{ t.title }}</h4>

          <div v-if="t.voice_url" class="bg-slate-950 p-2 rounded-xl border border-slate-800">
            <audio :src="t.voice_url" controls class="w-full h-8"></audio>
          </div>

          <p class="text-xs text-slate-300 bg-slate-950/70 p-2.5 rounded-xl border border-slate-800"><strong>ТЗ:</strong> {{ t.ai_summary }}</p>

          <button @click="openChat(t)" class="w-full bg-slate-800 hover:bg-slate-700 text-indigo-300 font-bold py-2 rounded-xl text-xs flex items-center justify-center gap-2 border border-slate-700">
            <i class="fa-solid fa-comments"></i>
            <span>Чат по задаче</span>
            <span v-if="t.unread_count > 0" class="bg-indigo-600 text-white text-[10px] px-1.5 py-0.2 rounded-full">{{ t.unread_count }}</span>
          </button>
        </div>
      </div>
    </div>

    <!-- 2. ЭКРАН ЗАМДИРЕКТОРА -->
    <div v-if="role === 'DEPUTY'" class="space-y-4">
      <div class="flex gap-1 p-1 bg-slate-900 rounded-xl border border-slate-800">
        <button @click="deputyTab = 'inbox'" :class="deputyTab === 'inbox' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="flex-1 py-1.5 text-xs rounded-lg">Входящие ({{ inboxTasks.length }})</button>
        <button @click="deputyTab = 'active'" :class="deputyTab === 'active' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="flex-1 py-1.5 text-xs rounded-lg">В работе ({{ activeTasks.length }})</button>
        <button @click="deputyTab = 'archive'" :class="deputyTab === 'archive' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="flex-1 py-1.5 text-xs rounded-lg">Архив ({{ archiveTasks.length }})</button>
      </div>

      <div class="space-y-3">
        <div v-for="t in displayedDeputyTasks" :key="t.id" class="bg-slate-900 border border-slate-800 rounded-2xl p-3.5 space-y-3 shadow">
          
          <div class="flex justify-between items-start gap-2">
            <div class="flex flex-wrap items-center gap-1.5">
              <span class="text-[10px] font-mono font-bold bg-slate-800 text-indigo-300 px-1.5 py-0.5 rounded">#{{ t.id }}</span>
              <span class="text-[10px] font-bold px-2 py-0.5 rounded" :class="priorityBadge(t.priority)">{{ priorityLabel(t.priority) }}</span>
              <span v-if="t.status === 'IN_PROGRESS'" class="text-[10px] font-mono px-2 py-0.5 rounded border" :class="getDeadlineBadge(t.deadline)">
                ⏰ {{ getDeadlineCountdown(t.deadline) }}
              </span>
            </div>
            <span class="text-[10px] font-bold px-2 py-0.5 rounded" :class="statusBadge(t.status)">{{ statusLabel(t.status) }}</span>
          </div>

          <div v-if="t.voice_url" class="bg-slate-950 p-2 rounded-xl border border-slate-800">
            <audio :src="t.voice_url" controls class="w-full h-8"></audio>
          </div>
          <p class="text-[11px] text-amber-300/90 bg-amber-950/20 p-2 rounded-xl border border-amber-900/40">
            <strong>🗣 Исходное поручение:</strong> {{ t.raw_input_text }}
          </p>

          <!-- ФОРМА УТВЕРЖДЕНИЯ ДЛЯ ЗАМА (ВХОДЯЩИЕ) -->
          <div v-if="t.status === 'DRAFT'" class="space-y-2 pt-1">
            <input v-model="editDrafts[t.id].title" placeholder="Заголовок" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-white font-bold">
            <textarea v-model="editDrafts[t.id].ai_summary" rows="2" placeholder="Суть ТЗ" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-slate-200"></textarea>
            <textarea v-model="editDrafts[t.id].definition_of_done" rows="2" placeholder="Критерии сдачи (DoD)" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-slate-200"></textarea>

            <div class="grid grid-cols-2 gap-2 pt-1">
              <div>
                <label class="block text-[10px] font-bold text-slate-400 mb-1">Важность:</label>
                <select v-model="editDrafts[t.id].priority" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-white">
                  <option value="URGENT">🔴 Оперативно</option>
                  <option value="NORMAL">🟡 Умеренно</option>
                  <option value="FUTURE">🔵 На будущее</option>
                </select>
              </div>

              <div>
                <label class="block text-[10px] font-bold text-slate-400 mb-1">Исполнитель:</label>
                <select v-model="editDrafts[t.id].lead_id" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-white">
                  <option v-for="u in employeesOnly" :key="u.id" :value="u.id">{{ u.full_name }}</option>
                </select>
              </div>
            </div>

            <div>
              <label class="block text-[10px] font-bold text-slate-400 mb-1">Дедлайн выполнения:</label>
              <input v-model="editDrafts[t.id].deadline" type="datetime-local" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-white">
            </div>

            <button @click="assignTask(t.id)" class="w-full bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-2.5 rounded-xl text-xs shadow transition mt-1">
              🚀 Утвердить и отправить Лиду
            </button>
          </div>

          <!-- КАРТОЧКА В РАБОТЕ ДЛЯ ЗАМА -->
          <div v-if="t.status === 'IN_PROGRESS'" class="space-y-2 pt-1">
            <h4 class="text-sm font-bold text-white">{{ t.title }}</h4>
            <div class="p-2.5 bg-slate-950 rounded-xl border border-slate-800 text-xs space-y-1">
              <p class="text-slate-300"><strong>ТЗ:</strong> {{ t.ai_summary }}</p>
              <p class="text-slate-400 whitespace-pre-line"><strong>Критерии:</strong><br>{{ t.definition_of_done }}</p>
              <p class="text-indigo-300 pt-1"><strong>Лид:</strong> {{ t.lead_name }}</p>
            </div>

            <div class="grid grid-cols-2 gap-2">
              <button @click="openChat(t)" class="bg-slate-800 hover:bg-slate-700 text-indigo-300 font-bold py-2 rounded-xl text-xs flex items-center justify-center gap-1.5 border border-slate-700">
                <i class="fa-solid fa-comments"></i> Чат задачи
              </button>
              <button @click="completeTask(t.id)" class="bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-2.5 rounded-xl text-xs shadow">
                🏁 Закрыть в архив
              </button>
            </div>
          </div>

        </div>
      </div>
    </div>

    <!-- 3. ЭКРАН СОТРУДНИКА -->
    <div v-if="role === 'EMPLOYEE'" class="space-y-3.5">
      <div class="bg-indigo-950/40 border border-indigo-800/40 p-2.5 rounded-2xl text-xs text-indigo-300">
        👋 Ваши задачи в работе. Общайтесь и отчитывайтесь в чате задачи:
      </div>

      <div class="space-y-3">
        <div v-for="t in tasks.filter(x => x.status === 'IN_PROGRESS')" :key="t.id" class="bg-slate-900 border border-slate-800 rounded-2xl p-3.5 space-y-2.5 shadow">
          
          <div class="flex justify-between items-start gap-2">
            <div class="flex items-center gap-1.5">
              <span class="text-[10px] font-mono font-bold bg-slate-800 text-indigo-300 px-1.5 py-0.5 rounded">#{{ t.id }}</span>
              <span class="text-[10px] font-bold px-2 py-0.5 rounded" :class="priorityBadge(t.priority)">{{ priorityLabel(t.priority) }}</span>
            </div>
            <span class="text-[10px] font-mono px-2 py-0.5 rounded border" :class="getDeadlineBadge(t.deadline)">
              ⏰ {{ getDeadlineCountdown(t.deadline) }}
            </span>
          </div>

          <h4 class="text-sm font-bold text-white">{{ t.title }}</h4>
          <p class="text-xs text-slate-300 bg-slate-950 p-2.5 rounded-xl border border-slate-800">{{ t.ai_summary }}</p>
          <p class="text-xs text-slate-400 whitespace-pre-line px-1"><strong>Критерии приемки:</strong><br>{{ t.definition_of_done }}</p>

          <div class="grid grid-cols-2 gap-2 pt-1">
            <button @click="openChat(t)" class="bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-2 rounded-xl text-xs flex items-center justify-center gap-1.5 shadow">
              <i class="fa-solid fa-comments"></i> Открыть чат
            </button>
            <button @click="sendRedFlag(t.id)" class="bg-red-950 text-red-300 border border-red-800 hover:bg-red-900 text-xs font-bold py-2 rounded-xl flex items-center justify-center gap-1">
              🚩 Red Flag (Блокер)
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- МОДАЛЬНОЕ ОКНО ЧАТА -->
    <div v-if="activeChatTask" class="fixed inset-0 bg-slate-950/90 backdrop-blur-md z-50 flex flex-col justify-end">
      <div class="bg-slate-900 border-t border-slate-800 rounded-t-3xl h-[85vh] flex flex-col max-w-md w-full mx-auto shadow-2xl">
        
        <div class="p-3.5 border-b border-slate-800 flex justify-between items-center bg-slate-900/80">
          <div>
            <span class="text-[10px] font-bold text-indigo-400 uppercase">Чат по задаче #{{ activeChatTask.id }}</span>
            <h3 class="text-xs font-bold text-white truncate max-w-[240px]">{{ activeChatTask.title }}</h3>
          </div>
          <button @click="activeChatTask = null" class="w-8 h-8 rounded-full bg-slate-800 text-slate-400 flex items-center justify-center text-sm hover:text-white">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>

        <div ref="chatContainer" class="flex-1 overflow-y-auto p-3.5 space-y-2.5 bg-slate-950/40">
          <div v-if="chatMessages.length === 0" class="text-center text-slate-500 text-xs py-8">
            Сообщений пока нет. Напишите или надиктуйте ответ ниже!
          </div>

          <div v-for="m in chatMessages" :key="m.id" :class="m.sender_role === role ? 'justify-end' : 'justify-start'" class="flex">
            <div :class="m.sender_role === role ? 'bg-indigo-600 text-white rounded-tr-none' : (m.message_type === 'REDFLAG' ? 'bg-red-950/80 border border-red-800 text-red-200' : 'bg-slate-800 text-slate-200 rounded-tl-none')" class="max-w-[80%] rounded-2xl p-2.5 shadow-sm text-xs space-y-1">
              
              <div class="flex justify-between items-center gap-3 text-[9px] opacity-75 font-semibold">
                <span>{{ m.sender_name }} ({{ formatRoleName(m.sender_role) }})</span>
                <span>{{ m.time_str }}</span>
              </div>

              <p v-if="m.message_type === 'TEXT' || m.message_type === 'REDFLAG' || m.message_type === 'SYSTEM'" class="leading-relaxed whitespace-pre-wrap">{{ m.content }}</p>

              <div v-if="m.message_type === 'VOICE'" class="py-1">
                <audio :src="m.media_url" controls class="h-8 w-48"></audio>
              </div>

              <div v-if="m.message_type === 'IMAGE'" class="py-1">
                <img :src="m.media_url" class="rounded-lg max-h-44 object-cover">
              </div>

              <div class="text-right text-[10px] leading-none pt-0.5">
                <span v-if="m.sender_role === role" :class="m.is_read ? 'text-sky-300 font-bold' : 'opacity-60'">
                  {{ m.is_read ? '✓✓' : '✓' }}
                </span>
              </div>
            </div>
          </div>
        </div>

        <div class="p-2.5 border-t border-slate-800 bg-slate-900 space-y-2">
          <div class="flex items-center gap-1.5">
            <label class="w-9 h-9 rounded-xl bg-slate-800 text-slate-300 flex items-center justify-center cursor-pointer hover:bg-slate-700 text-sm">
              <i class="fa-solid fa-paperclip"></i>
              <input type="file" accept="image/*" @change="uploadChatImage" class="hidden">
            </label>

            <button @click="toggleChatVoice" :class="isChatRecording ? 'bg-red-500 animate-pulse text-white' : 'bg-slate-800 text-slate-300 hover:bg-slate-700'" class="w-9 h-9 rounded-xl flex items-center justify-center text-sm">
              <i :class="isChatRecording ? 'fa-solid fa-stop' : 'fa-solid fa-microphone'"></i>
            </button>

            <input v-model="chatInput" @keyup.enter="sendChatMessage" placeholder="Сообщение..." class="flex-1 bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-white">

            <button @click="sendChatMessage" class="w-9 h-9 rounded-xl bg-indigo-600 text-white flex items-center justify-center text-sm hover:bg-indigo-500">
              <i class="fa-solid fa-paper-plane"></i>
            </button>
          </div>
        </div>

      </div>
    </div>

  </div>

  <script>
    const { createApp, ref, computed, onMounted } = Vue;
    createApp({
      setup() {
        const role = ref('OWNER');
        const deputyTab = ref('inbox');
        const isRecording = ref(false);
        const isProcessing = ref(false);
        const recordSeconds = ref(0);
        const textInput = ref('');
        const editDrafts = ref({});
        let timerInterval = null;

        const tasks = ref([]);
        const users = ref([]);
        let mediaRecorder = null;
        let audioChunks = [];

        const activeChatTask = ref(null);
        const chatMessages = ref([]);
        const chatInput = ref('');
        const isChatRecording = ref(false);
        let chatRecorder = null;
        let chatAudioChunks = [];
        let chatPollInterval = null;

        const roleTitle = computed(() => {
          if (role.value === 'OWNER') return 'Шеф (Владелец)';
          if (role.value === 'DEPUTY') return 'Замдиректора';
          return 'Сотрудник (Лид)';
        });

        const currentUserName = computed(() => {
          if (role.value === 'OWNER') return 'Шеф';
          if (role.value === 'DEPUTY') return 'Замдиректора';
          return 'Азизов Рустам';
        });

        const employeesOnly = computed(() => users.value.filter(u => u.role === 'EMPLOYEE'));
        const inboxTasks = computed(() => tasks.value.filter(t => t.status === 'DRAFT'));
        const activeTasks = computed(() => tasks.value.filter(t => t.status === 'IN_PROGRESS'));
        const archiveTasks = computed(() => tasks.value.filter(t => t.status === 'ARCHIVED'));
        const displayedDeputyTasks = computed(() => {
          if (deputyTab.value === 'inbox') return inboxTasks.value;
          if (deputyTab.value === 'active') return activeTasks.value;
          return archiveTasks.value;
        });

        const switchRole = (newRole) => {
          role.value = newRole;
          loadData();
        };

        const loadData = async () => {
          try {
            const [rTasks, rUsers] = await Promise.all([
              fetch('/api/tasks').then(r => r.json()),
              fetch('/api/users').then(r => r.json())
            ]);
            tasks.value = rTasks;
            users.value = rUsers;
            
            const empList = rUsers.filter(u => u.role === 'EMPLOYEE');
            const defaultEmpId = empList.length > 0 ? empList[0].id : 1;
            const defaultDeadline = new Date(Date.now() + 86400000).toISOString().slice(0, 16);

            rTasks.forEach(t => {
              if (!editDrafts.value[t.id]) {
                editDrafts.value[t.id] = {
                  title: t.title,
                  ai_summary: t.ai_summary,
                  definition_of_done: t.definition_of_done,
                  priority: t.priority || 'URGENT',
                  lead_id: t.lead_user_id || defaultEmpId,
                  deadline: t.deadline ? t.deadline.slice(0, 16) : defaultDeadline
                };
              }
            });
          } catch (e) {
            console.error("Ошибка загрузки:", e);
          }
        };

        const toggleRecord = async () => {
          if (!isRecording.value) {
            try {
              const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
              mediaRecorder = new MediaRecorder(stream);
              audioChunks = [];
              recordSeconds.value = 0;
              timerInterval = setInterval(() => recordSeconds.value++, 1000);

              mediaRecorder.ondataavailable = e => { if (e.data.size > 0) audioChunks.push(e.data); };
              mediaRecorder.onstop = async () => {
                clearInterval(timerInterval);
                isProcessing.value = true;
                const blob = new Blob(audioChunks, { type: mediaRecorder.mimeType || 'audio/webm' });
                const fd = new FormData();
                fd.append('audio', blob, 'recording.webm');
                fd.append('user_id', 1);

                try {
                  const res = await fetch('/api/tasks/create-voice', { method: 'POST', body: fd });
                  if (!res.ok) throw new Error('Ошибка создания');
                  await loadData();
                  alert('✅ Поручение создано!');
                } catch (err) {
                  alert('❌ ' + err.message);
                } finally {
                  isProcessing.value = false;
                }
              };

              mediaRecorder.start();
              isRecording.value = true;
            } catch (err) {
              alert('Разрешите доступ к микрофону в браузере!');
            }
          } else {
            mediaRecorder.stop();
            isRecording.value = false;
          }
        };

        const sendTextTask = async () => {
          if (!textInput.value.trim()) return;
          isProcessing.value = true;
          const fd = new FormData();
          fd.append('text', textInput.value);
          fd.append('user_id', 1);
          try {
            const res = await fetch('/api/tasks/create-text', { method: 'POST', body: fd });
            if (!res.ok) throw new Error('Ошибка создания');
            textInput.value = '';
            await loadData();
            alert('✅ Задача создана!');
          } catch (e) {
            alert('❌ ' + e.message);
          } finally {
            isProcessing.value = false;
          }
        };

        const assignTask = async (id) => {
          const draft = editDrafts.value[id] || {};
          const fd = new FormData();
          fd.append('lead_id', draft.lead_id || employeesOnly.value[0]?.id || 1);
          fd.append('priority', draft.priority || 'URGENT');
          fd.append('deadline', draft.deadline || new Date(Date.now() + 86400000).toISOString());
          fd.append('title', draft.title || '');
          fd.append('ai_summary', draft.ai_summary || '');
          fd.append('definition_of_done', draft.definition_of_done || '');

          try {
            const res = await fetch(`/api/tasks/${id}/assign`, { method: 'POST', body: fd });
            if (!res.ok) throw new Error('Ошибка назначения');
            await loadData();
            deputyTab.value = 'active';
            alert('🚀 Задача утверждена и переведена в работу!');
          } catch (e) {
            alert('❌ ' + e.message);
          }
        };

        const openChat = async (task) => {
          activeChatTask.value = task;
          await loadMessages();
          if (chatPollInterval) clearInterval(chatPollInterval);
          chatPollInterval = setInterval(loadMessages, 3000);
        };

        const loadMessages = async () => {
          if (!activeChatTask.value) return;
          try {
            const res = await fetch(`/api/tasks/${activeChatTask.value.id}/messages?viewer_role=${role.value}`);
            chatMessages.value = await res.json();
          } catch (e) {
            console.error(e);
          }
        };

        const sendChatMessage = async () => {
          if (!chatInput.value.trim() || !activeChatTask.value) return;
          const fd = new FormData();
          fd.append('sender_role', role.value);
          fd.append('sender_name', currentUserName.value);
          fd.append('content', chatInput.value);
          await fetch(`/api/tasks/${activeChatTask.value.id}/messages/text`, { method: 'POST', body: fd });
          chatInput.value = '';
          await loadMessages();
        };

        const toggleChatVoice = async () => {
          if (!isChatRecording.value) {
            const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
            chatRecorder = new MediaRecorder(stream);
            chatAudioChunks = [];
            chatRecorder.ondataavailable = e => chatAudioChunks.push(e.data);
            chatRecorder.onstop = async () => {
              const blob = new Blob(chatAudioChunks, { type: chatRecorder.mimeType || 'audio/webm' });
              const fd = new FormData();
              fd.append('sender_role', role.value);
              fd.append('sender_name', currentUserName.value);
              fd.append('audio', blob, 'voice.webm');
              await fetch(`/api/tasks/${activeChatTask.value.id}/messages/voice`, { method: 'POST', body: fd });
              await loadMessages();
            };
            chatRecorder.start();
            isChatRecording.value = true;
          } else {
            chatRecorder.stop();
            isChatRecording.value = false;
          }
        };

        const uploadChatImage = async (e) => {
          const file = e.target.files[0];
          if (!file || !activeChatTask.value) return;
          const fd = new FormData();
          fd.append('sender_role', role.value);
          fd.append('sender_name', currentUserName.value);
          fd.append('file', file);
          await fetch(`/api/tasks/${activeChatTask.value.id}/messages/image`, { method: 'POST', body: fd });
          await loadMessages();
        };

        const sendRedFlag = async (id) => {
          const r = prompt('В чем причина блокера?');
          if (!r) return;
          const fd = new FormData();
          fd.append('reason', r);
          fd.append('sender_name', currentUserName.value);
          await fetch(`/api/tasks/${id}/red-flag`, { method: 'POST', body: fd });
          await loadData();
          alert('🚨 Red Flag отправлен Замдиректора!');
        };

        const completeTask = async (id) => {
          if (!confirm('Закрыть задачу в архив?')) return;
          await fetch(`/api/tasks/${id}/complete`, { method: 'POST' });
          await loadData();
        };

        const getDeadlineCountdown = (dlStr) => {
          if (!dlStr) return 'Без срока';
          const diff = new Date(dlStr) - new Date();
          if (diff <= 0) return 'Просрочено!';
          const hours = Math.floor(diff / (1000 * 60 * 60));
          const mins = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
          return `${hours}ч ${mins}м`;
        };

        const getDeadlineBadge = (dlStr) => {
          if (!dlStr) return 'bg-slate-800 text-slate-400 border-slate-700';
          const diff = new Date(dlStr) - new Date();
          if (diff <= 0) return 'bg-red-950 text-red-300 border-red-800 font-black animate-pulse';
          if (diff < 1000 * 60 * 60 * 4) return 'bg-amber-950 text-amber-300 border-amber-800 font-bold';
          return 'bg-emerald-950 text-emerald-300 border-emerald-800';
        };

        const priorityBadge = (p) => {
          if (p === 'URGENT') return 'bg-red-950 text-red-300 border border-red-800/60';
          if (p === 'NORMAL') return 'bg-amber-950 text-amber-300 border border-amber-800/60';
          return 'bg-blue-950 text-blue-300 border border-blue-800/60';
        };

        const priorityLabel = (p) => {
          if (p === 'URGENT') return '🔴 Оперативно';
          if (p === 'NORMAL') return '🟡 Умеренно';
          return '🔵 На будущее';
        };

        const statusBadge = (s) => s === 'DRAFT' ? 'bg-amber-950 text-amber-300' : (s === 'IN_PROGRESS' ? 'bg-blue-950 text-blue-300' : 'bg-emerald-950 text-emerald-300');
        const statusLabel = (s) => s === 'DRAFT' ? 'Входящие' : (s === 'IN_PROGRESS' ? 'В работе' : 'Архив');
        const formatRoleName = (r) => r === 'OWNER' ? 'Шеф' : (r === 'DEPUTY' ? 'Зам' : 'Лид');
        const formatTime = (s) => `${Math.floor(s/60).toString().padStart(2,'0')}:${(s%60).toString().padStart(2,'0')}`;

        onMounted(() => {
          loadData();
          setInterval(loadData, 4000);
        });

        return {
          role, deputyTab, isRecording, isProcessing, recordSeconds, textInput,
          tasks, users, employeesOnly, editDrafts, roleTitle, inboxTasks, activeTasks, archiveTasks,
          displayedDeputyTasks, activeChatTask, chatMessages, chatInput, isChatRecording,
          toggleRecord, sendTextTask, assignTask, openChat, sendChatMessage, toggleChatVoice, uploadChatImage,
          sendRedFlag, completeTask, switchRole, getDeadlineCountdown, getDeadlineBadge, priorityBadge, priorityLabel,
          statusBadge, statusLabel, formatRoleName, formatTime
        };
      }
    }).mount('#app');
  </script>
</body>
</html>"""

