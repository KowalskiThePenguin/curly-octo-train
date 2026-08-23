import os
import json
import base64
import re
import traceback
import urllib.request
import urllib.error
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
            max_size=5
        )
    return db_pool

SYSTEM_PROMPT = """
Ты — операционный директор компании. Преврати поручение владельца в четкое техническое задание для Замдиректора.
Верни ТОЛЬКО чистый JSON без оформления markdown:
{
  "title": "Краткий заголовок (до 6 слов)",
  "ai_summary": "Суть задачи в 2 предложениях",
  "definition_of_done": "1. Первый результат\\n2. Второй результат",
  "task_type": "SOLO",
  "is_urgent": false
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
        except Exception as err:
            continue

    return {
        "title": "Новое поручение",
        "ai_summary": "Поручение передано в работу",
        "definition_of_done": "1. Выполнить задачу по регламенту",
        "task_type": "SOLO",
        "is_urgent": False
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
            SELECT t.*, m.file_url as voice_url, u.full_name as lead_name
            FROM tasks t
            LEFT JOIN media_attachments m ON m.task_id = t.id AND m.attachment_type = 'VOICE_ORIGINAL'
            LEFT JOIN users u ON u.id = t.lead_user_id
            ORDER BY t.id DESC
        """)
        result = []
        for t in tasks:
            td = dict(t)
            cps = await conn.fetch("SELECT * FROM task_checkpoints WHERE task_id = $1 ORDER BY id ASC", td["id"])
            td["checkpoints"] = [dict(c) for c in cps]
            result.append(td)
        return result

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
                INSERT INTO tasks (title, raw_input_text, ai_summary, definition_of_done, task_type, status, created_by, is_urgent)
                VALUES ($1, $2, $3, $4, $5, 'DRAFT', $6, $7)
                RETURNING id
            """, parsed.get("title", "Голосовое поручение"), "Голосовая аудиозапись Шефа", parsed.get("ai_summary", ""), parsed.get("definition_of_done", ""), parsed.get("task_type", "SOLO"), user_id, parsed.get("is_urgent", False))

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
                INSERT INTO tasks (title, raw_input_text, ai_summary, definition_of_done, task_type, status, created_by, is_urgent)
                VALUES ($1, $2, $3, $4, $5, 'DRAFT', $6, $7)
                RETURNING id
            """, parsed.get("title", text[:30]), text, parsed.get("ai_summary", text), parsed.get("definition_of_done", "1. Выполнить задачу"), "SOLO", user_id, False)

        return {"status": "ok", "task_id": task_id, "data": parsed}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")

@app.post("/api/tasks/{task_id}/assign")
async def assign_task(
    task_id: int, 
    lead_id: int = Form(...), 
    title: str = Form(None),
    ai_summary: str = Form(None),
    definition_of_done: str = Form(None)
):
    try:
        pool = await get_db()
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE tasks 
                SET lead_user_id = $1, 
                    title = COALESCE($2, title),
                    ai_summary = COALESCE($3, ai_summary),
                    definition_of_done = COALESCE($4, definition_of_done),
                    deadline = NOW() + INTERVAL '24 hours',
                    status = 'IN_PROGRESS'
                WHERE id = $5
            """, lead_id, title, ai_summary, definition_of_done, task_id)
            
            await conn.execute("DELETE FROM task_checkpoints WHERE task_id = $1", task_id)
            await conn.execute("""
                INSERT INTO task_checkpoints (task_id, cp_type, status)
                VALUES ($1, '30_PERCENT', 'PENDING'), ($1, '70_PERCENT', 'PENDING')
            """, task_id)
        return {"status": "ok"}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")

@app.post("/api/tasks/{task_id}/approve-checkpoint")
async def approve_checkpoint(task_id: int, cp_type: str = Form(...)):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE task_checkpoints 
            SET status = 'APPROVED'
            WHERE task_id = $1 AND cp_type = $2
        """, task_id, cp_type)
    return {"status": "checkpoint_approved"}

@app.post("/api/tasks/{task_id}/checkpoint-report")
async def submit_report(task_id: int, cp_type: str = Form(...), report_text: str = Form(...)):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE task_checkpoints 
            SET status = 'SUBMITTED', report_text = $1, submitted_at = NOW()
            WHERE task_id = $2 AND cp_type = $3
        """, report_text, task_id, cp_type)
    return {"status": "report_submitted"}

@app.post("/api/tasks/{task_id}/red-flag")
async def red_flag(task_id: int, reason: str = Form(...)):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE tasks SET risks_notes = $1, is_urgent = TRUE WHERE id = $2
        """, f"🚨 БЛОКЕР: {reason}", task_id)
    return {"status": "flagged"}

@app.post("/api/tasks/{task_id}/complete")
async def complete_task(task_id: int):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE tasks SET status = 'ARCHIVED', completed_at = NOW() WHERE id = $1", task_id)
    return {"status": "completed"}

@app.get("/", response_class=HTMLResponse)
async def index():
    return """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <title>Task Control Core</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://unpkg.com/vue@3.4.21/dist/vue.global.prod.js"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
  <style>[v-cloak] { display: none !important; }</style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen font-sans">
  <div id="app" v-cloak class="max-w-md mx-auto p-4 pb-24">
    
    <header class="bg-slate-900 border border-slate-800 p-3 rounded-2xl mb-4 shadow-md">
      <div class="flex justify-between items-center mb-2.5">
        <span class="text-[11px] font-black tracking-wider text-slate-400">TASK CORE SYSTEM</span>
        <span class="text-[10px] font-bold px-2 py-0.5 rounded bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">{{ roleTitle }}</span>
      </div>
      <div class="grid grid-cols-3 gap-1 p-1 bg-slate-950 rounded-xl border border-slate-800">
        <button @click="role = 'OWNER'" :class="role === 'OWNER' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="py-1.5 text-xs rounded-lg transition">Шеф</button>
        <button @click="role = 'DEPUTY'" :class="role === 'DEPUTY' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="py-1.5 text-xs rounded-lg transition">Зам</button>
        <button @click="role = 'EMPLOYEE'" :class="role === 'EMPLOYEE' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="py-1.5 text-xs rounded-lg transition">Сотрудник</button>
      </div>
    </header>

    <!-- 1. ЭКРАН ШЕФА -->
    <div v-if="role === 'OWNER'" class="space-y-4">
      <div class="bg-slate-900 border border-slate-800 p-6 rounded-3xl text-center space-y-4 shadow-xl">
        <h2 class="text-base font-bold text-white">Голосовое поручение</h2>
        
        <div class="flex justify-center py-2">
          <button @click="toggleRecord" :class="isRecording ? 'bg-red-500 animate-pulse scale-105' : 'bg-indigo-600 active:scale-95'" class="w-24 h-24 rounded-full flex flex-col items-center justify-center text-white shadow-2xl transition duration-200">
            <i :class="isRecording ? 'fa-solid fa-stop text-2xl' : 'fa-solid fa-microphone text-3xl'"></i>
            <span v-if="isRecording" class="text-[11px] font-mono font-bold mt-1">{{ formatTime(recordSeconds) }}</span>
          </button>
        </div>

        <div v-if="isProcessing" class="text-xs text-indigo-400 font-semibold animate-pulse">
          <i class="fa-solid fa-circle-notch fa-spin mr-1"></i> ИИ обрабатывает задачу...
        </div>
        <p v-else class="text-xs font-semibold" :class="isRecording ? 'text-red-400' : 'text-slate-400'">
          {{ isRecording ? 'Идет запись... Нажмите для отправки' : 'Нажмите микрофон и говорите' }}
        </p>

        <div class="pt-3 border-t border-slate-800 text-left space-y-2">
          <span class="text-[10px] font-bold text-slate-400 uppercase">Или введите текстом:</span>
          <div class="flex gap-2">
            <input v-model="textInput" @keyup.enter="sendTextTask" placeholder="Например: Узнать цену сахара..." class="flex-1 bg-slate-950 border border-slate-700 text-xs p-2.5 rounded-xl text-white">
            <button @click="sendTextTask" class="bg-indigo-600 hover:bg-indigo-500 px-3.5 rounded-xl text-white font-bold text-xs">
              <i class="fa-solid fa-paper-plane"></i>
            </button>
          </div>
        </div>
      </div>

      <div class="space-y-2">
        <h3 class="text-xs font-bold text-slate-400 uppercase px-1">Созданные поручения ({{ tasks.length }})</h3>
        <div v-for="t in tasks" :key="t.id" class="bg-slate-900 border border-slate-800 rounded-2xl p-4 space-y-2.5 shadow">
          <div class="flex justify-between items-start">
            <div>
              <span class="text-[10px] font-mono font-bold bg-slate-800 text-indigo-300 px-2 py-0.5 rounded">#{{ t.id }}</span>
              <h4 class="text-sm font-bold text-white mt-1">{{ t.title }}</h4>
            </div>
            <span class="text-[10px] font-bold px-2 py-1 rounded" :class="badgeClass(t.status)">{{ badgeText(t.status) }}</span>
          </div>

          <div v-if="t.voice_url" class="bg-slate-950 p-2 rounded-xl border border-slate-800">
            <audio :src="t.voice_url" controls class="w-full h-8"></audio>
          </div>

          <!-- Исходный текст -->
          <p class="text-[11px] text-amber-300/90 bg-amber-950/30 p-2 rounded-xl border border-amber-900/40">
            <strong>🗣 Исходное поручение:</strong> {{ t.raw_input_text }}
          </p>

          <p class="text-xs text-slate-300 bg-slate-950/60 p-2 rounded-xl border border-slate-800/40"><strong>ТЗ:</strong> {{ t.ai_summary }}</p>
        </div>
      </div>
    </div>

    <!-- 2. ЭКРАН ЗАМДИРЕКТОРА -->
    <div v-if="role === 'DEPUTY'" class="space-y-4">
      <div class="flex gap-1.5 p-1 bg-slate-900 rounded-xl border border-slate-800">
        <button @click="deputyTab = 'inbox'" :class="deputyTab === 'inbox' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="flex-1 py-1.5 text-xs rounded-lg">Входящие ({{ inboxTasks.length }})</button>
        <button @click="deputyTab = 'active'" :class="deputyTab === 'active' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="flex-1 py-1.5 text-xs rounded-lg">В работе ({{ activeTasks.length }})</button>
        <button @click="deputyTab = 'archive'" :class="deputyTab === 'archive' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="flex-1 py-1.5 text-xs rounded-lg">Архив ({{ archiveTasks.length }})</button>
      </div>

      <div class="space-y-3">
        <div v-for="t in displayedDeputyTasks" :key="t.id" class="bg-slate-900 border border-slate-800 rounded-2xl p-4 space-y-3 shadow">
          
          <div class="flex justify-between items-start">
            <span class="text-[10px] font-mono font-bold bg-slate-800 text-indigo-300 px-2 py-0.5 rounded">#{{ t.id }}</span>
            <span class="text-[10px] font-bold px-2 py-1 rounded" :class="badgeClass(t.status)">{{ badgeText(t.status) }}</span>
          </div>

          <!-- Исходник голоса или текста Шефа -->
          <div v-if="t.voice_url" class="bg-slate-950 p-2 rounded-xl border border-slate-800 space-y-1">
            <span class="text-[10px] text-slate-400 font-bold block">🎙 Аудио Шефа:</span>
            <audio :src="t.voice_url" controls class="w-full h-8"></audio>
          </div>
          <div class="p-2.5 bg-amber-950/20 border border-amber-900/40 rounded-xl text-xs text-amber-300/90">
            <strong>🗣 Исходный запрос Шефа:</strong> {{ t.raw_input_text }}
          </div>

          <!-- ФОРМА РЕДАКТИРОВАНИЯ ТЗ И НАЗНАЧЕНИЯ (ДЛЯ ВХОДЯЩИХ) -->
          <div v-if="t.status === 'DRAFT'" class="space-y-2.5 pt-1">
            <div>
              <label class="block text-[10px] font-bold text-slate-400 mb-1">Заголовок задачи:</label>
              <input v-model="editDrafts[t.id].title" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-white font-bold">
            </div>

            <div>
              <label class="block text-[10px] font-bold text-slate-400 mb-1">Суть задачи (ТЗ):</label>
              <textarea v-model="editDrafts[t.id].ai_summary" rows="2" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-slate-200"></textarea>
            </div>

            <div>
              <label class="block text-[10px] font-bold text-slate-400 mb-1">Критерии сдачи (DoD):</label>
              <textarea v-model="editDrafts[t.id].definition_of_done" rows="2" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-slate-200"></textarea>
            </div>

            <div class="pt-2 border-t border-slate-800 space-y-2">
              <label class="block text-[11px] font-bold text-indigo-300">Назначить сотрудника:</label>
              <select v-model="editDrafts[t.id].lead_id" class="w-full bg-slate-950 border border-slate-700 text-xs p-2.5 rounded-xl text-white">
                <option v-for="u in employeesOnly" :key="u.id" :value="u.id">{{ u.full_name }} ({{ u.department }})</option>
              </select>

              <button @click="assignTask(t.id)" class="w-full bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-2.5 rounded-xl text-xs shadow transition">
                🚀 Отправить Лиду в работу
              </button>
            </div>
          </div>

          <!-- БЛОК КОНТРОЛЯ В РАБОТЕ -->
          <div v-if="t.status === 'IN_PROGRESS'" class="space-y-2.5 pt-1">
            <h4 class="text-sm font-bold text-white">{{ t.title }}</h4>
            <div class="p-2.5 bg-slate-950 rounded-xl border border-slate-800 text-xs space-y-1">
              <p class="text-slate-300"><strong>ТЗ:</strong> {{ t.ai_summary }}</p>
              <p class="text-slate-400 whitespace-pre-line"><strong>Критерии:</strong><br>{{ t.definition_of_done }}</p>
              <p class="text-indigo-300 pt-1"><strong>Исполнитель:</strong> {{ t.lead_name }}</p>
            </div>

            <div class="grid grid-cols-2 gap-2 pt-1">
              <button @click="approveCp(t.id, '30_PERCENT')" class="bg-slate-800 hover:bg-slate-700 border border-slate-700 text-xs font-bold py-2 rounded-xl">✅ Подтвердить 30%</button>
              <button @click="approveCp(t.id, '70_PERCENT')" class="bg-slate-800 hover:bg-slate-700 border border-slate-700 text-xs font-bold py-2 rounded-xl">✅ Подтвердить 70%</button>
            </div>

            <button @click="completeTask(t.id)" class="w-full bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-2.5 rounded-xl text-xs shadow">
              🏁 Принять и закрыть в архив
            </button>
          </div>

        </div>
      </div>
    </div>

    <!-- 3. ЭКРАН СОТРУДНИКА -->
    <div v-if="role === 'EMPLOYEE'" class="space-y-4">
      <div class="bg-indigo-950/40 border border-indigo-800/40 p-3 rounded-2xl text-xs text-indigo-300">
        👋 Задачи, находящиеся у вас в работе:
      </div>

      <div class="space-y-3">
        <div v-for="t in tasks.filter(x => x.status === 'IN_PROGRESS')" :key="t.id" class="bg-slate-900 border border-slate-800 rounded-2xl p-4 space-y-2.5 shadow">
          <div class="flex justify-between items-start">
            <span class="text-[10px] font-mono font-bold bg-slate-800 text-indigo-300 px-2 py-0.5 rounded">#{{ t.id }}</span>
            <span class="text-[10px] font-bold px-2 py-0.5 rounded bg-blue-950 text-blue-300">В работе</span>
          </div>
          <h4 class="text-sm font-bold text-white">{{ t.title }}</h4>
          <p class="text-xs text-slate-300 bg-slate-950 p-2.5 rounded-xl border border-slate-800">{{ t.ai_summary }}</p>
          <p class="text-xs text-slate-400 whitespace-pre-line px-1"><strong>Критерии приемки:</strong><br>{{ t.definition_of_done }}</p>

          <div class="grid grid-cols-2 gap-2 pt-2 border-t border-slate-800">
            <button @click="sendReport(t.id, '30_PERCENT')" class="bg-slate-800 border border-slate-700 text-xs font-bold py-2 rounded-xl">📝 Сдать 30%</button>
            <button @click="sendReport(t.id, '70_PERCENT')" class="bg-slate-800 border border-slate-700 text-xs font-bold py-2 rounded-xl">📝 Сдать 70%</button>
          </div>
          <button @click="sendRedFlag(t.id)" class="w-full bg-red-950 text-red-300 border border-red-800 text-xs font-bold py-2 rounded-xl">🚩 Red Flag (Блокер)</button>
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

        const roleTitle = computed(() => {
          if (role.value === 'OWNER') return 'Шеф (Владелец)';
          if (role.value === 'DEPUTY') return 'Замдиректора';
          return 'Сотрудник (Лид)';
        });

        // Только сотрудники (без Шефа и Зама)
        const employeesOnly = computed(() => users.value.filter(u => u.role === 'EMPLOYEE'));

        const inboxTasks = computed(() => tasks.value.filter(t => t.status === 'DRAFT'));
        const activeTasks = computed(() => tasks.value.filter(t => t.status === 'IN_PROGRESS'));
        const archiveTasks = computed(() => tasks.value.filter(t => t.status === 'ARCHIVED'));
        const displayedDeputyTasks = computed(() => {
          if (deputyTab.value === 'inbox') return inboxTasks.value;
          if (deputyTab.value === 'active') return activeTasks.value;
          return archiveTasks.value;
        });

        const loadData = async () => {
          try {
            const [rTasks, rUsers] = await Promise.all([
              fetch('/api/tasks').then(r => r.json()),
              fetch('/api/users').then(r => r.json())
            ]);
            tasks.value = rTasks;
            users.value = rUsers;
            
            // Инициализация редактируемых полей
            const empList = rUsers.filter(u => u.role === 'EMPLOYEE');
            const defaultEmpId = empList.length > 0 ? empList[0].id : (rUsers[0]?.id || 1);

            rTasks.forEach(t => {
              if (!editDrafts.value[t.id]) {
                editDrafts.value[t.id] = {
                  title: t.title,
                  ai_summary: t.ai_summary,
                  definition_of_done: t.definition_of_done,
                  lead_id: defaultEmpId
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
                  if (!res.ok) throw new Error('Ошибка сервера');
                  await loadData();
                  alert('✅ Поручение создано и передано Заму!');
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
          fd.append('title', draft.title || '');
          fd.append('ai_summary', draft.ai_summary || '');
          fd.append('definition_of_done', draft.definition_of_done || '');

          try {
            const res = await fetch(`/api/tasks/${id}/assign`, { method: 'POST', body: fd });
            if (!res.ok) throw new Error('Ошибка назначения');
            await loadData();
            // Автоматически переключаем на вкладку "В работе"
            deputyTab.value = 'active';
            alert('🚀 Задача утверждена и переведена в работу!');
          } catch (e) {
            alert('❌ ' + e.message);
          }
        };

        const approveCp = async (id, type) => {
          const fd = new FormData();
          fd.append('cp_type', type);
          await fetch(`/api/tasks/${id}/approve-checkpoint`, { method: 'POST', body: fd });
          await loadData();
          alert('Чекпоинт подтвержден!');
        };

        const sendReport = async (id, type) => {
          const text = prompt('Что сделано по этапу?');
          if (!text) return;
          const fd = new FormData();
          fd.append('cp_type', type);
          fd.append('report_text', text);
          await fetch(`/api/tasks/${id}/checkpoint-report`, { method: 'POST', body: fd });
          await loadData();
          alert('Отчет отправлен!');
        };

        const sendRedFlag = async (id) => {
          const r = prompt('Опишите проблему:');
          if (!r) return;
          const fd = new FormData();
          fd.append('reason', r);
          await fetch(`/api/tasks/${id}/red-flag`, { method: 'POST', body: fd });
          await loadData();
          alert('🚨 Блокер зафиксирован!');
        };

        const completeTask = async (id) => {
          if (!confirm('Закрыть задачу в архив?')) return;
          await fetch(`/api/tasks/${id}/complete`, { method: 'POST' });
          await loadData();
        };

        const formatTime = (s) => `${Math.floor(s/60).toString().padStart(2,'0')}:${(s%60).toString().padStart(2,'0')}`;
        const badgeClass = (s) => s === 'DRAFT' ? 'bg-amber-950 text-amber-300' : (s === 'IN_PROGRESS' ? 'bg-blue-950 text-blue-300' : 'bg-emerald-950 text-emerald-300');
        const badgeText = (s) => s === 'DRAFT' ? 'Входящие' : (s === 'IN_PROGRESS' ? 'В работе' : 'Архив');

        onMounted(() => {
          loadData();
          setInterval(loadData, 4000);
        });

        return {
          role, deputyTab, isRecording, isProcessing, recordSeconds, textInput,
          tasks, users, employeesOnly, editDrafts, roleTitle, inboxTasks, activeTasks, archiveTasks,
          displayedDeputyTasks, toggleRecord, sendTextTask, assignTask, approveCp,
          sendReport, sendRedFlag, completeTask, formatTime, badgeClass, badgeText
        };
      }
    }).mount('#app');
  </script>
</body>
</html>"""

