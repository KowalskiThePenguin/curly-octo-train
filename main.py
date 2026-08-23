import os
import json
import base64
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import HTMLResponse
import asyncpg
import google.generativeai as genai

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    gemini_model = genai.GenerativeModel("gemini-1.5-flash")

db_pool = None

@app.on_event("startup")
async def startup():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)

SYSTEM_PROMPT = """
Ты — операционный директор. Пользователь прислал голосовое сообщение с задачей.
Верни ТОЛЬКО валидный JSON:
{
  "title": "Короткий заголовок (до 6 слов)",
  "ai_summary": "Суть задачи в 2 предложениях",
  "definition_of_done": "Четкие критерии сдачи (1... 2...)",
  "task_type": "SOLO" или "GROUP",
  "is_urgent": true/false
}
"""

@app.post("/api/tasks/create-voice")
async def create_task_voice(audio: UploadFile = File(...), user_id: int = Form(1)):
    audio_bytes = await audio.read()
    
    response = gemini_model.generate_content([
        {"mime_type": audio.content_type or "audio/webm", "data": audio_bytes},
        SYSTEM_PROMPT
    ], generation_config={"response_mime_type": "application/json"})
    
    parsed = json.loads(response.text)
    audio_b64 = "data:audio/webm;base64," + base64.b64encode(audio_bytes).decode('utf-8')

    async with db_pool.acquire() as conn:
        task_id = await conn.fetchval("""
            INSERT INTO tasks (title, raw_input_text, ai_summary, definition_of_done, task_type, status, created_by, is_urgent)
            VALUES ($1, $2, $3, $4, $5, 'DRAFT', $6, $7)
            RETURNING id
        """, parsed["title"], "Voice Audio", parsed["ai_summary"], parsed["definition_of_done"], parsed["task_type"], user_id, parsed["is_urgent"])

        await conn.execute("""
            INSERT INTO media_attachments (task_id, sender_id, attachment_type, file_url, transcript)
            VALUES ($1, $2, 'VOICE_ORIGINAL', $3, $4)
        """, task_id, user_id, audio_b64, parsed["ai_summary"])

    return {"status": "ok", "task_id": task_id, "data": parsed}

@app.get("/api/tasks")
async def get_tasks():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT t.*, m.file_url as voice_url, u.full_name as lead_name
            FROM tasks t
            LEFT JOIN media_attachments m ON m.task_id = t.id AND m.attachment_type = 'VOICE_ORIGINAL'
            LEFT JOIN users u ON u.id = t.lead_user_id
            ORDER BY t.id DESC
        """)
        return [dict(r) for r in rows]

@app.post("/api/tasks/{task_id}/assign")
async def assign_task(task_id: int, lead_id: int = Form(...), deadline: str = Form(...)):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE tasks 
            SET lead_user_id = $1, deadline = $2::timestamptz, status = 'IN_PROGRESS'
            WHERE id = $3
        """, lead_id, deadline, task_id)
        
        await conn.execute("""
            INSERT INTO task_checkpoints (task_id, cp_type, status)
            VALUES ($1, '30_PERCENT', 'PENDING'), ($1, '70_PERCENT', 'PENDING')
        """, task_id)
    return {"status": "assigned"}

@app.post("/api/tasks/{task_id}/approve-checkpoint")
async def approve_checkpoint(task_id: int, cp_type: str = Form(...)):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE task_checkpoints 
            SET status = 'APPROVED'
            WHERE task_id = $1 AND cp_type = $2
        """, task_id, cp_type)
    return {"status": "checkpoint_approved"}

@app.post("/api/tasks/{task_id}/complete")
async def complete_task(task_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE tasks SET status = 'ARCHIVED', completed_at = NOW() WHERE id = $1", task_id)
    return {"status": "completed"}

@app.get("/", response_class=HTMLResponse)
async def index():
    return """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>Task Core</title>
      <script src="https://cdn.tailwindcss.com"></script>
      <script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
      <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    </head>
    <body class="bg-slate-900 text-slate-100 min-h-screen">
      <div id="app" class="max-w-md mx-auto p-4 pb-20">
        
        <div class="flex justify-between items-center mb-6 bg-slate-800 p-3 rounded-2xl border border-slate-700">
          <div>
            <h1 class="text-xs text-slate-400 font-bold uppercase tracking-wider">Роль:</h1>
            <p class="text-sm font-black text-indigo-400">{{ roleName }}</p>
          </div>
          <div class="flex gap-1">
            <button @click="role = 'OWNER'" :class="role === 'OWNER' ? 'bg-indigo-600 text-white' : 'bg-slate-700 text-slate-300'" class="px-2.5 py-1 text-xs font-bold rounded-lg">Шеф</button>
            <button @click="role = 'DEPUTY'" :class="role === 'DEPUTY' ? 'bg-indigo-600 text-white' : 'bg-slate-700 text-slate-300'" class="px-2.5 py-1 text-xs font-bold rounded-lg">Зам</button>
            <button @click="role = 'EMPLOYEE'" :class="role === 'EMPLOYEE' ? 'bg-indigo-600 text-white' : 'bg-slate-700 text-slate-300'" class="px-2.5 py-1 text-xs font-bold rounded-lg">Сотрудник</button>
          </div>
        </div>

        <div v-if="role === 'OWNER'" class="space-y-6">
          <div class="bg-slate-800 border border-slate-700 p-6 rounded-3xl text-center space-y-4">
            <h2 class="text-base font-bold text-white">Надиктуйте задачу на ходу</h2>
            <p class="text-xs text-slate-400">Оригинал аудио сохранится, а ИИ составит ТЗ для Зама</p>
            
            <button @click="toggleRecord" :class="isRecording ? 'bg-red-500 animate-pulse ring-4 ring-red-500/30' : 'bg-indigo-600 hover:bg-indigo-500'" class="w-24 h-24 rounded-full flex items-center justify-center mx-auto text-white text-3xl shadow-xl transition">
              <i :class="isRecording ? 'fa-solid fa-stop' : 'fa-solid fa-microphone'"></i>
            </button>
            <p class="text-xs font-semibold" :class="isRecording ? 'text-red-400' : 'text-slate-500'">{{ isRecording ? 'Идет запись... Нажмите Стоп' : 'Нажмите для записи' }}</p>
          </div>
        </div>

        <div v-else class="space-y-4">
          <div v-for="t in tasks" :key="t.id" class="bg-slate-800 border border-slate-700 rounded-2xl p-4 space-y-3">
            <div class="flex justify-between items-start">
              <div>
                <span class="text-[10px] font-extrabold bg-slate-700 text-indigo-300 px-2 py-0.5 rounded">#{{ t.id }}</span>
                <span class="text-[10px] font-extrabold bg-purple-900/50 text-purple-300 px-2 py-0.5 rounded ml-1">{{ t.task_type }}</span>
                <h3 class="text-sm font-bold text-white mt-1">{{ t.title }}</h3>
              </div>
              <span class="text-[10px] font-bold px-2 py-1 rounded" :class="t.status === 'DRAFT' ? 'bg-amber-900/40 text-amber-300' : (t.status === 'IN_PROGRESS' ? 'bg-blue-900/40 text-blue-300' : 'bg-emerald-900/40 text-emerald-300')">{{ t.status }}</span>
            </div>

            <div v-if="t.voice_url" class="bg-slate-900 p-2.5 rounded-xl flex items-center justify-between">
              <audio :src="t.voice_url" controls class="h-8 max-w-[200px]"></audio>
              <span class="text-[10px] text-slate-400 font-bold">Оригинал Шефа</span>
            </div>

            <p class="text-xs text-slate-300 bg-slate-900/50 p-2.5 rounded-xl border border-slate-700/50"><strong>ТЗ:</strong> {{ t.ai_summary }}</p>
            <p class="text-xs text-slate-400"><strong>Критерии:</strong> {{ t.definition_of_done }}</p>

            <div v-if="role === 'DEPUTY' && t.status === 'DRAFT'" class="pt-2 border-t border-slate-700 space-y-2">
              <button @click="assignTask(t.id)" class="w-full bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-2 rounded-xl text-xs">
                🚀 Назначить Лида и запустить в работу
              </button>
            </div>

            <div v-if="role === 'DEPUTY' && t.status === 'IN_PROGRESS'" class="pt-2 border-t border-slate-700 flex gap-2">
              <button @click="approveCp(t.id, '30_PERCENT')" class="flex-1 bg-slate-700 hover:bg-slate-600 text-white font-bold py-1.5 rounded-xl text-xs">✅ 30%</button>
              <button @click="approveCp(t.id, '70_PERCENT')" class="flex-1 bg-slate-700 hover:bg-slate-600 text-white font-bold py-1.5 rounded-xl text-xs">✅ 70%</button>
              <button @click="completeTask(t.id)" class="flex-1 bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-1.5 rounded-xl text-xs">🏁 Закрыть</button>
            </div>
          </div>
        </div>

      </div>

      <script>
        const { createApp, ref, computed, onMounted } = Vue;
        createApp({
          setup() {
            const role = ref('OWNER');
            const isRecording = ref(false);
            const tasks = ref([]);
            let mediaRecorder, audioChunks = [];

            const roleName = computed(() => {
              if (role.value === 'OWNER') return 'Владелец (Шеф)';
              if (role.value === 'DEPUTY') return 'Замдиректора (Диспетчер)';
              return 'Сотрудник (Исполнитель)';
            });

            const loadTasks = async () => {
              const res = await fetch('/api/tasks');
              tasks.value = await res.json();
            };

            const toggleRecord = async () => {
              if (!isRecording.value) {
                const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
                mediaRecorder = new MediaRecorder(stream);
                audioChunks = [];
                mediaRecorder.ondataavailable = e => audioChunks.push(e.data);
                mediaRecorder.onstop = async () => {
                  const audioBlob = new Blob(audioChunks, { type: 'audio/webm' });
                  const fd = new FormData();
                  fd.append('audio', audioBlob, 'voice.webm');
                  fd.append('user_id', 1);
                  await fetch('/api/tasks/create-voice', { method: 'POST', body: fd });
                  await loadTasks();
                  alert('Задача создана и передана Заму!');
                };
                mediaRecorder.start();
                isRecording.value = true;
              } else {
                mediaRecorder.stop();
                isRecording.value = false;
              }
            };

            const assignTask = async (id) => {
              const fd = new FormData();
              fd.append('lead_id', 3);
              fd.append('deadline', new Date(Date.now() + 86400000).toISOString());
              await fetch(`/api/tasks/${id}/assign`, { method: 'POST', body: fd });
              await loadTasks();
            };

            const approveCp = async (id, type) => {
              const fd = new FormData();
              fd.append('cp_type', type);
              await fetch(`/api/tasks/${id}/approve-checkpoint`, { method: 'POST', body: fd });
              alert('Чекпоинт подтвержден!');
            };

            const completeTask = async (id) => {
              await fetch(`/api/tasks/${id}/complete`, { method: 'POST' });
              await loadTasks();
            };

            onMounted(() => {
              loadTasks();
              setInterval(loadTasks, 5000);
            });

            return { role, roleName, isRecording, tasks, toggleRecord, assignTask, approveCp, completeTask };
          }
        }).mount('#app');
      </script>
    </body>
    </html>
    """

