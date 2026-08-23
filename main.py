import os
import json
import base64
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
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
Ты — операционный директор компании. Владелец надиктовал задачу голосом.
Преврати эту речь в четкое техническое задание для Замдиректора.

Верни ТОЛЬКО валидный JSON:
{
  "title": "Краткий заголовок (до 6 слов)",
  "ai_summary": "Суть задачи в 2 предложениях",
  "definition_of_done": "1. Первый пункт\\n2. Второй пункт",
  "task_type": "SOLO" или "GROUP",
  "is_urgent": true/false
}
"""

@app.get("/api/users")
async def get_users():
    async with db_pool.acquire() as conn:
        users = await conn.fetch("SELECT id, full_name, role, department FROM users WHERE is_active = TRUE ORDER BY id ASC")
        return [dict(u) for u in users]

@app.get("/api/tasks")
async def get_tasks():
    async with db_pool.acquire() as conn:
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
            # Загружаем чекпоинты
            cps = await conn.fetch("SELECT * FROM task_checkpoints WHERE task_id = $1 ORDER BY id ASC", td["id"])
            td["checkpoints"] = [dict(c) for c in cps]
            result.append(td)
        return result

@app.post("/api/tasks/create-voice")
async def create_task_voice(audio: UploadFile = File(...), user_id: int = Form(1)):
    audio_bytes = await audio.read()
    
    # ИИ анализ через Gemini
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
        """, parsed["title"], "Voice message", parsed["ai_summary"], parsed["definition_of_done"], parsed["task_type"], user_id, parsed.get("is_urgent", False))

        await conn.execute("""
            INSERT INTO media_attachments (task_id, sender_id, attachment_type, file_url, transcript)
            VALUES ($1, $2, 'VOICE_ORIGINAL', $3, $4)
        """, task_id, user_id, audio_b64, parsed["ai_summary"])

    return {"status": "ok", "task_id": task_id, "data": parsed}

@app.post("/api/tasks/{task_id}/assign")
async def assign_task(
    task_id: int, 
    lead_id: int = Form(...), 
    deadline: str = Form(...),
    title: str = Form(None),
    dod: str = Form(None)
):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE tasks 
            SET lead_user_id = $1, 
                deadline = $2::timestamptz, 
                title = COALESCE($3, title),
                definition_of_done = COALESCE($4, definition_of_done),
                status = 'IN_PROGRESS'
            WHERE id = $5
        """, lead_id, deadline, title, dod, task_id)
        
        # Создаем чекпоинты 30% и 70%
        await conn.execute("DELETE FROM task_checkpoints WHERE task_id = $1", task_id)
        await conn.execute("""
            INSERT INTO task_checkpoints (task_id, cp_type, status)
            VALUES ($1, '30_PERCENT', 'PENDING'), ($1, '70_PERCENT', 'PENDING')
        """, task_id)
    return {"status": "assigned"}

@app.post("/api/tasks/{task_id}/checkpoint-report")
async def submit_checkpoint_report(task_id: int, cp_type: str = Form(...), report_text: str = Form(...)):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE task_checkpoints 
            SET status = 'SUBMITTED', report_text = $1, submitted_at = NOW()
            WHERE task_id = $2 AND cp_type = $3
        """, report_text, task_id, cp_type)
    return {"status": "report_submitted"}

@app.post("/api/tasks/{task_id}/approve-checkpoint")
async def approve_checkpoint(task_id: int, cp_type: str = Form(...)):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE task_checkpoints 
            SET status = 'APPROVED'
            WHERE task_id = $1 AND cp_type = $2
        """, task_id, cp_type)
    return {"status": "checkpoint_approved"}

@app.post("/api/tasks/{task_id}/red-flag")
async def red_flag(task_id: int, reason: str = Form(...)):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE tasks SET risks_notes = $1, is_urgent = TRUE WHERE id = $2
        """, f"🚨 RED FLAG: {reason}", task_id)
    return {"status": "flagged"}

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
      <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
      <title>Task Core OS</title>
      <script src="https://cdn.tailwindcss.com"></script>
      <script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
      <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
      <style>
        [v-cloak] { display: none; }
        .tab-btn { transition: all 0.2s ease; }
      </style>
    </head>
    <body class="bg-slate-950 text-slate-100 min-h-screen font-sans antialiased">
      <div id="app" v-cloak class="max-w-md mx-auto p-4 pb-24">
        
        <!-- HEADER: ПЕРЕКЛЮЧАТЕЛЬ РОЛЕЙ -->
        <header class="bg-slate-900 border border-slate-800 p-3 rounded-2xl mb-4 shadow-lg">
          <div class="flex justify-between items-center mb-2.5">
            <div class="flex items-center gap-2">
              <div class="w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse"></div>
              <span class="text-[11px] font-black uppercase tracking-wider text-slate-400">Task Control Core</span>
            </div>
            <span class="text-[10px] font-bold px-2 py-0.5 rounded bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">{{ currentRoleLabel }}</span>
          </div>

          <div class="grid grid-cols-3 gap-1.5 p-1 bg-slate-950 rounded-xl border border-slate-800/80">
            <button @click="role = 'OWNER'" :class="role === 'OWNER' ? 'bg-indigo-600 text-white font-bold shadow' : 'text-slate-400 font-medium'" class="tab-btn py-1.5 text-xs rounded-lg text-center">Шеф</button>
            <button @click="role = 'DEPUTY'" :class="role === 'DEPUTY' ? 'bg-indigo-600 text-white font-bold shadow' : 'text-slate-400 font-medium'" class="tab-btn py-1.5 text-xs rounded-lg text-center">Зам</button>
            <button @click="role = 'EMPLOYEE'" :class="role === 'EMPLOYEE' ? 'bg-indigo-600 text-white font-bold shadow' : 'text-slate-400 font-medium'" class="tab-btn py-1.5 text-xs rounded-lg text-center">Сотрудник</button>
          </div>
        </header>

        <!-- ========================================== -->
        <!-- 1. ЭКРАН ВЛАДЕЛЬЦА (ШЕФ) -->
        <!-- ========================================== -->
        <div v-if="role === 'OWNER'" class="space-y-4">
          
          <!-- Карточка записи голоса -->
          <div class="bg-gradient-to-b from-slate-900 to-slate-900/90 border border-slate-800 p-6 rounded-3xl text-center shadow-xl space-y-4">
            <div>
              <h2 class="text-base font-bold text-white">Надиктуйте поручение</h2>
              <p class="text-xs text-slate-400 mt-1">Оригинал аудио сохранится, а ИИ сформирует ТЗ</p>
            </div>

            <!-- Большая кнопка диктофона -->
            <div class="relative flex items-center justify-center py-2">
              <div v-if="isRecording" class="absolute w-28 h-28 bg-red-500/20 rounded-full animate-ping"></div>
              <button @click="toggleRecord" :class="isRecording ? 'bg-red-500 scale-105' : 'bg-indigo-600 hover:bg-indigo-500 active:scale-95'" class="w-24 h-24 rounded-full flex flex-col items-center justify-center text-white text-2xl shadow-2xl transition duration-300 z-10">
                <i :class="isRecording ? 'fa-solid fa-stop text-2xl' : 'fa-solid fa-microphone text-3xl'"></i>
                <span v-if="isRecording" class="text-[11px] font-mono font-bold mt-1">{{ formatTime(recordSeconds) }}</span>
              </button>
            </div>

            <!-- Статусы обработки -->
            <div v-if="isProcessing" class="flex items-center justify-center gap-2 text-xs text-indigo-400 font-semibold py-1">
              <i class="fa-solid fa-circle-notch fa-spin"></i>
              <span>ИИ расшифровывает и формирует ТЗ...</span>
            </div>
            <p v-else class="text-xs font-semibold" :class="isRecording ? 'text-red-400' : 'text-slate-500'">
              {{ isRecording ? 'Идет запись... Нажмите кнопку для отправки' : 'Нажмите на микрофон и говорите' }}
            </p>
          </div>

          <!-- Живая лента задач для Шефа -->
          <div class="space-y-2.5">
            <h3 class="text-xs font-bold text-slate-400 uppercase tracking-wider px-1">Ваши поручения ({{ tasks.length }})</h3>
            
            <div v-if="tasks.length === 0" class="p-6 text-center text-slate-500 text-xs bg-slate-900/50 rounded-2xl border border-slate-800/50">
              Пока нет созданных поручений. Нажмите микрофон выше!
            </div>

            <div v-for="t in tasks" :key="t.id" class="bg-slate-900 border border-slate-800/80 rounded-2xl p-4 space-y-2.5 shadow-sm">
              <div class="flex justify-between items-start gap-2">
                <div>
                  <div class="flex items-center gap-1.5 mb-1">
                    <span class="text-[10px] font-mono font-bold bg-slate-800 text-indigo-300 px-2 py-0.5 rounded">#{{ t.id }}</span>
                    <span class="text-[10px] font-bold px-2 py-0.5 rounded" :class="t.task_type === 'GROUP' ? 'bg-purple-950 text-purple-300 border border-purple-800/50' : 'bg-blue-950 text-blue-300 border border-blue-800/50'">{{ t.task_type }}</span>
                    <span v-if="t.is_urgent" class="text-[10px] font-bold px-1.5 py-0.5 rounded bg-red-950 text-red-300 border border-red-800/50">🔥 Срочно</span>
                  </div>
                  <h4 class="text-sm font-bold text-white leading-snug">{{ t.title }}</h4>
                </div>
                <span class="text-[10px] font-bold px-2 py-1 rounded shrink-0" :class="getStatusBadge(t.status)">{{ getStatusLabel(t.status) }}</span>
              </div>

              <!-- Аудиоплеер Шефа -->
              <div v-if="t.voice_url" class="bg-slate-950 p-2 rounded-xl flex items-center gap-2 border border-slate-800/60">
                <audio :src="t.voice_url" controls class="w-full h-8 opacity-90"></audio>
              </div>

              <div class="bg-slate-950/60 p-2.5 rounded-xl border border-slate-800/40 text-xs space-y-1">
                <p class="text-slate-300"><strong>Суть:</strong> {{ t.ai_summary }}</p>
                <p class="text-slate-400"><strong>Исполнитель:</strong> {{ t.lead_name || 'Ожидает назначения Замом' }}</p>
              </div>
            </div>
          </div>
        </div>

        <!-- ========================================== -->
        <!-- 2. ЭКРАН ЗАМДИРЕКТОРА (ДИСПЕТЧЕР) -->
        <!-- ========================================== -->
        <div v-if="role === 'DEPUTY'" class="space-y-4">
          
          <!-- Вкладки фильтрации -->
          <div class="flex gap-1.5 p-1 bg-slate-900 rounded-xl border border-slate-800">
            <button @click="deputyTab = 'inbox'" :class="deputyTab === 'inbox' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="flex-1 py-1.5 text-xs rounded-lg text-center">
              Входящие ({{ inboxTasks.length }})
            </button>
            <button @click="deputyTab = 'active'" :class="deputyTab === 'active' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="flex-1 py-1.5 text-xs rounded-lg text-center">
              В работе ({{ activeTasks.length }})
            </button>
            <button @click="deputyTab = 'archive'" :class="deputyTab === 'archive' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="flex-1 py-1.5 text-xs rounded-lg text-center">
              Архив ({{ archiveTasks.length }})
            </button>
          </div>

          <!-- Список задач Зама -->
          <div class="space-y-3">
            <div v-for="t in displayedDeputyTasks" :key="t.id" class="bg-slate-900 border border-slate-800 rounded-2xl p-4 space-y-3 shadow-md">
              
              <div class="flex justify-between items-start gap-2">
                <div>
                  <div class="flex items-center gap-1.5 mb-1">
                    <span class="text-[10px] font-mono font-bold bg-slate-800 text-indigo-300 px-2 py-0.5 rounded">#{{ t.id }}</span>
                    <span class="text-[10px] font-bold px-2 py-0.5 rounded bg-purple-950 text-purple-300">{{ t.task_type }}</span>
                  </div>
                  <h4 class="text-sm font-bold text-white leading-snug">{{ t.title }}</h4>
                </div>
                <span class="text-[10px] font-bold px-2 py-1 rounded shrink-0" :class="getStatusBadge(t.status)">{{ getStatusLabel(t.status) }}</span>
              </div>

              <!-- Плеер аудио Шефа с ускорением -->
              <div v-if="t.voice_url" class="bg-slate-950 p-2.5 rounded-xl border border-slate-800 space-y-1.5">
                <div class="flex justify-between items-center text-[10px] text-slate-400">
                  <span class="font-bold">🎙 Голос Шефа (Оригинал)</span>
                  <div class="flex gap-1">
                    <button @click="setSpeed($event, 1.0)" class="px-1.5 py-0.5 bg-slate-800 rounded text-[9px] hover:bg-slate-700">1.0x</button>
                    <button @click="setSpeed($event, 1.5)" class="px-1.5 py-0.5 bg-slate-800 rounded text-[9px] hover:bg-slate-700">1.5x</button>
                    <button @click="setSpeed($event, 2.0)" class="px-1.5 py-0.5 bg-slate-800 rounded text-[9px] hover:bg-slate-700">2.0x</button>
                  </div>
                </div>
                <audio :src="t.voice_url" controls class="w-full h-8 opacity-95"></audio>
              </div>

              <!-- Блок ТЗ от ИИ -->
              <div class="bg-slate-950/70 p-3 rounded-xl border border-slate-800/80 text-xs space-y-1.5">
                <p class="text-slate-300 leading-relaxed"><strong class="text-indigo-400">🤖 ТЗ:</strong> {{ t.ai_summary }}</p>
                <div class="text-slate-400 pt-1 border-t border-slate-800/60 whitespace-pre-line">
                  <strong class="text-slate-300">🎯 Критерии приемки (DoD):</strong><br>{{ t.definition_of_done }}
                </div>
              </div>

              <!-- БЛОК НАЗНАЧЕНИЯ (Для новых задач) -->
              <div v-if="t.status === 'DRAFT'" class="pt-2 border-t border-slate-800 space-y-2.5">
                <label class="block text-[11px] font-bold text-slate-300">Назначить Главного Лида:</label>
                <select v-model="assignForm[t.id].lead_id" class="w-full bg-slate-950 border border-slate-700 text-xs p-2.5 rounded-xl text-white focus:ring-2 focus:ring-indigo-500">
                  <option v-for="u in employees" :key="u.id" :value="u.id">{{ u.full_name }} ({{ u.department }})</option>
                </select>

                <label class="block text-[11px] font-bold text-slate-300">Срок выполнения (Дедлайн):</label>
                <input v-model="assignForm[t.id].deadline" type="datetime-local" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-white">

                <button @click="assignTask(t.id)" class="w-full bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-2.5 rounded-xl text-xs shadow-lg transition">
                  🚀 Утвердить и отправить Лиду
                </button>
              </div>

              <!-- БЛОК КОНТРОЛЯ ЧЕКПОИНТОВ (В процессе) -->
              <div v-if="t.status === 'IN_PROGRESS'" class="pt-2 border-t border-slate-800 space-y-2">
                <div class="flex justify-between items-center text-xs">
                  <span class="text-slate-400">Лид: <strong class="text-white">{{ t.lead_name }}</strong></span>
                </div>

                <!-- Статусы 30% и 70% -->
                <div class="grid grid-cols-2 gap-2">
                  <button @click="approveCp(t.id, '30_PERCENT')" :class="getCpButtonClass(t, '30_PERCENT')" class="py-2 px-2 text-xs font-bold rounded-xl border flex items-center justify-center gap-1">
                    <span>30%</span>
                    <span>{{ getCpStatusText(t, '30_PERCENT') }}</span>
                  </button>
                  <button @click="approveCp(t.id, '70_PERCENT')" :class="getCpButtonClass(t, '70_PERCENT')" class="py-2 px-2 text-xs font-bold rounded-xl border flex items-center justify-center gap-1">
                    <span>70%</span>
                    <span>{{ getCpStatusText(t, '70_PERCENT') }}</span>
                  </button>
                </div>

                <button @click="completeTask(t.id)" class="w-full bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-2.5 rounded-xl text-xs shadow transition">
                  🏁 Принять работу и закрыть в Архив
                </button>
              </div>

            </div>
          </div>
        </div>

        <!-- ========================================== -->
        <!-- 3. ЭКРАН СОТРУДНИКА / ЛИДА -->
        <!-- ========================================== -->
        <div v-if="role === 'EMPLOYEE'" class="space-y-4">
          <div class="bg-indigo-950/40 border border-indigo-800/40 p-3 rounded-2xl text-xs text-indigo-300">
            👋 Вы вошли как <strong>Азизов Рустам (Склад)</strong>. Здесь отображаются назначенные на вас задачи.
          </div>

          <div class="space-y-3">
            <div v-for="t in employeeTasks" :key="t.id" class="bg-slate-900 border border-slate-800 rounded-2xl p-4 space-y-3 shadow">
              <div class="flex justify-between items-start">
                <div>
                  <span class="text-[10px] font-mono font-bold bg-slate-800 text-indigo-300 px-2 py-0.5 rounded">#{{ t.id }}</span>
                  <h4 class="text-sm font-bold text-white mt-1">{{ t.title }}</h4>
                </div>
                <span class="text-[10px] font-bold px-2 py-1 rounded" :class="getStatusBadge(t.status)">{{ getStatusLabel(t.status) }}</span>
              </div>

              <div class="bg-slate-950/70 p-3 rounded-xl border border-slate-800/60 text-xs space-y-2">
                <p class="text-slate-300"><strong>Что сделать:</strong> {{ t.ai_summary }}</p>
                <p class="text-slate-400 whitespace-pre-line"><strong>Критерии сдачи:</strong><br>{{ t.definition_of_done }}</p>
              </div>

              <!-- Кнопки действий сотрудника -->
              <div v-if="t.status === 'IN_PROGRESS'" class="pt-2 border-t border-slate-800 space-y-2">
                <div class="grid grid-cols-2 gap-2">
                  <button @click="sendReport(t.id, '30_PERCENT')" class="bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 text-xs font-bold py-2 rounded-xl">
                    📝 Сдать 30%
                  </button>
                  <button @click="sendReport(t.id, '70_PERCENT')" class="bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 text-xs font-bold py-2 rounded-xl">
                    📝 Сдать 70%
                  </button>
                </div>

                <button @click="triggerRedFlag(t.id)" class="w-full bg-red-950/60 hover:bg-red-900/60 text-red-300 border border-red-800/50 text-xs font-bold py-2 rounded-xl">
                  🚩 Red Flag (Уперся в блокер)
                </button>
              </div>
            </div>
          </div>
        </div>

      </div>

      <!-- VUE 3 LOGIC -->
      <script>
        const { createApp, ref, computed, onMounted } = Vue;
        createApp({
          setup() {
            const role = ref('OWNER');
            const deputyTab = ref('inbox');
            const isRecording = ref(false);
            const isProcessing = ref(false);
            const recordSeconds = ref(0);
            let timerInterval = null;

            const tasks = ref([]);
            const users = ref([]);
            const assignForm = ref({});
            let mediaRecorder = null;
            let audioChunks = [];

            const currentRoleLabel = computed(() => {
              if (role.value === 'OWNER') return 'Шеф (Владелец)';
              if (role.value === 'DEPUTY') return 'Замдиректора (Диспетчер)';
              return 'Сотрудник (Лид)';
            });

            const employees = computed(() => users.value.filter(u => u.role === 'EMPLOYEE' || u.role === 'DEPUTY'));
            const inboxTasks = computed(() => tasks.value.filter(t => t.status === 'DRAFT'));
            const activeTasks = computed(() => tasks.value.filter(t => t.status === 'IN_PROGRESS'));
            const archiveTasks = computed(() => tasks.value.filter(t => t.status === 'ARCHIVED'));
            const employeeTasks = computed(() => tasks.value.filter(t => t.status === 'IN_PROGRESS' || t.status === 'DRAFT'));

            const displayedDeputyTasks = computed(() => {
              if (deputyTab.value === 'inbox') return inboxTasks.value;
              if (deputyTab.value === 'active') return activeTasks.value;
              return archiveTasks.value;
            });

            const loadData = async () => {
              try {
                const [resTasks, resUsers] = await Promise.all([
                  fetch('/api/tasks').then(r => r.json()),
                  fetch('/api/users').then(r => r.json())
                ]);
                tasks.value = resTasks;
                users.value = resUsers;

                // Инициализация формы назначения
                resTasks.forEach(t => {
                  if (!assignForm.value[t.id]) {
                    const defaultDeadline = new Date(Date.now() + 86400000).toISOString().slice(0, 16);
                    assignForm.value[t.id] = {
                      lead_id: users.value.find(u => u.role === 'EMPLOYEE')?.id || 1,
                      deadline: defaultDeadline
                    };
                  }
                });
              } catch (e) {
                console.error(e);
              }
            };

            const toggleRecord = async () => {
              if (!isRecording.value) {
                try {
                  const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
                  mediaRecorder = new MediaRecorder(stream);
                  audioChunks = [];
                  recordSeconds.value = 0;
                  
                  timerInterval = setInterval(() => {
                    recordSeconds.value++;
                  }, 1000);

                  mediaRecorder.ondataavailable = e => audioChunks.push(e.data);
                  mediaRecorder.onstop = async () => {
                    clearInterval(timerInterval);
                    isProcessing.value = true;
                    const audioBlob = new Blob(audioChunks, { type: 'audio/webm' });
                    const fd = new FormData();
                    fd.append('audio', audioBlob, 'voice.webm');
                    fd.append('user_id', 1);

                    try {
                      await fetch('/api/tasks/create-voice', { method: 'POST', body: fd });
                      await loadData();
                    } catch (err) {
                      alert('Ошибка обработки ИИ: ' + err.message);
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

            const formatTime = (secs) => {
              const m = Math.floor(secs / 60).toString().padStart(2, '0');
              const s = (secs % 60).toString().padStart(2, '0');
              return `${m}:${s}`;
            };

            const assignTask = async (taskId) => {
              const form = assignForm.value[taskId];
              const fd = new FormData();
              fd.append('lead_id', form.lead_id);
              fd.append('deadline', form.deadline);
              await fetch(`/api/tasks/${taskId}/assign`, { method: 'POST', body: fd });
              await loadData();
            };

            const approveCp = async (taskId, cpType) => {
              const fd = new FormData();
              fd.append('cp_type', cpType);
              await fetch(`/api/tasks/${taskId}/approve-checkpoint`, { method: 'POST', body: fd });
              await loadData();
            };

            const sendReport = async (taskId, cpType) => {
              const text = prompt('Введите краткий отчет по этапу (что сделано):');
              if (!text) return;
              const fd = new FormData();
              fd.append('cp_type', cpType);
              fd.append('report_text', text);
              await fetch(`/api/tasks/${taskId}/checkpoint-report`, { method: 'POST', body: fd });
              await loadData();
              alert('Отчет отправлен Замдиректора!');
            };

            const triggerRedFlag = async (taskId) => {
              const reason = prompt('В чем блокер? Опишите проблему:');
              if (!reason) return;
              const fd = new FormData();
              fd.append('reason', reason);
              await fetch(`/api/tasks/${taskId}/red-flag`, { method: 'POST', body: fd });
              await loadData();
              alert('🚨 Блокер зафиксирован! Замдиректора уведомлен.');
            };

            const completeTask = async (taskId) => {
              if (!confirm('Принять результат и отправить задачу в архив?')) return;
              await fetch(`/api/tasks/${taskId}/complete`, { method: 'POST' });
              await loadData();
            };

            const setSpeed = (event, speed) => {
              const player = event.target.closest('.bg-slate-950').querySelector('audio');
              if (player) player.playbackRate = speed;
            };

            const getStatusBadge = (status) => {
              if (status === 'DRAFT') return 'bg-amber-950 text-amber-300 border border-amber-800/50';
              if (status === 'IN_PROGRESS') return 'bg-blue-950 text-blue-300 border border-blue-800/50';
              if (status === 'ARCHIVED') return 'bg-emerald-950 text-emerald-300 border border-emerald-800/50';
              return 'bg-slate-800 text-slate-300';
            };

            const getStatusLabel = (status) => {
              if (status === 'DRAFT') return 'Входящие';
              if (status === 'IN_PROGRESS') return 'В работе';
              if (status === 'ARCHIVED') return 'Архив';
              return status;
            };

            const getCpStatusText = (task, type) => {
              const cp = task.checkpoints?.find(c => c.cp_type === type);
              if (!cp) return '—';
              if (cp.status === 'APPROVED') return '✅ Принят';
              if (cp.status === 'SUBMITTED') return '⏳ Сдан';
              return '⏳ Ждет';
            };

            const getCpButtonClass = (task, type) => {
              const cp = task.checkpoints?.find(c => c.cp_type === type);
              if (cp?.status === 'APPROVED') return 'bg-emerald-950/60 text-emerald-300 border-emerald-800/60';
              if (cp?.status === 'SUBMITTED') return 'bg-amber-950/60 text-amber-300 border-amber-800/60 animate-pulse';
              return 'bg-slate-950 text-slate-400 border-slate-800 hover:bg-slate-800';
            };

            onMounted(() => {
              loadData();
              setInterval(loadData, 4000);
            });

            return {
              role, deputyTab, currentRoleLabel, isRecording, isProcessing, recordSeconds,
              tasks, users, employees, inboxTasks, activeTasks, archiveTasks, employeeTasks,
              displayedDeputyTasks, assignForm, toggleRecord, formatTime, assignTask,
              approveCp, sendReport, triggerRedFlag, completeTask, setSpeed,
              getStatusBadge, getStatusLabel, getCpStatusText, getCpButtonClass
            };
          }
        }).mount('#app');
      </script>
    </body>
    </html>
    """

