import os
import json
import base64
import re
import asyncio
import traceback
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from starlette.middleware.base import BaseHTTPMiddleware
import asyncpg

app = FastAPI()

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, private"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        return response

app.add_middleware(SecurityHeadersMiddleware)

DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")

db_pool = None
sse_subscribers = set()
FAILED_LOGIN_ATTEMPTS = {}

async def broadcast_event(event_type: str = "update"):
    for queue in list(sse_subscribers):
        try:
            await queue.put(f"data: {json.dumps({'event': event_type})}\n\n")
        except Exception:
            sse_subscribers.discard(queue)

def send_telegram_alert(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        }).encode("utf-8")
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f"Telegram Alert Error: {e}")

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

async def keep_alive_worker():
    while True:
        await asyncio.sleep(480)
        try:
            pool = await get_db()
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            if RENDER_EXTERNAL_URL:
                req = urllib.request.Request(f"{RENDER_EXTERNAL_URL}/api/health", headers={"User-Agent": "KeepAlive"})
                urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            print(f"Keep-alive error: {e}")

@app.on_event("startup")
async def startup():
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            ALTER TABLE users ADD COLUMN IF NOT EXISTS username VARCHAR(50);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS password VARCHAR(100);
            ALTER TABLE users ALTER COLUMN phone_or_login DROP NOT NULL;
            
            ALTER TABLE tasks ADD COLUMN IF NOT EXISTS priority VARCHAR(30) DEFAULT 'NORMAL';
            ALTER TABLE tasks ADD COLUMN IF NOT EXISTS deadline TIMESTAMPTZ;
            ALTER TABLE tasks ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
            ALTER TABLE tasks ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
            ALTER TABLE tasks ADD COLUMN IF NOT EXISTS result_report TEXT;
            ALTER TABLE tasks ADD COLUMN IF NOT EXISTS progress INT DEFAULT 0;
            ALTER TABLE tasks ADD COLUMN IF NOT EXISTS pending_request VARCHAR(30);
            ALTER TABLE tasks ADD COLUMN IF NOT EXISTS project_group VARCHAR(100) DEFAULT 'Проект Кормовая Мука';
            ALTER TABLE tasks ADD COLUMN IF NOT EXISTS assignee_ids INT[] DEFAULT '{}';
            ALTER TABLE tasks ADD COLUMN IF NOT EXISTS created_by INT DEFAULT 1;

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

            CREATE TABLE IF NOT EXISTS task_user_reads (
                task_id INT REFERENCES tasks(id) ON DELETE CASCADE,
                user_id INT REFERENCES users(id) ON DELETE CASCADE,
                last_read_msg_id INT DEFAULT 0,
                PRIMARY KEY (task_id, user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_status_priority ON tasks (status, priority, deadline);
            CREATE INDEX IF NOT EXISTS idx_task_messages_task_id ON task_messages (task_id, id ASC);
            CREATE INDEX IF NOT EXISTS idx_task_user_reads ON task_user_reads (task_id, user_id);
        """)

        await conn.execute("UPDATE users SET is_active = FALSE")

        team_users = [
            (1, 'khurshid', 'Хуршид', 'OWNER', 'Дирекция', 'khurshid', 'treds5'),
            (2, 'zhamoliddin', 'Жамолиддин', 'DEPUTY', 'Управление', 'zhamoliddin', 'tru1'),
            (3, 'marat', 'Марат', 'EMPLOYEE', 'Исполнитель', 'marat', 'fruti9'),
            (4, 'sagynay', 'Сагынай', 'EMPLOYEE', 'Исполнитель', 'sagynay', 'reet3'),
            (5, 'ibrohim', 'Иброхим', 'EMPLOYEE', 'Исполнитель', 'ibrohim', 'frost')
        ]
        for uid, phone_login, name, role, dept, uname, pwd in team_users:
            await conn.execute("""
                INSERT INTO users (id, phone_or_login, full_name, role, department, username, password, is_active)
                VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE)
                ON CONFLICT (id) DO UPDATE 
                SET phone_or_login = $2, username = $6, password = $7, full_name = $3, role = $4, department = $5, is_active = TRUE
            """, uid, phone_login, name, role, dept, uname, pwd)

    asyncio.create_task(keep_alive_worker())

@app.get("/api/health")
async def health():
    return {"status": "alive"}

@app.get("/api/events")
async def events_stream(request: Request):
    async def event_generator():
        queue = asyncio.Queue()
        sse_subscribers.add(queue)
        try:
            yield "data: {\"event\": \"connected\"}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                data = await queue.get()
                yield data
        finally:
            sse_subscribers.discard(queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.post("/api/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    client_ip = request.client.host if request.client else "unknown"
    now = datetime.now(timezone.utc)
    
    if client_ip in FAILED_LOGIN_ATTEMPTS:
        attempts, lock_until = FAILED_LOGIN_ATTEMPTS[client_ip]
        if lock_until and now < lock_until:
            wait_sec = int((lock_until - now).total_seconds())
            raise HTTPException(status_code=429, detail=f"Слишком много попыток. Подождите {wait_sec} сек.")
    
    pool = await get_db()
    async with pool.acquire() as conn:
        user = await conn.fetchrow("""
            SELECT id, full_name, role, department, username 
            FROM users 
            WHERE (LOWER(TRIM(username)) = LOWER(TRIM($1)) OR LOWER(TRIM(phone_or_login)) = LOWER(TRIM($1))) 
              AND TRIM(password) = TRIM($2) 
              AND is_active = TRUE
        """, username, password)
        
        if not user:
            attempts, _ = FAILED_LOGIN_ATTEMPTS.get(client_ip, (0, None))
            attempts += 1
            lock = now + timedelta(minutes=10) if attempts >= 5 else None
            FAILED_LOGIN_ATTEMPTS[client_ip] = (attempts, lock)
            raise HTTPException(status_code=401, detail="Неверный логин или пароль")
        
        FAILED_LOGIN_ATTEMPTS.pop(client_ip, None)
        return {"status": "ok", "user": dict(user)}

SYSTEM_PROMPT = """
Ты — операционный директор компании. Преврати поручение руководства в четкое техническое задание для команды.
Верни ТОЛЬКО валидный JSON (без markdown):
{
  "title": "Краткий заголовок (до 6 слов)",
  "ai_summary": "Суть задачи в 2 предложениях",
  "definition_of_done": "1. Первый пункт\\n2. Второй пункт",
  "task_type": "SOLO",
  "priority": "URGENT",
  "project_group": "Проект Кормовая Мука"
}
"""

def query_gemini_direct(parts_list: list) -> dict:
    raw_key = os.getenv("GEMINI_API_KEY", "")
    clean_key = raw_key.strip().strip("[]'\"")

    if not clean_key:
        print("[Gemini Error] GEMINI_API_KEY пустой или не задан в Render!")
        return {
            "title": "Новое поручение",
            "ai_summary": "Поручение принято и зарегистрировано в системе",
            "definition_of_done": "1. Выполнить поручение в срок",
            "task_type": "SOLO",
            "priority": "URGENT",
            "project_group": "Проект Кормовая Мука"
        }

    models = ["gemini-3.6-flash", "gemini-2.5-flash", "gemini-2.5-pro"]
    
    for model_name in models:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={clean_key}"
        payload = {
            "contents": [{"parts": parts_list}],
            "generationConfig": {
                "temperature": 0.2,
                "responseMimeType": "application/json"
            }
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                res_json = json.loads(resp.read().decode("utf-8"))
                raw_text = res_json["candidates"][0]["content"]["parts"][0]["text"]
                clean_text = raw_text.replace("```json", "").replace("
