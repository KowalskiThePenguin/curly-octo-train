import os
import json
import base64
import re
import asyncio
import traceback
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse
import asyncpg

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")

db_pool = None
sse_subscribers = set()

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
async def login(username: str = Form(...), password: str = Form(...)):
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
            raise HTTPException(status_code=401, detail="Неверный логин или пароль")
        return {"status": "ok", "user": dict(user)}

SYSTEM_PROMPT = """
Ты — операционный директор компании. Преврати поручение владельца в четкое техническое задание для Замдиректора.
Верни ТОЛЬКО валидный JSON (без markdown):
{
  "title": "Краткий заголовок (до 6 слов)",
  "ai_summary": "Суть задачи в 2 предложениях",
  "definition_of_done": "1. Первый пункт\\n2. Второй пункт",
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
        "ai_summary": "Поручение принято и передано Директору",
        "definition_of_done": "1. Выполнить поручение в срок",
        "task_type": "SOLO",
        "priority": "URGENT"
    }

@app.get("/api/users")
async def get_users():
    pool = await get_db()
    async with pool.acquire() as conn:
        users = await conn.fetch("SELECT id, full_name, role, department, username FROM users WHERE is_active = TRUE ORDER BY id ASC")
        return [dict(u) for u in users]

@app.get("/api/tasks")
async def get_tasks(viewer_user_id: int = 1):
    pool = await get_db()
    async with pool.acquire() as conn:
        tasks = await conn.fetch("""
            SELECT t.id, t.title, t.raw_input_text, t.ai_summary, t.definition_of_done,
                   t.task_type, t.status, t.priority, t.lead_user_id, t.result_report,
                   to_char(t.deadline AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as deadline,
                   to_char(t.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as created_at,
                   to_char(t.completed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as completed_at,
                   u.full_name as lead_name,
                   (EXISTS(SELECT 1 FROM media_attachments m WHERE m.task_id = t.id AND m.attachment_type = 'VOICE_ORIGINAL')) as has_voice,
                   (SELECT COUNT(*) FROM task_messages msg 
                    WHERE msg.task_id = t.id 
                      AND msg.sender_id != $1 
                      AND msg.id > COALESCE((SELECT last_read_msg_id FROM task_user_reads r WHERE r.task_id = t.id AND r.user_id = $1), 0)
                   ) as unread_count
            FROM tasks t
            LEFT JOIN users u ON u.id = t.lead_user_id
            ORDER BY 
                CASE WHEN t.status = 'ARCHIVED' THEN 2 ELSE 1 END ASC,
                CASE 
                    WHEN t.priority = 'URGENT' THEN 1 
                    WHEN t.priority = 'NORMAL' THEN 2 
                    WHEN t.priority = 'FUTURE' THEN 3 
                    ELSE 4 
                END ASC,
                CASE WHEN t.deadline IS NULL THEN 1 ELSE 0 END ASC,
                t.deadline ASC,
                t.id DESC
        """, viewer_user_id)
        return [dict(t) for t in tasks]

@app.get("/api/tasks/{task_id}/voice")
async def get_task_voice(task_id: int):
    pool = await get_db()
    async with pool.acquire() as conn:
        audio_b64 = await conn.fetchval("""
            SELECT file_url FROM media_attachments 
            WHERE task_id = $1 AND attachment_type = 'VOICE_ORIGINAL' 
            LIMIT 1
        """, task_id)
        if not audio_b64:
            raise HTTPException(status_code=404, detail="Аудио не найдено")
        header, encoded = audio_b64.split(",", 1)
        mime = "audio/webm"
        if "audio/" in header:
            mime = header.split(";")[0].replace("data:", "")
        return Response(content=base64.b64decode(encoded), media_type=mime)

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
                INSERT INTO tasks (title, raw_input_text, ai_summary, definition_of_done, task_type, status, priority, created_by, is_urgent, created_at)
                VALUES ($1, $2, $3, $4, $5, 'DRAFT', $6, $7, $8, NOW())
                RETURNING id
            """, parsed.get("title", "Голосовое поручение"), "Голосовая аудиозапись Шефа", parsed.get("ai_summary", ""), parsed.get("definition_of_done", ""), parsed.get("task_type", "SOLO"), parsed.get("priority", "URGENT"), user_id, True)

            await conn.execute("""
                INSERT INTO media_attachments (task_id, sender_id, attachment_type, file_url, transcript)
                VALUES ($1, $2, 'VOICE_ORIGINAL', $3, $4)
            """, task_id, user_id, audio_b64, parsed.get("ai_summary", ""))

        send_telegram_alert(f"🎙 <b>Новое поручение #{task_id} от Хуршида</b>\n\n<b>Тема:</b> {parsed.get('title')}\n<b>ТЗ:</b> {parsed.get('ai_summary')}")
        await broadcast_event("new_task")
        return {"status": "ok", "task_id": task_id}
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
                INSERT INTO tasks (title, raw_input_text, ai_summary, definition_of_done, task_type, status, priority, created_by, is_urgent, created_at)
                VALUES ($1, $2, $3, $4, $5, 'DRAFT', $6, $7, $8, NOW())
                RETURNING id
            """, parsed.get("title", text[:30]), text, parsed.get("ai_summary", text), parsed.get("definition_of_done", "1. Выполнить задачу"), "SOLO", parsed.get("priority", "URGENT"), user_id, True)

        send_telegram_alert(f"📝 <b>Новое текстовое поручение #{task_id} от Хуршида</b>\n\n<b>Исходник:</b> {text}\n<b>ТЗ:</b> {parsed.get('ai_summary')}")
        await broadcast_event("new_task")
        return {"status": "ok", "task_id": task_id}
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
            lead_name = await conn.fetchval("SELECT full_name FROM users WHERE id = $1", lead_id)

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
                VALUES ($1, 2, 'DEPUTY', 'Жамолиддин', 'SYSTEM', $2)
            """, task_id, f"🚀 Задача назначена исполнителю: {lead_name}")

        send_telegram_alert(f"🚀 <b>Задача #{task_id} передана в работу</b>\n<b>Исполнитель:</b> {lead_name}\n<b>Приоритет:</b> {priority}")
        await broadcast_event("task_assigned")
        return {"status": "ok"}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Ошибка: {str(e)}")

@app.post("/api/tasks/{task_id}/submit-review")
async def submit_review(
    task_id: int, 
    sender_id: int = Form(...), 
    sender_name: str = Form(...), 
    report_text: str = Form(...)
):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE tasks 
            SET status = 'REVIEW', result_report = $1 
            WHERE id = $2
        """, report_text, task_id)

        await conn.execute("""
            INSERT INTO task_messages (task_id, sender_id, sender_role, sender_name, message_type, content)
            VALUES ($1, $2, 'EMPLOYEE', $3, 'SYSTEM', $4)
        """, task_id, sender_id, sender_name, f"🏁 Результат сдан на проверку:\n{report_text}")

    send_telegram_alert(f"🏁 <b>Задача #{task_id} сдана на проверку!</b>\n<b>Исполнитель:</b> {sender_name}\n<b>Отчет:</b> {report_text}")
    await broadcast_event("task_submitted")
    return {"status": "ok"}

@app.post("/api/tasks/{task_id}/reject-review")
async def reject_review(task_id: int, reason: str = Form(...)):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE tasks SET status = 'IN_PROGRESS' WHERE id = $1", task_id)
        await conn.execute("""
            INSERT INTO task_messages (task_id, sender_id, sender_role, sender_name, message_type, content)
            VALUES ($1, 2, 'DEPUTY', 'Жамолиддин', 'SYSTEM', $2)
        """, task_id, f"⚠️ Задача возвращена на доработку: {reason}")
    await broadcast_event("task_rejected")
    return {"status": "ok"}

# GET сообщений БЕЗ вызова SSE-триггера для предотвращения циклов
@app.get("/api/tasks/{task_id}/messages")
async def get_messages(task_id: int, viewer_user_id: int = 1):
    pool = await get_db()
    async with pool.acquire() as conn:
        max_id = await conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM task_messages WHERE task_id = $1", task_id)
        if max_id > 0:
            await conn.execute("""
                INSERT INTO task_user_reads (task_id, user_id, last_read_msg_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (task_id, user_id) DO UPDATE 
                SET last_read_msg_id = GREATEST(task_user_reads.last_read_msg_id, EXCLUDED.last_read_msg_id)
            """, task_id, viewer_user_id, max_id)

        rows = await conn.fetch("""
            SELECT msg.id, msg.task_id, msg.sender_id, msg.sender_role, msg.sender_name, msg.message_type, msg.content, msg.media_url,
                   to_char(msg.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as created_at,
                   EXISTS(
                       SELECT 1 FROM task_user_reads r 
                       WHERE r.task_id = msg.task_id 
                         AND r.user_id != msg.sender_id 
                         AND r.last_read_msg_id >= msg.id
                   ) as is_read
            FROM task_messages msg 
            WHERE msg.task_id = $1 
            ORDER BY msg.id ASC
        """, task_id)

        return [dict(r) for r in rows]

async def check_task_not_archived(conn, task_id: int):
    status = await conn.fetchval("SELECT status FROM tasks WHERE id = $1", task_id)
    if status == 'ARCHIVED':
        raise HTTPException(status_code=400, detail="Задача находится в архиве. Чат заблокирован.")

@app.post("/api/tasks/{task_id}/messages/text")
async def send_text_msg(
    task_id: int, 
    sender_id: int = Form(1),
    sender_role: str = Form(...), 
    sender_name: str = Form(...), 
    content: str = Form(...)
):
    pool = await get_db()
    async with pool.acquire() as conn:
        await check_task_not_archived(conn, task_id)
        msg_id = await conn.fetchval("""
            INSERT INTO task_messages (task_id, sender_id, sender_role, sender_name, message_type, content, created_at)
            VALUES ($1, $2, $3, $4, 'TEXT', $5, NOW())
            RETURNING id
        """, task_id, sender_id, sender_role, sender_name, content)
        
        await conn.execute("""
            INSERT INTO task_user_reads (task_id, user_id, last_read_msg_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (task_id, user_id) DO UPDATE 
            SET last_read_msg_id = GREATEST(task_user_reads.last_read_msg_id, EXCLUDED.last_read_msg_id)
        """, task_id, sender_id, msg_id)

    await broadcast_event(f"chat_{task_id}")
    return {"status": "ok"}

@app.post("/api/tasks/{task_id}/messages/voice")
async def send_voice_msg(
    task_id: int, 
    sender_id: int = Form(1),
    sender_role: str = Form(...), 
    sender_name: str = Form(...), 
    audio: UploadFile = File(...)
):
    pool = await get_db()
    async with pool.acquire() as conn:
        await check_task_not_archived(conn, task_id)
        audio_bytes = await audio.read()
        mime = audio.content_type or "audio/webm"
        audio_b64 = f"data:{mime};base64," + base64.b64encode(audio_bytes).decode('utf-8')
        msg_id = await conn.fetchval("""
            INSERT INTO task_messages (task_id, sender_id, sender_role, sender_name, message_type, media_url, content, created_at)
            VALUES ($1, $2, $3, $4, 'VOICE', $5, 'Голосовое сообщение', NOW())
            RETURNING id
        """, task_id, sender_id, sender_role, sender_name, audio_b64)
        
        await conn.execute("""
            INSERT INTO task_user_reads (task_id, user_id, last_read_msg_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (task_id, user_id) DO UPDATE 
            SET last_read_msg_id = GREATEST(task_user_reads.last_read_msg_id, EXCLUDED.last_read_msg_id)
        """, task_id, sender_id, msg_id)

    await broadcast_event(f"chat_{task_id}")
    return {"status": "ok"}

@app.post("/api/tasks/{task_id}/messages/image")
async def send_image_msg(
    task_id: int, 
    sender_id: int = Form(1),
    sender_role: str = Form(...), 
    sender_name: str = Form(...), 
    file: UploadFile = File(...)
):
    pool = await get_db()
    async with pool.acquire() as conn:
        await check_task_not_archived(conn, task_id)
        file_bytes = await file.read()
        mime = file.content_type or "image/jpeg"
        file_b64 = f"data:{mime};base64," + base64.b64encode(file_bytes).decode('utf-8')
        msg_id = await conn.fetchval("""
            INSERT INTO task_messages (task_id, sender_id, sender_role, sender_name, message_type, media_url, content, created_at)
            VALUES ($1, $2, $3, $4, 'IMAGE', $5, 'Прикрепленное фото', NOW())
            RETURNING id
        """, task_id, sender_id, sender_role, sender_name, file_b64)
        
        await conn.execute("""
            INSERT INTO task_user_reads (task_id, user_id, last_read_msg_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (task_id, user_id) DO UPDATE 
            SET last_read_msg_id = GREATEST(task_user_reads.last_read_msg_id, EXCLUDED.last_read_msg_id)
        """, task_id, sender_id, msg_id)

    await broadcast_event(f"chat_{task_id}")
    return {"status": "ok"}

@app.post("/api/tasks/{task_id}/red-flag")
async def red_flag(task_id: int, reason: str = Form(...), sender_id: int = Form(1), sender_name: str = Form("Исполнитель")):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE tasks SET priority = 'URGENT', is_urgent = TRUE, risks_notes = $1 WHERE id = $2
        """, f"🚨 RED FLAG: {reason}", task_id)
        
        msg_id = await conn.fetchval("""
            INSERT INTO task_messages (task_id, sender_id, sender_role, sender_name, message_type, content, created_at)
            VALUES ($1, $2, 'EMPLOYEE', $3, 'REDFLAG', $4, NOW())
            RETURNING id
        """, task_id, sender_id, sender_name, f"🚨 RED FLAG (БЛОКЕР): {reason}")

        await conn.execute("""
            INSERT INTO task_user_reads (task_id, user_id, last_read_msg_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (task_id, user_id) DO UPDATE 
            SET last_read_msg_id = GREATEST(task_user_reads.last_read_msg_id, EXCLUDED.last_read_msg_id)
        """, task_id, sender_id, msg_id)

    send_telegram_alert(f"🚨🚨🚨 <b>RED FLAG на задаче #{task_id}!</b>\n<b>Исполнитель:</b> {sender_name}\n<b>Проблема:</b> {reason}")
    await broadcast_event("red_flag")
    return {"status": "flagged"}

@app.post("/api/tasks/{task_id}/complete")
async def complete_task(task_id: int):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE tasks SET status = 'ARCHIVED', completed_at = NOW() WHERE id = $1", task_id)
        await conn.execute("""
            INSERT INTO task_messages (task_id, sender_id, sender_role, sender_name, message_type, content, created_at)
            VALUES ($1, 2, 'DEPUTY', 'Жамолиддин', 'SYSTEM', '🏁 Задача утверждена и закрыта в архив', NOW())
        """, task_id)
    await broadcast_event("task_completed")
    return {"status": "completed"}

@app.get("/", response_class=HTMLResponse)
async def index():
    return """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
  <title>Task OS Corporate</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://unpkg.com/vue@3.4.21/dist/vue.global.prod.js"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
  <style>
    [v-cloak] { display: none !important; }
    .overscroll-contain { overscroll-behavior: contain; }
  </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen font-sans antialiased select-none">
  <div id="app" v-cloak class="max-w-md mx-auto p-3.5 pb-24">
    
    <!-- ЭКРАН ВХОДА -->
    <div v-if="!currentUser" class="min-h-[85vh] flex flex-col justify-center px-2">
      <div class="bg-slate-900/90 border border-slate-800 p-6 rounded-3xl space-y-4 shadow-2xl text-center backdrop-blur-xl">
        <div class="w-14 h-14 bg-gradient-to-tr from-indigo-600 to-indigo-500 rounded-2xl flex items-center justify-center text-white text-2xl mx-auto shadow-lg shadow-indigo-600/30">
          <i class="fa-solid fa-shield-halved"></i>
        </div>
        <div>
          <h2 class="text-lg font-black tracking-wide text-white">TASK CONTROL OS</h2>
          <p class="text-xs text-slate-400 mt-0.5">Вход в персональный кабинет</p>
        </div>

        <form @submit.prevent="handleLogin" class="text-left space-y-3 pt-2">
          <div>
            <label class="block text-[10px] font-bold text-slate-400 uppercase mb-1">Ваш логин:</label>
            <input v-model="loginForm.username" type="text" autocapitalize="none" required placeholder="Введите логин" class="w-full bg-slate-950/80 border border-slate-700/80 text-xs p-3 rounded-xl text-white font-medium focus:border-indigo-500 outline-none">
          </div>

          <div>
            <label class="block text-[10px] font-bold text-slate-400 uppercase mb-1">Пароль:</label>
            <input v-model="loginForm.password" type="password" required placeholder="••••" class="w-full bg-slate-950/80 border border-slate-700/80 text-xs p-3 rounded-xl text-white font-medium focus:border-indigo-500 outline-none">
          </div>

          <button type="submit" :disabled="isLoggingIn" class="w-full bg-gradient-to-r from-indigo-600 to-indigo-500 hover:from-indigo-500 hover:to-indigo-400 text-white font-bold py-3 rounded-xl text-xs shadow-lg shadow-indigo-600/30 transition mt-2 flex items-center justify-center gap-2">
            <i v-if="isLoggingIn" class="fa-solid fa-circle-notch fa-spin"></i>
            <span>{{ isLoggingIn ? 'Вход...' : 'Войти в кабинет' }}</span>
          </button>
        </form>
      </div>
    </div>

    <!-- ОСНОВНОЙ ИНТЕРФЕЙС -->
    <div v-else class="space-y-4">
      
      <!-- ХЕДЕР -->
      <header class="bg-slate-900/90 border border-slate-800 p-3 rounded-2xl shadow-lg backdrop-blur-md flex justify-between items-center">
        <div class="flex items-center gap-2.5">
          <div class="w-2.5 h-2.5 rounded-full" :class="sseConnected ? 'bg-emerald-500 animate-pulse' : 'bg-amber-500'"></div>
          <div>
            <h3 class="text-xs font-bold text-white leading-none">{{ currentUser.full_name }}</h3>
            <span class="text-[9px] font-bold text-indigo-400 uppercase tracking-wider">{{ roleBadgeTitle }}</span>
          </div>
        </div>
        <button @click="handleLogout" class="text-[10px] font-bold text-red-400 bg-red-950/40 hover:bg-red-900/60 px-2.5 py-1.5 rounded-xl border border-red-800/50 transition">
          <i class="fa-solid fa-right-from-bracket mr-1"></i> Выйти
        </button>
      </header>

      <!-- 1. КАБИНЕТ ШЕФА (ХУРШИД) -->
      <div v-if="currentUser.role === 'OWNER'" class="space-y-4">
        <div class="bg-slate-900/90 border border-slate-800 p-5 rounded-3xl text-center space-y-3.5 shadow-xl backdrop-blur-md">
          <h2 class="text-base font-bold text-white">Голосовое поручение</h2>
          
          <div class="flex justify-center py-1">
            <button @click="toggleRecord" :class="isRecording ? 'bg-red-500 animate-pulse scale-105' : 'bg-indigo-600 active:scale-95'" class="w-20 h-20 rounded-full flex flex-col items-center justify-center text-white shadow-2xl transition duration-200">
              <i :class="isRecording ? 'fa-solid fa-stop text-xl' : 'fa-solid fa-microphone text-2xl'"></i>
              <span v-if="isRecording" class="text-[10px] font-mono font-bold mt-1">{{ formatTime(recordSeconds) }}</span>
            </button>
          </div>

          <div v-if="isProcessing" class="text-xs text-indigo-400 font-semibold animate-pulse">
            <i class="fa-solid fa-circle-notch fa-spin mr-1"></i> ИИ формирует ТЗ...
          </div>
          <p v-else class="text-[11px] font-semibold" :class="isRecording ? 'text-red-400' : 'text-slate-400'">
            {{ isRecording ? 'Идет запись... Нажмите для завершения' : 'Нажмите микрофон и говорите' }}
          </p>

          <div class="pt-2.5 border-t border-slate-800 text-left space-y-1.5">
            <div class="flex gap-1.5">
              <input v-model="textInput" @keyup.enter="sendTextTask" placeholder="Или напишите поручение текстом..." class="flex-1 bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-white outline-none focus:border-indigo-500">
              <button @click="sendTextTask" class="bg-indigo-600 hover:bg-indigo-500 px-3 rounded-xl text-white font-bold text-xs">
                <i class="fa-solid fa-paper-plane"></i>
              </button>
            </div>
          </div>
        </div>

        <div class="grid grid-cols-4 gap-1 p-1 bg-slate-900 rounded-xl border border-slate-800 text-[11px]">
          <button @click="ownerTab = 'inbox'" :class="ownerTab === 'inbox' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="py-1.5 rounded-lg text-center">Вход ({{ inboxTasks.length }})</button>
          <button @click="ownerTab = 'active'" :class="ownerTab === 'active' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="py-1.5 rounded-lg text-center">В работе ({{ activeTasks.length }})</button>
          <button @click="ownerTab = 'review'" :class="ownerTab === 'review' ? 'bg-amber-600 text-white font-bold animate-pulse' : 'text-amber-400 font-bold'" class="py-1.5 rounded-lg text-center">Сдано ({{ reviewTasks.length }})</button>
          <button @click="ownerTab = 'archive'" :class="ownerTab === 'archive' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="py-1.5 rounded-lg text-center">Архив ({{ archiveTasks.length }})</button>
        </div>

        <div class="space-y-3">
          <div v-if="displayedOwnerTasks.length === 0" class="p-8 text-center text-slate-500 text-xs bg-slate-900/50 rounded-2xl border border-slate-800">
            В этом разделе пока нет задач.
          </div>

          <div v-for="t in displayedOwnerTasks" :key="t.id" class="bg-slate-900 border border-slate-800 rounded-2xl p-4 space-y-3 shadow">
            <div class="flex justify-between items-start gap-2">
              <div class="flex flex-wrap items-center gap-1.5">
                <span class="text-[10px] font-mono font-bold bg-slate-800 text-indigo-300 px-1.5 py-0.5 rounded">#{{ t.id }}</span>
                <span class="text-[10px] font-bold px-2 py-0.5 rounded" :class="priorityBadge(t.priority)">{{ priorityLabel(t.priority) }}</span>
                <span v-if="t.status === 'IN_PROGRESS' || t.status === 'REVIEW'" class="text-[10px] font-mono px-2 py-0.5 rounded border" :class="getDeadlineBadge(t.deadline)">
                  ⏰ {{ getDeadlineCountdown(t.deadline) }}
                </span>
              </div>
              <span class="text-[10px] font-bold px-2 py-0.5 rounded" :class="statusBadge(t.status)">{{ statusLabel(t.status) }}</span>
            </div>

            <h4 class="text-sm font-bold text-white leading-snug">{{ t.title }}</h4>

            <!-- ПЛЕЕР ГОЛОСА ШЕФА -->
            <div v-if="t.has_voice" class="bg-slate-950 p-2.5 rounded-2xl border border-slate-800">
              <div class="flex items-center gap-3">
                <button @click="togglePlayAudio('task_' + t.id, '/api/tasks/' + t.id + '/voice')" class="w-10 h-10 rounded-full bg-indigo-600 hover:bg-indigo-500 text-white flex items-center justify-center text-sm shrink-0 shadow transition">
                  <i :class="activeAudioId === 'task_' + t.id && isAudioPlaying ? 'fa-solid fa-pause' : 'fa-solid fa-play ml-0.5'"></i>
                </button>
                <div class="flex-1 space-y-1">
                  <div @click="seekAudio('task_' + t.id, $event)" @touchmove="handleTouchSeek('task_' + t.id, $event)" class="h-6 flex items-center gap-0.5 cursor-pointer py-1">
                    <div v-for="(h, idx) in getWaveformBars(t.id)" :key="idx" 
                         :style="{ height: h + '%' }" 
                         :class="(activeAudioId === 'task_' + t.id && (idx / 28) <= audioProgress) ? 'bg-indigo-400' : 'bg-slate-700'"
                         class="w-1 rounded-full transition-colors"></div>
                  </div>
                  <div class="flex justify-between text-[10px] text-slate-400 font-mono">
                    <span>{{ activeAudioId === 'task_' + t.id ? formatAudioTime(audioCurrentTime) : 'Голос Шефа' }}</span>
                    <span>{{ activeAudioId === 'task_' + t.id ? formatAudioTime(audioDuration) : '' }}</span>
                  </div>
                </div>
              </div>
            </div>

            <p class="text-[11px] text-amber-300/90 bg-amber-950/20 p-2.5 rounded-xl border border-amber-900/40">
              <strong>🗣 Исходное поручение:</strong> {{ t.raw_input_text }}
            </p>

            <div class="p-2.5 bg-slate-950 rounded-xl border border-slate-800 text-xs space-y-1.5">
              <p class="text-slate-300"><strong>Суть задачи:</strong> {{ t.ai_summary }}</p>
              <div class="text-slate-400 pt-1 border-t border-slate-800/80 whitespace-pre-line">
                <strong class="text-indigo-300">Критерии сдачи (DoD):</strong><br>{{ t.definition_of_done }}
              </div>
            </div>

            <p class="text-xs text-indigo-300 font-semibold px-1">
              👤 Исполнитель: {{ t.lead_name || 'Не назначен' }}
            </p>

            <div class="text-[10px] text-slate-400 space-y-0.5 pt-1 border-t border-slate-800/80">
              <div class="flex justify-between">
                <span>📅 Создано:</span>
                <span class="text-slate-200 font-mono">{{ formatLocalDT(t.created_at) }}</span>
              </div>
              <div v-if="t.deadline" class="flex justify-between">
                <span>⏰ Дедлайн:</span>
                <span class="text-amber-300 font-mono font-bold">{{ formatLocalDT(t.deadline) }}</span>
              </div>
              <div v-if="t.completed_at" class="flex justify-between">
                <span>🏁 Завершено:</span>
                <span class="text-emerald-400 font-mono">{{ formatLocalDT(t.completed_at) }}</span>
              </div>
            </div>

            <div v-if="t.status === 'REVIEW'" class="p-2.5 bg-amber-950/30 rounded-xl border border-amber-800/60 text-xs space-y-1">
              <span class="text-amber-300 font-bold block">📝 Сданный отчет исполнителя:</span>
              <p class="text-slate-200 whitespace-pre-wrap">{{ t.result_report }}</p>
            </div>

            <button @click="openChat(t)" class="w-full bg-slate-800 hover:bg-slate-700 text-indigo-300 font-bold py-2 rounded-xl text-xs flex items-center justify-center gap-2 border border-slate-700 transition">
              <i class="fa-solid fa-comments"></i>
              <span>Чат по задаче</span>
              <span v-if="t.unread_count > 0" class="bg-indigo-600 text-white text-[10px] px-1.5 py-0.2 rounded-full font-black">{{ t.unread_count }}</span>
            </button>
          </div>
        </div>
      </div>

      <!-- 2. КАБИНЕТ ДИРЕКТОРА (ЖАМОЛИДДИН) -->
      <div v-if="currentUser.role === 'DEPUTY'" class="space-y-4">
        <div class="grid grid-cols-4 gap-1 p-1 bg-slate-900 rounded-xl border border-slate-800 text-[11px]">
          <button @click="deputyTab = 'inbox'" :class="deputyTab === 'inbox' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="py-1.5 rounded-lg text-center">Вход ({{ inboxTasks.length }})</button>
          <button @click="deputyTab = 'active'" :class="deputyTab === 'active' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="py-1.5 rounded-lg text-center">В работе ({{ activeTasks.length }})</button>
          <button @click="deputyTab = 'review'" :class="deputyTab === 'review' ? 'bg-amber-600 text-white font-bold animate-pulse' : 'text-amber-400 font-bold'" class="py-1.5 rounded-lg text-center">Сдано ({{ reviewTasks.length }})</button>
          <button @click="deputyTab = 'archive'" :class="deputyTab === 'archive' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="py-1.5 rounded-lg text-center">Архив ({{ archiveTasks.length }})</button>
        </div>

        <div class="space-y-3">
          <div v-if="displayedDeputyTasks.length === 0" class="p-8 text-center text-slate-500 text-xs bg-slate-900/50 rounded-2xl border border-slate-800">
            В этом разделе пока нет задач.
          </div>

          <div v-for="t in displayedDeputyTasks" :key="t.id" class="bg-slate-900 border border-slate-800 rounded-2xl p-4 space-y-3 shadow">
            <div class="flex justify-between items-start gap-2">
              <div class="flex flex-wrap items-center gap-1.5">
                <span class="text-[10px] font-mono font-bold bg-slate-800 text-indigo-300 px-1.5 py-0.5 rounded">#{{ t.id }}</span>
                <span class="text-[10px] font-bold px-2 py-0.5 rounded" :class="priorityBadge(t.priority)">{{ priorityLabel(t.priority) }}</span>
                <span v-if="t.status === 'IN_PROGRESS' || t.status === 'REVIEW'" class="text-[10px] font-mono px-2 py-0.5 rounded border" :class="getDeadlineBadge(t.deadline)">
                  ⏰ {{ getDeadlineCountdown(t.deadline) }}
                </span>
              </div>
              <span class="text-[10px] font-bold px-2 py-0.5 rounded" :class="statusBadge(t.status)">{{ statusLabel(t.status) }}</span>
            </div>

            <h4 class="text-sm font-bold text-white leading-snug">{{ t.title }}</h4>

            <div v-if="t.has_voice" class="bg-slate-950 p-2.5 rounded-2xl border border-slate-800">
              <div class="flex items-center gap-3">
                <button @click="togglePlayAudio('task_' + t.id, '/api/tasks/' + t.id + '/voice')" class="w-10 h-10 rounded-full bg-indigo-600 hover:bg-indigo-500 text-white flex items-center justify-center text-sm shrink-0 shadow transition">
                  <i :class="activeAudioId === 'task_' + t.id && isAudioPlaying ? 'fa-solid fa-pause' : 'fa-solid fa-play ml-0.5'"></i>
                </button>
                <div class="flex-1 space-y-1">
                  <div @click="seekAudio('task_' + t.id, $event)" @touchmove="handleTouchSeek('task_' + t.id, $event)" class="h-6 flex items-center gap-0.5 cursor-pointer py-1">
                    <div v-for="(h, idx) in getWaveformBars(t.id)" :key="idx" 
                         :style="{ height: h + '%' }" 
                         :class="(activeAudioId === 'task_' + t.id && (idx / 28) <= audioProgress) ? 'bg-indigo-400' : 'bg-slate-700'"
                         class="w-1 rounded-full transition-colors"></div>
                  </div>
                  <div class="flex justify-between text-[10px] text-slate-400 font-mono">
                    <span>{{ activeAudioId === 'task_' + t.id ? formatAudioTime(audioCurrentTime) : 'Голос Шефа' }}</span>
                    <span>{{ activeAudioId === 'task_' + t.id ? formatAudioTime(audioDuration) : '' }}</span>
                  </div>
                </div>
              </div>
            </div>

            <p class="text-[11px] text-amber-300/90 bg-amber-950/20 p-2.5 rounded-xl border border-amber-900/40">
              <strong>🗣 Исходное поручение:</strong> {{ t.raw_input_text }}
            </p>

            <div class="p-2.5 bg-slate-950 rounded-xl border border-slate-800 text-xs space-y-1.5">
              <p class="text-slate-300"><strong>Суть задачи:</strong> {{ t.ai_summary }}</p>
              <div class="text-slate-400 pt-1 border-t border-slate-800/80 whitespace-pre-line">
                <strong class="text-indigo-300">Критерии сдачи (DoD):</strong><br>{{ t.definition_of_done }}
              </div>
            </div>

            <p class="text-xs text-indigo-300 font-semibold px-1">
              👤 Исполнитель: {{ t.lead_name || 'Не назначен' }}
            </p>

            <div class="text-[10px] text-slate-400 space-y-0.5 pt-1 border-t border-slate-800/80">
              <div class="flex justify-between">
                <span>📅 Создано:</span>
                <span class="text-slate-200 font-mono">{{ formatLocalDT(t.created_at) }}</span>
              </div>
              <div v-if="t.deadline" class="flex justify-between">
                <span>⏰ Дедлайн:</span>
                <span class="text-amber-300 font-mono font-bold">{{ formatLocalDT(t.deadline) }}</span>
              </div>
              <div v-if="t.completed_at" class="flex justify-between">
                <span>🏁 Завершено:</span>
                <span class="text-emerald-400 font-mono">{{ formatLocalDT(t.completed_at) }}</span>
              </div>
            </div>

            <div v-if="t.status === 'DRAFT'" class="space-y-2 pt-1 border-t border-slate-800">
              <input v-model="editDrafts[t.id].title" placeholder="Заголовок" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-white font-bold outline-none focus:border-indigo-500">
              <textarea v-model="editDrafts[t.id].ai_summary" rows="2" placeholder="Суть ТЗ" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-slate-200 outline-none focus:border-indigo-500"></textarea>
              <textarea v-model="editDrafts[t.id].definition_of_done" rows="2" placeholder="Критерии сдачи (DoD)" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-slate-200 outline-none focus:border-indigo-500"></textarea>

              <div class="grid grid-cols-2 gap-2 pt-1">
                <div>
                  <label class="block text-[10px] font-bold text-slate-400 mb-1">Важность:</label>
                  <select v-model="editDrafts[t.id].priority" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-white outline-none focus:border-indigo-500">
                    <option value="URGENT">🔴 Оперативно</option>
                    <option value="NORMAL">🟡 Умеренно</option>
                    <option value="FUTURE">🔵 На будущее</option>
                  </select>
                </div>

                <div>
                  <label class="block text-[10px] font-bold text-slate-400 mb-1">Исполнитель:</label>
                  <select v-model="editDrafts[t.id].lead_id" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-white outline-none focus:border-indigo-500">
                    <option v-for="u in employeesOnly" :key="u.id" :value="u.id">{{ u.full_name }}</option>
                  </select>
                </div>
              </div>

              <div>
                <label class="block text-[10px] font-bold text-slate-400 mb-1">Дедлайн (ваше местное время):</label>
                <input v-model="editDrafts[t.id].deadline" type="datetime-local" class="w-full bg-slate-950 border border-slate-700 text-xs p-2 rounded-xl text-white outline-none focus:border-indigo-500">
              </div>

              <button @click="assignTask(t.id)" class="w-full bg-indigo-600 hover:bg-indigo-500 text-white font-bold py-2.5 rounded-xl text-xs shadow transition mt-1">
                🚀 Утвердить и отправить исполнителю
              </button>
            </div>

            <div v-if="t.status === 'REVIEW'" class="space-y-2 pt-1 border-t border-amber-800/40">
              <div class="p-2.5 bg-amber-950/30 rounded-xl border border-amber-800/60 text-xs space-y-1.5">
                <p class="text-amber-300 font-bold">📝 Отчет исполнителя ({{ t.lead_name }}):</p>
                <p class="text-slate-200 whitespace-pre-wrap">{{ t.result_report }}</p>
              </div>

              <div class="grid grid-cols-2 gap-2 pt-1">
                <button @click="rejectTask(t.id)" class="bg-red-950 hover:bg-red-900 text-red-300 border border-red-800 font-bold py-2 rounded-xl text-xs transition">
                  ↩️ На доработку
                </button>
                <button @click="completeTask(t.id)" class="bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-2 rounded-xl text-xs shadow transition">
                  ✅ Принять и в архив
                </button>
              </div>
            </div>

            <button @click="openChat(t)" class="w-full bg-slate-800 hover:bg-slate-700 text-indigo-300 font-bold py-2 rounded-xl text-xs flex items-center justify-center gap-2 border border-slate-700 transition">
              <i class="fa-solid fa-comments"></i>
              <span>Чат по задаче</span>
              <span v-if="t.unread_count > 0" class="bg-indigo-600 text-white text-[10px] px-1.5 py-0.2 rounded-full font-black">{{ t.unread_count }}</span>
            </button>
          </div>
        </div>
      </div>

      <!-- 3. ЛИЧНЫЙ КАБИНЕТ ИСПОЛНИТЕЛЯ -->
      <div v-if="currentUser.role === 'EMPLOYEE'" class="space-y-4">
        <div class="bg-gradient-to-r from-slate-900 to-indigo-950 border border-indigo-800/50 p-4 rounded-3xl space-y-3 shadow-xl">
          <div class="flex items-center gap-3">
            <div class="w-10 h-10 rounded-2xl bg-indigo-600 text-white flex items-center justify-center font-black text-base shadow">
              {{ currentUser.full_name.charAt(0) }}
            </div>
            <div>
              <h3 class="text-sm font-bold text-white">{{ currentUser.full_name }}</h3>
              <p class="text-[11px] text-indigo-300 font-semibold">Исполнитель команды</p>
            </div>
          </div>

          <div class="grid grid-cols-3 gap-2 pt-2 border-t border-slate-800/80 text-center">
            <div class="bg-slate-950/60 p-2 rounded-xl border border-slate-800">
              <span class="text-[10px] text-slate-400 block font-semibold">В работе</span>
              <span class="text-sm font-black text-indigo-400">{{ myActiveTasks.length }}</span>
            </div>
            <div class="bg-slate-950/60 p-2 rounded-xl border border-slate-800">
              <span class="text-[10px] text-slate-400 block font-semibold">На проверке</span>
              <span class="text-sm font-black text-amber-400">{{ myReviewTasks.length }}</span>
            </div>
            <div class="bg-slate-950/60 p-2 rounded-xl border border-slate-800">
              <span class="text-[10px] text-slate-400 block font-semibold">Выполнено</span>
              <span class="text-sm font-black text-emerald-400">{{ myArchiveTasks.length }}</span>
            </div>
          </div>
        </div>

        <div class="flex gap-1.5 p-1 bg-slate-900 rounded-xl border border-slate-800 text-xs">
          <button @click="empTab = 'active'" :class="empTab === 'active' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="flex-1 py-1.5 rounded-lg">В работе ({{ myActiveTasks.length }})</button>
          <button @click="empTab = 'review'" :class="empTab === 'review' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="flex-1 py-1.5 rounded-lg">На проверке ({{ myReviewTasks.length }})</button>
          <button @click="empTab = 'archive'" :class="empTab === 'archive' ? 'bg-indigo-600 text-white font-bold' : 'text-slate-400'" class="flex-1 py-1.5 rounded-lg">История</button>
        </div>

        <div class="space-y-3">
          <div v-if="displayedEmpTasks.length === 0" class="p-8 text-center text-slate-500 text-xs bg-slate-900/50 rounded-2xl border border-slate-800">
            В этом разделе пока нет задач.
          </div>

          <div v-for="t in displayedEmpTasks" :key="t.id" class="bg-slate-900 border border-slate-800 rounded-2xl p-4 space-y-3 shadow">
            <div class="flex justify-between items-start gap-2">
              <div class="flex items-center gap-1.5">
                <span class="text-[10px] font-mono font-bold bg-slate-800 text-indigo-300 px-1.5 py-0.5 rounded">#{{ t.id }}</span>
                <span class="text-[10px] font-bold px-2 py-0.5 rounded" :class="priorityBadge(t.priority)">{{ priorityLabel(t.priority) }}</span>
              </div>
              <span class="text-[10px] font-mono px-2 py-0.5 rounded border" :class="getDeadlineBadge(t.deadline)">
                ⏰ {{ getDeadlineCountdown(t.deadline) }}
              </span>
            </div>

            <h4 class="text-sm font-bold text-white leading-snug">{{ t.title }}</h4>

            <div v-if="t.has_voice" class="bg-slate-950 p-2.5 rounded-2xl border border-slate-800">
              <div class="flex items-center gap-3">
                <button @click="togglePlayAudio('task_' + t.id, '/api/tasks/' + t.id + '/voice')" class="w-10 h-10 rounded-full bg-indigo-600 hover:bg-indigo-500 text-white flex items-center justify-center text-sm shrink-0 shadow transition">
                  <i :class="activeAudioId === 'task_' + t.id && isAudioPlaying ? 'fa-solid fa-pause' : 'fa-solid fa-play ml-0.5'"></i>
                </button>
                <div class="flex-1 space-y-1">
                  <div @click="seekAudio('task_' + t.id, $event)" @touchmove="handleTouchSeek('task_' + t.id, $event)" class="h-6 flex items-center gap-0.5 cursor-pointer py-1">
                    <div v-for="(h, idx) in getWaveformBars(t.id)" :key="idx" 
                         :style="{ height: h + '%' }" 
                         :class="(activeAudioId === 'task_' + t.id && (idx / 28) <= audioProgress) ? 'bg-indigo-400' : 'bg-slate-700'"
                         class="w-1 rounded-full transition-colors"></div>
                  </div>
                  <div class="flex justify-between text-[10px] text-slate-400 font-mono">
                    <span>{{ activeAudioId === 'task_' + t.id ? formatAudioTime(audioCurrentTime) : 'Голос Шефа' }}</span>
                    <span>{{ activeAudioId === 'task_' + t.id ? formatAudioTime(audioDuration) : '' }}</span>
                  </div>
                </div>
              </div>
            </div>

            <p class="text-[11px] text-amber-300/90 bg-amber-950/20 p-2.5 rounded-xl border border-amber-900/40">
              <strong>🗣 Исходное поручение:</strong> {{ t.raw_input_text }}
            </p>
            
            <div class="p-2.5 bg-slate-950 rounded-xl border border-slate-800 text-xs space-y-1.5">
              <p class="text-slate-300"><strong>Суть задачи:</strong> {{ t.ai_summary }}</p>
              <div class="text-slate-400 pt-1 border-t border-slate-800/80 whitespace-pre-line">
                <strong class="text-indigo-300">Критерии сдачи (DoD):</strong><br>{{ t.definition_of_done }}
              </div>
            </div>

            <div class="text-[10px] text-slate-400 space-y-0.5 pt-1 border-t border-slate-800/80">
              <div class="flex justify-between">
                <span>📅 Создано:</span>
                <span class="text-slate-200 font-mono">{{ formatLocalDT(t.created_at) }}</span>
              </div>
              <div v-if="t.deadline" class="flex justify-between">
                <span>⏰ Сдать до:</span>
                <span class="text-amber-300 font-mono font-bold">{{ formatLocalDT(t.deadline) }}</span>
              </div>
              <div v-if="t.completed_at" class="flex justify-between">
                <span>🏁 Завершено:</span>
                <span class="text-emerald-400 font-mono">{{ formatLocalDT(t.completed_at) }}</span>
              </div>
            </div>

            <div v-if="t.status === 'IN_PROGRESS'" class="space-y-2 pt-1 border-t border-slate-800">
              <div class="grid grid-cols-2 gap-2">
                <button @click="openChat(t)" class="bg-slate-800 hover:bg-slate-700 text-indigo-300 font-bold py-2 rounded-xl text-xs flex items-center justify-center gap-1.5 border border-slate-700 transition">
                  <i class="fa-solid fa-comments"></i> Чат
                  <span v-if="t.unread_count > 0" class="bg-indigo-600 text-white text-[9px] px-1.5 py-0.2 rounded-full font-black">{{ t.unread_count }}</span>
                </button>
                <button @click="sendRedFlag(t.id)" class="bg-red-950 text-red-300 border border-red-800 hover:bg-red-900 text-xs font-bold py-2 rounded-xl flex items-center justify-center gap-1 transition">
                  🚩 Red Flag
                </button>
              </div>

              <button @click="submitTaskForReview(t.id)" class="w-full bg-emerald-600 hover:bg-emerald-500 text-white font-black py-2.5 rounded-xl text-xs shadow-lg transition">
                🏁 Сдать задачу на проверку Жамолиддину
              </button>
            </div>

            <div v-if="t.status === 'REVIEW'" class="space-y-2 pt-1">
              <div class="bg-amber-950/40 border border-amber-800/50 p-2.5 rounded-xl text-xs text-amber-300 text-center font-bold">
                ⏳ Отчет передан Жамолиддину на проверку.
              </div>
              <button @click="openChat(t)" class="w-full bg-slate-800 hover:bg-slate-700 text-indigo-300 font-bold py-2 rounded-xl text-xs flex items-center justify-center gap-2 border border-slate-700 transition">
                <i class="fa-solid fa-comments"></i> Чат по задаче
                <span v-if="t.unread_count > 0" class="bg-indigo-600 text-white text-[9px] px-1.5 py-0.2 rounded-full font-black">{{ t.unread_count }}</span>
              </button>
            </div>

            <div v-if="t.status === 'ARCHIVED'" class="pt-1">
              <button @click="openChat(t)" class="w-full bg-slate-800 hover:bg-slate-700 text-indigo-300 font-bold py-2 rounded-xl text-xs flex items-center justify-center gap-2 border border-slate-700 transition">
                <i class="fa-solid fa-comments"></i> История чата
              </button>
            </div>

          </div>
        </div>
      </div>

    </div>

    <!-- ========================================== -->
    <!-- МОДАЛЬНОЕ ОКНО ЧАТА ПО ЗАДАЧЕ (ENTERPRISE VIEW) -->
    <!-- ========================================== -->
    <div v-if="activeChatTask" class="fixed inset-0 bg-slate-950 z-50 flex flex-col h-[100dvh] w-full max-w-md mx-auto overscroll-contain">
      
      <!-- Шапка чата -->
      <div class="p-3.5 border-b border-slate-800/80 flex justify-between items-center bg-slate-900/90 backdrop-blur-xl shrink-0 shadow-lg">
        <div class="flex items-center gap-2.5">
          <div class="w-9 h-9 rounded-xl bg-gradient-to-tr from-indigo-600 to-indigo-500 text-white flex items-center justify-center font-bold text-xs shadow-md shadow-indigo-600/30">
            #{{ activeChatTask.id }}
          </div>
          <div>
            <h3 class="text-xs font-bold text-white truncate max-w-[230px] leading-tight">{{ activeChatTask.title }}</h3>
            <span class="text-[9px] font-semibold text-indigo-300/80">{{ statusLabel(activeChatTask.status) }}</span>
          </div>
        </div>
        <button @click="closeChat" class="w-8 h-8 rounded-full bg-slate-800/80 text-slate-300 flex items-center justify-center text-sm hover:text-white transition">
          <i class="fa-solid fa-xmark"></i>
        </button>
      </div>

      <!-- Лента сообщений -->
      <div ref="chatContainer" @scroll="onChatScroll" class="flex-1 overflow-y-auto overscroll-contain p-3.5 space-y-3 bg-slate-950 relative">
        
        <div v-if="isChatLoading" class="flex flex-col items-center justify-center h-full text-slate-400 py-16 space-y-2.5">
          <i class="fa-solid fa-circle-notch fa-spin text-2xl text-indigo-500"></i>
          <span class="text-xs font-semibold tracking-wide">Загрузка диалога...</span>
        </div>

        <div v-else-if="chatMessages.length === 0" class="text-center text-slate-500 text-xs py-16">
          <i class="fa-regular fa-comments text-3xl mb-2 block opacity-40"></i>
          Сообщений пока нет. Напишите или надиктуйте ответ!
        </div>

        <div v-else v-for="m in chatMessages" :key="m.id" :class="isMyMessage(m) ? 'justify-end' : 'justify-start'" class="flex">
          <div :class="isMyMessage(m) ? 'bg-gradient-to-br from-indigo-600 to-indigo-700 text-white rounded-tr-none shadow-indigo-600/20' : (m.message_type === 'REDFLAG' ? 'bg-red-950/80 border border-red-800/80 text-red-200' : 'bg-slate-900 border border-slate-800/90 text-slate-200 rounded-tl-none')" class="max-w-[84%] rounded-2xl p-3 shadow-md text-xs space-y-1.5 transition-all">
            
            <div class="flex justify-between items-center gap-3 text-[9px] opacity-75 font-semibold">
              <span>{{ m.sender_name }} ({{ formatRoleName(m.sender_role) }})</span>
              <span>{{ formatLocalTimeOnly(m.created_at) }}</span>
            </div>

            <p v-if="m.message_type === 'TEXT' || m.message_type === 'REDFLAG' || m.message_type === 'SYSTEM'" class="leading-relaxed whitespace-pre-wrap">{{ m.content }}</p>

            <!-- АУДИОСООБЩЕНИЕ С ВОЛНАМИ -->
            <div v-if="m.message_type === 'VOICE'" class="pt-1">
              <div class="flex items-center gap-2.5 bg-black/20 p-2 rounded-xl border border-white/5">
                <button @click="togglePlayAudio('msg_' + m.id, m.media_url)" :class="isMyMessage(m) ? 'bg-white text-indigo-600' : 'bg-indigo-600 text-white'" class="w-8 h-8 rounded-full flex items-center justify-center text-xs shrink-0 shadow transition">
                  <i :class="activeAudioId === 'msg_' + m.id && isAudioPlaying ? 'fa-solid fa-pause' : 'fa-solid fa-play ml-0.5'"></i>
                </button>
                <div class="flex-1 space-y-1">
                  <div @click="seekAudio('msg_' + m.id, $event)" @touchmove="handleTouchSeek('msg_' + m.id, $event)" class="h-5 flex items-center gap-0.5 cursor-pointer py-1">
                    <div v-for="(h, idx) in getWaveformBars(m.id)" :key="idx" 
                         :style="{ height: h + '%' }" 
                         :class="(activeAudioId === 'msg_' + m.id && (idx / 28) <= audioProgress) ? (isMyMessage(m) ? 'bg-white' : 'bg-indigo-400') : (isMyMessage(m) ? 'bg-indigo-400/50' : 'bg-slate-700')"
                         class="w-1 rounded-full transition-colors"></div>
                  </div>
                  <div class="flex justify-between text-[9px] font-mono opacity-80">
                    <span>{{ activeAudioId === 'msg_' + m.id ? formatAudioTime(audioCurrentTime) : 'Голосовое' }}</span>
                    <span>{{ activeAudioId === 'msg_' + m.id ? formatAudioTime(audioDuration) : '' }}</span>
                  </div>
                </div>
              </div>
            </div>

            <!-- Фотография -->
            <div v-if="m.message_type === 'IMAGE'" class="pt-1">
              <div @click="openImageLightbox(m.media_url)" class="relative group cursor-pointer overflow-hidden rounded-xl border border-white/10">
                <img :src="m.media_url" class="rounded-xl max-h-52 w-full object-cover group-hover:scale-105 transition duration-200">
                <div class="absolute inset-0 bg-black/30 opacity-0 group-hover:opacity-100 flex items-center justify-center text-white text-base transition">
                  <i class="fa-solid fa-magnifying-glass-plus"></i>
                </div>
              </div>
            </div>

            <div class="text-right text-[10px] leading-none pt-0.5">
              <span v-if="isMyMessage(m)" :class="m.is_read ? 'text-sky-300 font-bold' : 'opacity-60'">
                {{ m.is_read ? '✓✓' : '✓' }}
              </span>
            </div>
          </div>
        </div>
      </div>

      <!-- КНОПКА ВОЗВРАТА К ПОСЛЕДНИМ СООБЩЕНИЯМ (TELEGRAM STYLE) -->
      <button v-if="userScrolledUp" @click="scrollToBottomSmooth" class="fixed bottom-20 right-4 w-9 h-9 rounded-full bg-indigo-600 hover:bg-indigo-500 text-white shadow-xl shadow-indigo-600/40 flex items-center justify-center text-xs transition duration-200 z-50 border border-indigo-400/30 animate-bounce">
        <i class="fa-solid fa-arrow-down"></i>
      </button>

      <!-- ПАНЕЛЬ УПРАВЛЕНИЯ ГОЛОСОВЫМ -->
      <div v-if="recordedVoiceUrl" class="p-3 bg-slate-900 border-t border-slate-800 space-y-2.5 shrink-0">
        <div class="flex justify-between items-center text-[11px] font-bold text-slate-300">
          <span class="flex items-center gap-1.5 text-indigo-400">
            <i class="fa-solid fa-microphone-lines"></i> Прослушать перед отправкой:
          </span>
          <span class="font-mono text-xs text-slate-400">{{ formatTime(recordVoiceSeconds) }}</span>
        </div>

        <div class="flex items-center gap-3 bg-slate-950 p-2.5 rounded-2xl border border-slate-800">
          <button @click="togglePlayAudio('preview_voice', recordedVoiceUrl)" class="w-9 h-9 rounded-full bg-indigo-600 hover:bg-indigo-500 text-white flex items-center justify-center text-xs shrink-0 shadow transition">
            <i :class="activeAudioId === 'preview_voice' && isAudioPlaying ? 'fa-solid fa-pause' : 'fa-solid fa-play ml-0.5'"></i>
          </button>
          <div class="flex-1 space-y-1">
            <div @click="seekAudio('preview_voice', $event)" @touchmove="handleTouchSeek('preview_voice', $event)" class="h-6 flex items-center gap-0.5 cursor-pointer py-1">
              <div v-for="(h, idx) in getWaveformBars(999)" :key="idx" 
                   :style="{ height: h + '%' }" 
                   :class="(activeAudioId === 'preview_voice' && (idx / 28) <= audioProgress) ? 'bg-indigo-400' : 'bg-slate-700'"
                   class="w-1 rounded-full transition-colors"></div>
            </div>
            <div class="flex justify-between text-[10px] text-slate-400 font-mono">
              <span>{{ activeAudioId === 'preview_voice' ? formatAudioTime(audioCurrentTime) : '0:00' }}</span>
              <span>{{ activeAudioId === 'preview_voice' ? formatAudioTime(audioDuration) : formatTime(recordVoiceSeconds) }}</span>
            </div>
          </div>
        </div>

        <div class="grid grid-cols-2 gap-2 pt-0.5">
          <button @click="cancelVoiceRecording" class="py-2.5 rounded-xl bg-red-950/60 hover:bg-red-900/80 border border-red-800/60 text-red-300 text-xs font-bold flex items-center justify-center gap-1.5 transition">
            <i class="fa-solid fa-trash-can"></i> Удалить
          </button>
          <button @click="confirmSendVoice" class="py-2.5 rounded-xl bg-gradient-to-r from-indigo-600 to-indigo-500 hover:from-indigo-500 hover:to-indigo-400 text-white text-xs font-bold flex items-center justify-center gap-1.5 shadow transition">
            <i class="fa-solid fa-paper-plane"></i> Отправить
          </button>
        </div>
      </div>

      <!-- ПАНЕЛЬ АКТИВНОЙ ЗАПИСИ ГОЛОСА -->
      <div v-else-if="isRecordingVoice" class="p-3 bg-slate-900 border-t border-slate-800 flex items-center justify-between shrink-0 animate-pulse">
        <div class="flex items-center gap-2 text-xs font-bold text-red-400">
          <div class="w-3 h-3 rounded-full bg-red-500 animate-ping"></div>
          <span>Идет запись: {{ formatTime(recordVoiceSeconds) }}</span>
        </div>
        <div class="flex items-center gap-2">
          <button @click="cancelVoiceRecording" class="w-8 h-8 rounded-xl bg-slate-800 text-slate-400 flex items-center justify-center text-xs hover:text-red-300">
            <i class="fa-solid fa-xmark"></i>
          </button>
          <button @click="stopVoiceRecording" class="px-3.5 py-2 rounded-xl bg-red-600 text-white text-xs font-bold flex items-center gap-1.5 shadow">
            <i class="fa-solid fa-stop"></i> Завершить
          </button>
        </div>
      </div>

      <!-- ПАНЕЛЬ ТОЛЬКО ДЛЯ ЧТЕНИЯ В АРХИВЕ -->
      <div v-else-if="activeChatTask.status === 'ARCHIVED'" class="p-3 bg-slate-900 border-t border-slate-800 text-center text-xs text-slate-400 font-semibold flex items-center justify-center gap-2 shrink-0">
        <i class="fa-solid fa-lock text-slate-500"></i>
        <span>Задача закрыта в архив. Чат доступен только для чтения.</span>
      </div>

      <!-- СТАНДАРТНАЯ СТРОКА ВВОДА -->
      <div v-else class="p-2.5 border-t border-slate-800/80 bg-slate-900/90 backdrop-blur-md space-y-2 shrink-0">
        <div class="flex items-center gap-1.5">
          <label class="w-9 h-9 rounded-xl bg-slate-800 text-slate-300 flex items-center justify-center cursor-pointer hover:bg-slate-700 text-sm transition">
            <i class="fa-solid fa-paperclip"></i>
            <input type="file" accept="image/*" @change="uploadChatImage" class="hidden">
          </label>

          <button @click="startVoiceRecording" class="w-9 h-9 rounded-xl bg-slate-800 text-slate-300 hover:bg-slate-700 flex items-center justify-center text-sm transition">
            <i class="fa-solid fa-microphone"></i>
          </button>

          <input v-model="chatInput" @keyup.enter="sendChatMessage" placeholder="Сообщение..." class="flex-1 bg-slate-950 border border-slate-700/80 text-xs p-2 rounded-xl text-white outline-none focus:border-indigo-500">

          <button @click="sendChatMessage" class="w-9 h-9 rounded-xl bg-gradient-to-r from-indigo-600 to-indigo-500 text-white flex items-center justify-center text-sm hover:from-indigo-500 hover:to-indigo-400 shadow transition">
            <i class="fa-solid fa-paper-plane"></i>
          </button>
        </div>
      </div>

    </div>

    <!-- LIGHTBOX ДЛЯ ПРЕДОСМОТРА ФОТО -->
    <div v-if="previewImageUrl" class="fixed inset-0 bg-black/95 backdrop-blur-2xl z-[60] flex flex-col justify-between p-4" @click.self="previewImageUrl = null">
      <div class="flex justify-between items-center text-white shrink-0">
        <span class="text-xs font-bold opacity-75">Просмотр фото</span>
        <div class="flex items-center gap-3">
          <a :href="previewImageUrl" download="task_photo.jpg" target="_blank" class="w-9 h-9 rounded-xl bg-white/10 flex items-center justify-center text-white text-sm hover:bg-white/20 transition">
            <i class="fa-solid fa-download"></i>
          </a>
          <button @click="previewImageUrl = null" class="w-9 h-9 rounded-xl bg-white/10 flex items-center justify-center text-white text-sm hover:bg-white/20 transition">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>
      </div>

      <div class="flex-1 flex items-center justify-center overflow-hidden my-4">
        <img :src="previewImageUrl" class="max-h-full max-w-full object-contain rounded-2xl shadow-2xl">
      </div>

      <div class="text-center text-[11px] text-slate-400 shrink-0">
        Нажмите крестик или фон, чтобы закрыть
      </div>
    </div>

  </div>

  <script>
    const { createApp, ref, computed, onMounted, nextTick } = Vue;
    createApp({
      setup() {
        const currentUser = ref(null);
        const loginForm = ref({ username: '', password: '' });
        const isLoggingIn = ref(false);

        const ownerTab = ref('inbox');
        const deputyTab = ref('inbox');
        const empTab = ref('active');
        const isRecording = ref(false);
        const isProcessing = ref(false);
        const recordSeconds = ref(0);
        const textInput = ref('');
        const editDrafts = ref({});
        const sseConnected = ref(false);
        let timerInterval = null;

        const tasks = ref([]);
        const users = ref([]);
        let mediaRecorder = null;
        let audioChunks = [];

        // ЧАТ ПЕРЕМЕННЫЕ
        const activeChatTask = ref(null);
        const chatMessages = ref([]);
        const chatInput = ref('');
        const isChatLoading = ref(false);
        const chatContainer = ref(null);
        const previewImageUrl = ref(null);
        const userScrolledUp = ref(false);

        // ГОЛОСОВОЙ КОНТРОЛЬ ПЕРЕД ОТПРАВКОЙ
        const isRecordingVoice = ref(false);
        const recordVoiceSeconds = ref(0);
        const recordedVoiceBlob = ref(null);
        const recordedVoiceUrl = ref(null);
        let chatVoiceRecorder = null;
        let chatVoiceChunks = [];
        let chatVoiceTimer = null;

        // ЕДИНЫЙ ПЛЕЕР АУДИО С ВОЛНАМИ
        const activeAudioId = ref(null);
        const isAudioPlaying = ref(false);
        const audioCurrentTime = ref(0);
        const audioDuration = ref(0);
        const audioProgress = ref(0);
        let globalAudio = null;

        const roleBadgeTitle = computed(() => {
          if (!currentUser.value) return '';
          if (currentUser.value.role === 'OWNER') return 'Шеф (Владелец)';
          if (currentUser.value.role === 'DEPUTY') return 'Директор (Управление)';
          return 'Исполнитель команды';
        });

        const employeesOnly = computed(() => users.value.filter(u => u.role === 'EMPLOYEE'));

        const inboxTasks = computed(() => tasks.value.filter(t => t.status === 'DRAFT'));
        const activeTasks = computed(() => tasks.value.filter(t => t.status === 'IN_PROGRESS'));
        const reviewTasks = computed(() => tasks.value.filter(t => t.status === 'REVIEW'));
        const archiveTasks = computed(() => tasks.value.filter(t => t.status === 'ARCHIVED'));

        const displayedOwnerTasks = computed(() => {
          if (ownerTab.value === 'inbox') return inboxTasks.value;
          if (ownerTab.value === 'active') return activeTasks.value;
          if (ownerTab.value === 'review') return reviewTasks.value;
          return archiveTasks.value;
        });

        const displayedDeputyTasks = computed(() => {
          if (deputyTab.value === 'inbox') return inboxTasks.value;
          if (deputyTab.value === 'active') return activeTasks.value;
          if (deputyTab.value === 'review') return reviewTasks.value;
          return archiveTasks.value;
        });

        const myTasks = computed(() => tasks.value.filter(t => t.lead_user_id === currentUser.value?.id));
        const myActiveTasks = computed(() => myTasks.value.filter(t => t.status === 'IN_PROGRESS'));
        const myReviewTasks = computed(() => myTasks.value.filter(t => t.status === 'REVIEW'));
        const myArchiveTasks = computed(() => myTasks.value.filter(t => t.status === 'ARCHIVED'));

        const displayedEmpTasks = computed(() => {
          if (empTab.value === 'active') return myActiveTasks.value;
          if (empTab.value === 'review') return myReviewTasks.value;
          return myArchiveTasks.value;
        });

        const isMyMessage = (m) => {
          if (!currentUser.value) return false;
          return m.sender_id === currentUser.value.id;
        };

        const formatLocalDT = (isoStr) => {
          if (!isoStr) return '';
          const d = new Date(isoStr);
          if (isNaN(d.getTime())) return '';
          return d.toLocaleString('ru-RU', {
            day: 'numeric',
            month: 'short',
            hour: '2-digit',
            minute: '2-digit'
          });
        };

        const formatLocalTimeOnly = (isoStr) => {
          if (!isoStr) return '';
          const d = new Date(isoStr);
          if (isNaN(d.getTime())) return '';
          return d.toLocaleTimeString('ru-RU', {
            hour: '2-digit',
            minute: '2-digit'
          });
        };

        const formatAudioTime = (seconds) => {
          if (!seconds || isNaN(seconds)) return '0:00';
          const s = Math.floor(seconds);
          const mins = Math.floor(s / 60);
          const secs = s % 60;
          return `${mins}:${secs.toString().padStart(2, '0')}`;
        };

        const getDefaultLocalDateTimeInput = () => {
          const now = new Date();
          const offset = now.getTimezoneOffset() * 60000;
          return new Date(now.getTime() - offset).toISOString().slice(0, 16);
        };

        const getWaveformBars = (id) => {
          const count = 28;
          const bars = [];
          let seed = ((typeof id === 'number' ? id : 42) * 9301 + 49297) % 233280;
          for (let i = 0; i < count; i++) {
            seed = (seed * 9301 + 49297) % 233280;
            const rnd = seed / 233280;
            const h = Math.floor(25 + Math.sin(i * 0.45) * 20 + rnd * 55);
            bars.push(Math.min(100, Math.max(15, h)));
          }
          return bars;
        };

        const togglePlayAudio = (id, url) => {
          if (activeAudioId.value === id && globalAudio) {
            if (isAudioPlaying.value) {
              globalAudio.pause();
              isAudioPlaying.value = false;
            } else {
              globalAudio.play();
              isAudioPlaying.value = true;
            }
            return;
          }

          if (globalAudio) {
            globalAudio.pause();
            globalAudio = null;
          }

          activeAudioId.value = id;
          isAudioPlaying.value = true;
          audioCurrentTime.value = 0;
          audioProgress.value = 0;

          globalAudio = new Audio(url);
          globalAudio.onloadedmetadata = () => {
            audioDuration.value = globalAudio.duration;
          };
          globalAudio.ontimeupdate = () => {
            if (globalAudio && globalAudio.duration) {
              audioCurrentTime.value = globalAudio.currentTime;
              audioDuration.value = globalAudio.duration;
              audioProgress.value = globalAudio.currentTime / globalAudio.duration;
            }
          };
          globalAudio.onended = () => {
            isAudioPlaying.value = false;
            audioProgress.value = 0;
            audioCurrentTime.value = 0;
          };
          globalAudio.play().catch(e => {
            console.error("Audio error:", e);
            isAudioPlaying.value = false;
          });
        };

        const seekAudio = (id, event) => {
          const rect = event.currentTarget.getBoundingClientRect();
          const clickX = event.clientX - rect.left;
          const ratio = Math.max(0, Math.min(1, clickX / rect.width));

          if (activeAudioId.value === id && globalAudio && globalAudio.duration) {
            globalAudio.currentTime = ratio * globalAudio.duration;
            audioProgress.value = ratio;
            audioCurrentTime.value = globalAudio.currentTime;
          }
        };

        const handleTouchSeek = (id, event) => {
          if (event.touches && event.touches[0]) {
            const touch = event.touches[0];
            const target = event.currentTarget;
            const rect = target.getBoundingClientRect();
            const touchX = touch.clientX - rect.left;
            const ratio = Math.max(0, Math.min(1, touchX / rect.width));

            if (activeAudioId.value === id && globalAudio && globalAudio.duration) {
              globalAudio.currentTime = ratio * globalAudio.duration;
              audioProgress.value = ratio;
              audioCurrentTime.value = globalAudio.currentTime;
            }
          }
        };

        const handleLogin = async () => {
          isLoggingIn.value = true;
          try {
            const fd = new FormData();
            fd.append('username', loginForm.value.username);
            fd.append('password', loginForm.value.password);
            const res = await fetch('/api/login', { method: 'POST', body: fd });
            if (!res.ok) {
              const err = await res.json();
              throw new Error(err.detail || 'Ошибка входа');
            }
            const data = await res.json();
            currentUser.value = data.user;
            localStorage.setItem('task_auth_user', JSON.stringify(data.user));
            await loadData();
          } catch (e) {
            alert('❌ ' + e.message);
          } finally {
            isLoggingIn.value = false;
          }
        };

        const handleLogout = () => {
          if (globalAudio) globalAudio.pause();
          currentUser.value = null;
          localStorage.removeItem('task_auth_user');
          loginForm.value = { username: '', password: '' };
        };

        const loadData = async () => {
          if (!currentUser.value) return;
          try {
            const [rTasks, rUsers] = await Promise.all([
              fetch(`/api/tasks?viewer_user_id=${currentUser.value.id}`).then(r => r.json()),
              fetch('/api/users').then(r => r.json())
            ]);
            tasks.value = rTasks;
            users.value = rUsers;
            
            const empList = rUsers.filter(u => u.role === 'EMPLOYEE');
            const defaultDL = getDefaultLocalDateTimeInput();

            rTasks.forEach(t => {
              if (!editDrafts.value[t.id]) {
                let initialDL = defaultDL;
                if (t.deadline) {
                  const d = new Date(t.deadline);
                  const offset = d.getTimezoneOffset() * 60000;
                  initialDL = new Date(d.getTime() - offset).toISOString().slice(0, 16);
                }
                editDrafts.value[t.id] = {
                  title: t.title,
                  ai_summary: t.ai_summary,
                  definition_of_done: t.definition_of_done,
                  priority: t.priority || 'URGENT',
                  lead_id: t.lead_user_id || empList[0]?.id || 3,
                  deadline: initialDL
                };
              }
            });
          } catch (e) {
            console.error("Ошибка загрузки:", e);
          }
        };

        const setupSSE = () => {
          const evtSource = new EventSource('/api/events');
          evtSource.onopen = () => { sseConnected.value = true; };
          evtSource.onmessage = (event) => {
            loadData();
            if (activeChatTask.value) {
              loadMessages(false);
            }
          };
          evtSource.onerror = () => {
            sseConnected.value = false;
            evtSource.close();
            setTimeout(setupSSE, 5000);
          };
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
                fd.append('user_id', currentUser.value.id);

                try {
                  const res = await fetch('/api/tasks/create-voice', { method: 'POST', body: fd });
                  if (!res.ok) throw new Error('Ошибка создания');
                  await loadData();
                  alert('✅ Поручение создано и передано Жамолиддину!');
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
          fd.append('user_id', currentUser.value.id);
          try {
            const res = await fetch('/api/tasks/create-text', { method: 'POST', body: fd });
            if (!res.ok) throw new Error('Ошибка создания');
            textInput.value = '';
            await loadData();
            alert('✅ Поручение создано и передано Жамолиддину!');
          } catch (e) {
            alert('❌ ' + e.message);
          } finally {
            isProcessing.value = false;
          }
        };

        const assignTask = async (id) => {
          const draft = editDrafts.value[id] || {};
          const fd = new FormData();
          fd.append('lead_id', draft.lead_id || employeesOnly.value[0]?.id || 3);
          fd.append('priority', draft.priority || 'URGENT');
          
          const d = new Date(draft.deadline);
          const deadlineIso = isNaN(d.getTime()) ? new Date().toISOString() : d.toISOString();
          fd.append('deadline', deadlineIso);
          
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

        const submitTaskForReview = async (taskId) => {
          const report = prompt('Опишите итоговый результат выполнения задачи:');
          if (!report) return;
          const fd = new FormData();
          fd.append('sender_id', currentUser.value.id);
          fd.append('sender_name', currentUser.value.full_name);
          fd.append('report_text', report);
          await fetch(`/api/tasks/${taskId}/submit-review`, { method: 'POST', body: fd });
          await loadData();
          alert('🏁 Задача передана Жамолиддину на проверку!');
        };

        const rejectTask = async (taskId) => {
          const reason = prompt('Причина возврата на доработку:');
          if (!reason) return;
          const fd = new FormData();
          fd.append('reason', reason);
          await fetch(`/api/tasks/${taskId}/reject-review`, { method: 'POST', body: fd });
          await loadData();
          alert('Задача возвращена исполнителю.');
        };

        const completeTask = async (id) => {
          if (!confirm('Утвердить выполнение и закрыть задачу в архив?')) return;
          await fetch(`/api/tasks/${id}/complete`, { method: 'POST' });
          await loadData();
        };

        // ОТСЛЕЖИВАНИЕ СКРОЛЛА
        const onChatScroll = () => {
          if (!chatContainer.value) return;
          const { scrollTop, scrollHeight, clientHeight } = chatContainer.value;
          userScrolledUp.value = (scrollHeight - scrollTop - clientHeight) > 60;
        };

        const scrollToBottomSmooth = () => {
          if (chatContainer.value) {
            chatContainer.value.scrollTo({
              top: chatContainer.value.scrollHeight,
              behavior: 'smooth'
            });
            userScrolledUp.value = false;
          }
        };

        const openChat = async (task) => {
          activeChatTask.value = task;
          chatMessages.value = [];
          isChatLoading.value = true;
          userScrolledUp.value = false;
          cancelVoiceRecording();
          document.body.classList.add('overflow-hidden');
          task.unread_count = 0;
          
          await loadMessages(true);
          isChatLoading.value = false;
        };

        const closeChat = () => {
          if (globalAudio) globalAudio.pause();
          activeChatTask.value = null;
          cancelVoiceRecording();
          document.body.classList.remove('overflow-hidden');
          loadData();
        };

        const loadMessages = async (forceScrollBottom = false) => {
          if (!activeChatTask.value || !currentUser.value) return;

          try {
            const res = await fetch(`/api/tasks/${activeChatTask.value.id}/messages?viewer_user_id=${currentUser.value.id}`);
            const newMsgs = await res.json();
            
            const currentJson = JSON.stringify(chatMessages.value);
            const newJson = JSON.stringify(newMsgs);

            if (currentJson !== newJson) {
              chatMessages.value = newMsgs;
              await nextTick();
              
              if (forceScrollBottom || !userScrolledUp.value) {
                if (chatContainer.value) {
                  chatContainer.value.scrollTop = chatContainer.value.scrollHeight;
                }
              }
            }
          } catch (e) {
            console.error(e);
          }
        };

        const sendChatMessage = async () => {
          if (!chatInput.value.trim() || !activeChatTask.value || !currentUser.value) return;
          const fd = new FormData();
          fd.append('sender_id', currentUser.value.id);
          fd.append('sender_role', currentUser.value.role);
          fd.append('sender_name', currentUser.value.full_name);
          fd.append('content', chatInput.value);
          const res = await fetch(`/api/tasks/${activeChatTask.value.id}/messages/text`, { method: 'POST', body: fd });
          if (!res.ok) {
            const err = await res.json();
            alert('❌ ' + (err.detail || 'Ошибка отправки'));
            return;
          }
          chatInput.value = '';
          userScrolledUp.value = false;
          await loadMessages(true);
        };

        const startVoiceRecording = async () => {
          try {
            const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
            chatVoiceRecorder = new MediaRecorder(stream);
            chatVoiceChunks = [];
            recordVoiceSeconds.value = 0;
            isRecordingVoice.value = true;
            recordedVoiceBlob.value = null;
            recordedVoiceUrl.value = null;

            chatVoiceTimer = setInterval(() => recordVoiceSeconds.value++, 1000);

            chatVoiceRecorder.ondataavailable = e => {
              if (e.data.size > 0) chatVoiceChunks.push(e.data);
            };

            chatVoiceRecorder.onstop = () => {
              clearInterval(chatVoiceTimer);
              const blob = new Blob(chatVoiceChunks, { type: chatVoiceRecorder.mimeType || 'audio/webm' });
              recordedVoiceBlob.value = blob;
              recordedVoiceUrl.value = URL.createObjectURL(blob);
              isRecordingVoice.value = false;
            };

            chatVoiceRecorder.start();
          } catch (err) {
            alert('Разрешите доступ к микрофону для записи!');
          }
        };

        const stopVoiceRecording = () => {
          if (chatVoiceRecorder && isRecordingVoice.value) {
            chatVoiceRecorder.stop();
          }
        };

        const cancelVoiceRecording = () => {
          if (chatVoiceTimer) clearInterval(chatVoiceTimer);
          if (chatVoiceRecorder && isRecordingVoice.value) {
            chatVoiceRecorder.stop();
          }
          isRecordingVoice.value = false;
          recordVoiceSeconds.value = 0;
          recordedVoiceBlob.value = null;
          if (recordedVoiceUrl.value) {
            URL.revokeObjectURL(recordedVoiceUrl.value);
            recordedVoiceUrl.value = null;
          }
        };

        const confirmSendVoice = async () => {
          if (!recordedVoiceBlob.value || !activeChatTask.value || !currentUser.value) return;
          const fd = new FormData();
          fd.append('sender_id', currentUser.value.id);
          fd.append('sender_role', currentUser.value.role);
          fd.append('sender_name', currentUser.value.full_name);
          fd.append('audio', recordedVoiceBlob.value, 'voice.webm');
          
          await fetch(`/api/tasks/${activeChatTask.value.id}/messages/voice`, { method: 'POST', body: fd });
          cancelVoiceRecording();
          userScrolledUp.value = false;
          await loadMessages(true);
        };

        const uploadChatImage = async (e) => {
          const file = e.target.files[0];
          if (!file || !activeChatTask.value || !currentUser.value) return;
          const fd = new FormData();
          fd.append('sender_id', currentUser.value.id);
          fd.append('sender_role', currentUser.value.role);
          fd.append('sender_name', currentUser.value.full_name);
          fd.append('file', file);
          await fetch(`/api/tasks/${activeChatTask.value.id}/messages/image`, { method: 'POST', body: fd });
          userScrolledUp.value = false;
          await loadMessages(true);
        };

        const openImageLightbox = (url) => {
          previewImageUrl.value = url;
        };

        const sendRedFlag = async (id) => {
          const r = prompt('В чем причина блокера?');
          if (!r) return;
          const fd = new FormData();
          fd.append('reason', r);
          fd.append('sender_id', currentUser.value.id);
          fd.append('sender_name', currentUser.value.full_name);
          await fetch(`/api/tasks/${id}/red-flag`, { method: 'POST', body: fd });
          await loadData();
          alert('🚨 Red Flag отправлен Жамолиддину!');
        };

        const getDeadlineCountdown = (dlStr) => {
          if (!dlStr) return 'Без срока';
          const diff = new Date(dlStr).getTime() - Date.now();
          if (diff <= 0) return 'Просрочено!';
          const hours = Math.floor(diff / (1000 * 60 * 60));
          const mins = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
          if (hours > 24) {
            const days = Math.floor(hours / 24);
            return `${days}д ${hours % 24}ч`;
          }
          return `${hours}ч ${mins}м`;
        };

        const getDeadlineBadge = (dlStr) => {
          if (!dlStr) return 'bg-slate-800 text-slate-400 border-slate-700';
          const diff = new Date(dlStr).getTime() - Date.now();
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

        const statusBadge = (s) => {
          if (s === 'DRAFT') return 'bg-amber-950 text-amber-300';
          if (s === 'IN_PROGRESS') return 'bg-blue-950 text-blue-300';
          if (s === 'REVIEW') return 'bg-purple-950 text-purple-300 border border-purple-800 animate-pulse';
          return 'bg-emerald-950 text-emerald-300';
        };

        const statusLabel = (s) => {
          if (s === 'DRAFT') return 'Входящие';
          if (s === 'IN_PROGRESS') return 'В работе';
          if (s === 'REVIEW') return 'На проверке';
          return 'Архив';
        };

        const formatRoleName = (r) => r === 'OWNER' ? 'Шеф' : (r === 'DEPUTY' ? 'Директор' : 'Исполнитель');
        const formatTime = (s) => `${Math.floor(s/60).toString().padStart(2,'0')}:${(s%60).toString().padStart(2,'0')}`;

        onMounted(() => {
          const saved = localStorage.getItem('task_auth_user');
          if (saved) {
            try {
              currentUser.value = JSON.parse(saved);
            } catch (e) {}
          }
          loadData();
          setupSSE();
        });

        return {
          currentUser, loginForm, isLoggingIn, ownerTab, deputyTab, empTab, isRecording, isProcessing,
          recordSeconds, textInput, sseConnected, tasks, users, employeesOnly, editDrafts, roleBadgeTitle,
          inboxTasks, activeTasks, reviewTasks, archiveTasks,
          displayedOwnerTasks, displayedDeputyTasks,
          myActiveTasks, myReviewTasks, myArchiveTasks, displayedEmpTasks, isMyMessage,
          activeChatTask, chatMessages, chatInput, isChatLoading, chatContainer, previewImageUrl,
          isRecordingVoice, recordVoiceSeconds, recordedVoiceUrl, userScrolledUp,
          activeAudioId, isAudioPlaying, audioCurrentTime, audioDuration, audioProgress,
          togglePlayAudio, seekAudio, handleTouchSeek, getWaveformBars, formatAudioTime,
          handleLogin, handleLogout, toggleRecord, sendTextTask, assignTask, submitTaskForReview, rejectTask,
          completeTask, openChat, closeChat, sendChatMessage, uploadChatImage, openImageLightbox,
          startVoiceRecording, stopVoiceRecording, cancelVoiceRecording, confirmSendVoice, sendRedFlag,
          onChatScroll, scrollToBottomSmooth,
          formatLocalDT, formatLocalTimeOnly,
          getDeadlineCountdown, getDeadlineBadge, priorityBadge, priorityLabel, statusBadge, statusLabel,
          formatRoleName, formatTime
        };
      }
    }).mount('#app');
  </script>
</body>
</html>"""
