import os
import json
import logging
import sqlite3
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask
from openai import OpenAI
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================
# 基本設定
# =========================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "").strip()
TZ_NAME = os.getenv("TZ", "Asia/Taipei")
PORT = int(os.getenv("PORT", "10000"))
DB_PATH = os.getenv("DB_PATH", "reminders.db")

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN 未設定")
if not OWNER_ID:
    raise ValueError("OWNER_ID 未設定")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY 未設定")
if not OPENAI_MODEL:
    raise ValueError("OPENAI_MODEL 未設定")

tz = ZoneInfo(TZ_NAME)
client = OpenAI(api_key=OPENAI_API_KEY)
flask_app = Flask(__name__)


# =========================
# Flask 健康檢查
# =========================
@flask_app.route("/")
def healthcheck():
    return "Bot is running v3.2", 200


def run_web_server():
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


# =========================
# 資料庫
# =========================
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            schedule_type TEXT NOT NULL,         -- single / daily
            remind_at TEXT,                      -- single 用 ISO datetime
            time_local TEXT,                     -- daily 用 HH:MM
            message TEXT NOT NULL,
            sent INTEGER NOT NULL DEFAULT 0,     -- single 用
            last_sent_date TEXT,                 -- daily 用 YYYY-MM-DD
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def add_single_reminder(chat_id: int, remind_at_iso: str, message: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO reminders (
            chat_id, schedule_type, remind_at, time_local, message,
            sent, last_sent_date, created_at
        ) VALUES (?, 'single', ?, NULL, ?, 0, NULL, ?)
    """, (chat_id, remind_at_iso, message, now_local_iso()))
    conn.commit()
    reminder_id = cur.lastrowid
    conn.close()
    return reminder_id


def add_daily_reminder(chat_id: int, time_local: str, message: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO reminders (
            chat_id, schedule_type, remind_at, time_local, message,
            sent, last_sent_date, created_at
        ) VALUES (?, 'daily', NULL, ?, ?, 0, NULL, ?)
    """, (chat_id, time_local, message, now_local_iso()))
    conn.commit()
    reminder_id = cur.lastrowid
    conn.close()
    return reminder_id


def list_reminders(chat_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, schedule_type, remind_at, time_local, message, sent, last_sent_date
        FROM reminders
        WHERE chat_id = ?
        ORDER BY
            CASE WHEN schedule_type = 'single' THEN 0 ELSE 1 END,
            remind_at ASC,
            time_local ASC
    """, (chat_id,))
    rows = cur.fetchall()
    conn.close()
    return rows


def delete_reminder(chat_id: int, reminder_id: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM reminders
        WHERE id = ? AND chat_id = ?
    """, (reminder_id, chat_id))
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def get_due_single_reminders():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, chat_id, remind_at, message
        FROM reminders
        WHERE schedule_type = 'single'
          AND sent = 0
    """)
    rows = cur.fetchall()
    conn.close()

    due = []
    now_dt = now_local_dt()
    for row in rows:
        try:
            remind_dt = datetime.fromisoformat(row["remind_at"]).astimezone(tz)
            if remind_dt <= now_dt:
                due.append(row)
        except Exception:
            logger.exception("single reminder parse error: id=%s", row["id"])
    return due


def get_due_daily_reminders():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, chat_id, time_local, message, last_sent_date
        FROM reminders
        WHERE schedule_type = 'daily'
    """)
    rows = cur.fetchall()
    conn.close()

    due = []
    now_dt = now_local_dt()
    today = now_dt.strftime("%Y-%m-%d")
    current_hm = now_dt.strftime("%H:%M")

    for row in rows:
        try:
            target_hm = row["time_local"]
            if current_hm >= target_hm and row["last_sent_date"] != today:
                due.append(row)
        except Exception:
            logger.exception("daily reminder parse error: id=%s", row["id"])
    return due


def mark_single_sent(reminder_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE reminders
        SET sent = 1
        WHERE id = ?
    """, (reminder_id,))
    conn.commit()
    conn.close()


def mark_daily_sent_today(reminder_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE reminders
        SET last_sent_date = ?
        WHERE id = ?
    """, (today_local_str(), reminder_id))
    conn.commit()
    conn.close()


# =========================
# 時間工具
# =========================
def now_local_dt() -> datetime:
    return datetime.now(tz)


def now_local_iso() -> str:
    return now_local_dt().isoformat()


def today_local_str() -> str:
    return now_local_dt().strftime("%Y-%m-%d")


# =========================
# 權限
# =========================
def is_owner(update: Update) -> bool:
    user = update.effective_user
    return user is not None and user.id == OWNER_ID


async def owner_only(update: Update) -> bool:
    if not is_owner(update):
        if update.message:
            await update.message.reply_text("此 bot 為私人使用。")
        return False
    return True


# =========================
# OpenAI 解析
# =========================
REMINDER_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "intent": {
            "type": "string",
            "enum": ["create_reminder", "unsupported"]
        },
        "schedule_type": {
            "type": ["string", "null"],
            "enum": ["single", "daily", None]
        },
        "datetime_local": {
            "type": ["string", "null"],
            "description": "單次提醒時間，格式必須是 YYYY-MM-DD HH:MM，使用 Asia/Taipei"
        },
        "time_local": {
            "type": ["string", "null"],
            "description": "每日提醒時間，格式必須是 HH:MM，使用 Asia/Taipei"
        },
        "message": {
            "type": ["string", "null"]
        },
        "confidence": {
            "type": "number"
        },
        "reason": {
            "type": "string"
        }
    },
    "required": [
        "intent",
        "schedule_type",
        "datetime_local",
        "time_local",
        "message",
        "confidence",
        "reason"
    ]
}


def parse_user_text_with_ai(user_text: str) -> dict:
    now_str = now_local_dt().strftime("%Y-%m-%d %H:%M")
    developer_prompt = f"""
你是一個提醒解析器，只能輸出符合 JSON schema 的資料，不要輸出其他文字。

目前時區：Asia/Taipei
目前本地時間：{now_str}

任務：
把使用者輸入解析成提醒資料。

規則：
1. 若明確是建立提醒，intent = "create_reminder"
2. 若不是提醒、資訊不足、時間太模糊，intent = "unsupported"
3. schedule_type 只能是 "single" 或 "daily" 或 null
4. 若是單次提醒，datetime_local 要填 YYYY-MM-DD HH:MM，time_local = null
5. 若是每日提醒，time_local 要填 HH:MM，datetime_local = null
6. message 要保留提醒重點，例如「吃早餐」「記帳」
7. 若使用者寫「提醒我」但沒給可執行時間，請標為 unsupported
8. 「明天早上八點提醒我吃早餐」=> single
9. 「每天晚上十點提醒我記帳」=> daily
10. confidence 範圍 0 到 1
11. reason 用一句簡短中文說明你的判斷
""".strip()

    response = client.responses.create(
        model=OPENAI_MODEL,
        input=[
            {"role": "developer", "content": developer_prompt},
            {"role": "user", "content": user_text},
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "reminder_parse",
                "schema": REMINDER_SCHEMA,
                "strict": True,
            }
        },
    )

    raw = response.output_text
    data = json.loads(raw)
    return data


def validate_ai_result(data: dict) -> tuple[bool, str, dict | None]:
    intent = data.get("intent")
    schedule_type = data.get("schedule_type")
    dt_str = data.get("datetime_local")
    time_local = data.get("time_local")
    message = (data.get("message") or "").strip()

    if intent != "create_reminder":
        return False, "我目前只會建立提醒，例如：明天早上8點提醒我吃早餐", None

    if schedule_type not in ("single", "daily"):
        return False, "提醒類型判斷失敗。", None

    if not message:
        return False, "提醒內容判斷失敗。", None

    if schedule_type == "single":
        if not dt_str:
            return False, "缺少單次提醒時間。", None
        try:
            remind_dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=tz)
        except ValueError:
            return False, "時間格式錯誤，AI 沒有成功解析。", None

        if remind_dt <= now_local_dt():
            return False, "提醒時間必須晚於現在。", None

        return True, "", {
            "schedule_type": "single",
            "remind_at_iso": remind_dt.isoformat(),
            "display_time": remind_dt.strftime("%Y-%m-%d %H:%M"),
            "message": message,
        }

    if schedule_type == "daily":
        if not time_local:
            return False, "缺少每日提醒時間。", None
        try:
            datetime.strptime(time_local, "%H:%M")
        except ValueError:
            return False, "每日提醒時間格式錯誤。", None

        return True, "", {
            "schedule_type": "daily",
            "time_local": time_local,
            "message": message,
        }

    return False, "未知錯誤。", None


# =========================
# 指令
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await owner_only(update):
        return

    text = (
        "✅ AI 提醒 bot 已啟動（v3.2）\n\n"
        "你可以直接傳：\n"
        "明天早上8點提醒我吃早餐\n"
        "今天晚上10點提醒我記帳\n"
        "每天早上7點提醒我吃藥\n\n"
        "其他指令：\n"
        "/list 查看提醒\n"
        "/delete 編號 刪除提醒\n"
        "/ping 測試 bot\n"
        "/now 顯示目前時間"
    )
    await update.message.reply_text(text)


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await owner_only(update):
        return
    await update.message.reply_text("pong ✅")


async def now_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await owner_only(update):
        return
    await update.message.reply_text(
        f"目前時間：{now_local_dt().strftime('%Y-%m-%d %H:%M:%S')} ({TZ_NAME})"
    )


async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await owner_only(update):
        return

    rows = list_reminders(update.effective_chat.id)
    if not rows:
        await update.message.reply_text("目前沒有提醒。")
        return

    lines = ["📋 提醒清單"]
    for row in rows:
        if row["schedule_type"] == "single":
            dt = datetime.fromisoformat(row["remind_at"]).astimezone(tz)
            status = "✅ 已送出" if row["sent"] else "⏳ 待提醒"
            lines.append(
                f'{row["id"]}. [單次] {dt.strftime("%Y-%m-%d %H:%M")} | {row["message"]} | {status}'
            )
        else:
            last_sent = row["last_sent_date"] or "尚未送出"
            lines.append(
                f'{row["id"]}. [每日] {row["time_local"]} | {row["message"]} | 上次送出：{last_sent}'
            )

    await update.message.reply_text("\n".join(lines))


async def delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await owner_only(update):
        return

    if not context.args:
        await update.message.reply_text("請用：/delete 編號")
        return

    try:
        reminder_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("提醒編號必須是數字。")
        return

    ok = delete_reminder(update.effective_chat.id, reminder_id)
    if ok:
        await update.message.reply_text(f"✅ 已刪除提醒 {reminder_id}")
    else:
        await update.message.reply_text("找不到這個提醒編號。")


# =========================
# 一般文字：交給 AI 解析
# =========================
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await owner_only(update):
        return

    user_text = (update.message.text or "").strip()
    if not user_text:
        return

    try:
        ai_result = parse_user_text_with_ai(user_text)
        ok, err, normalized = validate_ai_result(ai_result)

        if not ok:
            await update.message.reply_text(err)
            return

        if normalized["schedule_type"] == "single":
            reminder_id = add_single_reminder(
                chat_id=update.effective_chat.id,
                remind_at_iso=normalized["remind_at_iso"],
                message=normalized["message"],
            )
            await update.message.reply_text(
                "✅ 已建立單次提醒\n"
                f"編號：{reminder_id}\n"
                f"時間：{normalized['display_time']}\n"
                f"內容：{normalized['message']}"
            )
            return

        if normalized["schedule_type"] == "daily":
            reminder_id = add_daily_reminder(
                chat_id=update.effective_chat.id,
                time_local=normalized["time_local"],
                message=normalized["message"],
            )
            await update.message.reply_text(
                "✅ 已建立每日提醒\n"
                f"編號：{reminder_id}\n"
                f"每天：{normalized['time_local']}\n"
                f"內容：{normalized['message']}"
            )
            return

        await update.message.reply_text("解析成功，但建立提醒失敗。")

    except Exception as e:
        logger.exception("handle_text error: %s", e)
        await update.message.reply_text("建立提醒失敗，請稍後再試。")


# =========================
# 背景工作：送提醒
# =========================
async def reminder_worker(context: ContextTypes.DEFAULT_TYPE):
    try:
        # 單次提醒
        single_rows = get_due_single_reminders()
        for row in single_rows:
            try:
                dt = datetime.fromisoformat(row["remind_at"]).astimezone(tz)
                text = (
                    "⏰ 提醒時間到！\n"
                    f'時間：{dt.strftime("%Y-%m-%d %H:%M")}\n'
                    f'內容：{row["message"]}'
                )
                await context.bot.send_message(chat_id=row["chat_id"], text=text)
                mark_single_sent(row["id"])
            except Exception:
                logger.exception("send single reminder failed: id=%s", row["id"])

        # 每日提醒
        daily_rows = get_due_daily_reminders()
        for row in daily_rows:
            try:
                text = (
                    "⏰ 每日提醒\n"
                    f'時間：{row["time_local"]}\n'
                    f'內容：{row["message"]}'
                )
                await context.bot.send_message(chat_id=row["chat_id"], text=text)
                mark_daily_sent_today(row["id"])
            except Exception:
                logger.exception("send daily reminder failed: id=%s", row["id"])

    except Exception as e:
        logger.exception("reminder_worker error: %s", e)


async def post_init(application: Application):
    application.job_queue.run_repeating(
        reminder_worker,
        interval=30,
        first=10,
        name="reminder_worker",
    )
    logger.info("reminder_worker started")


# =========================
# 主程式
# =========================
def build_application() -> Application:
    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("ping", ping))
    application.add_handler(CommandHandler("now", now_command))
    application.add_handler(CommandHandler("list", list_command))
    application.add_handler(CommandHandler("delete", delete_command))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text)
    )

    return application


def main():
    init_db()

    web_thread = threading.Thread(target=run_web_server, daemon=True)
    web_thread.start()

    application = build_application()
    application.run_polling(
        poll_interval=1.5,
        timeout=20,
        drop_pending_updates=False,
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()
