import os
import json
import uuid
import logging
import sqlite3
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Flask
from openai import OpenAI, RateLimitError, AuthenticationError, BadRequestError, APIConnectionError, APITimeoutError
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

SLOT_MAP = {
    "morning": ["06:00", "08:00", "10:00"],
    "noon": ["11:00", "12:00", "13:00"],
    "afternoon": ["14:00", "16:00", "18:00"],
    "evening": ["18:00", "20:00", "22:00"],
}

SLOT_LABEL_MAP = {
    "morning": "早上",
    "noon": "中午",
    "afternoon": "下午",
    "evening": "晚上",
}


# =========================
# Flask 健康檢查
# =========================
@flask_app.route("/")
def healthcheck():
    return "Bot is running v3.3", 200


def run_web_server():
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


# =========================
# 時間工具
# =========================
def now_local_dt():
    return datetime.now(tz)


def now_local_iso():
    return now_local_dt().isoformat()


def today_local_str():
    return now_local_dt().strftime("%Y-%m-%d")


def parse_local_datetime(date_str: str, time_str: str):
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)


def choose_date_for_time(time_str: str):
    candidate_today = parse_local_datetime(today_local_str(), time_str)
    if candidate_today > now_local_dt():
        return today_local_str()
    return (now_local_dt() + timedelta(days=1)).strftime("%Y-%m-%d")


def choose_date_for_slot(slot: str):
    today = today_local_str()
    now_dt = now_local_dt()
    for t in SLOT_MAP.get(slot, []):
        if parse_local_datetime(today, t) > now_dt:
            return today
    return (now_dt + timedelta(days=1)).strftime("%Y-%m-%d")


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
        CREATE TABLE IF NOT EXISTS reminder_events (
            event_id TEXT PRIMARY KEY,
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            event_date TEXT NOT NULL,            -- YYYY-MM-DD
            event_time TEXT,                     -- HH:MM or NULL
            slot TEXT,                           -- morning/noon/afternoon/evening or NULL
            message TEXT NOT NULL,
            source_text TEXT,
            created_at TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS reminder_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            remind_at TEXT NOT NULL,             -- ISO datetime
            label TEXT NOT NULL,                 -- 2小時前 / 1小時前 / 30分鐘前 / 事件時間 / 時段提醒
            sent INTEGER NOT NULL DEFAULT 0,
            sent_at TEXT,
            FOREIGN KEY(event_id) REFERENCES reminder_events(event_id)
        )
    """)

    conn.commit()
    conn.close()


# =========================
# 權限
# =========================
def is_owner(update: Update):
    user = update.effective_user
    return user is not None and user.id == OWNER_ID


async def owner_only(update: Update):
    if not is_owner(update):
        if update.message:
            await update.message.reply_text("此 bot 為私人使用。")
        return False
    return True


# =========================
# 建立 / 刪除 / 查詢事件
# =========================
def create_exact_event(chat_id: int, user_id: int, event_date: str, event_time: str, message: str, source_text: str):
    base_dt = parse_local_datetime(event_date, event_time)
    if base_dt <= now_local_dt():
        raise ValueError("事件時間必須晚於現在。")

    event_id = str(uuid.uuid4())

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO reminder_events (
            event_id, chat_id, user_id, event_date, event_time, slot,
            message, source_text, created_at, is_active
        ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, 1)
    """, (
        event_id, chat_id, user_id, event_date, event_time,
        message, source_text, now_local_iso()
    ))

    offsets = [
        (120, "2小時前"),
        (60, "1小時前"),
        (30, "30分鐘前"),
        (0, "事件時間"),
    ]

    created_times = []
    for minutes_before, label in offsets:
        remind_dt = base_dt - timedelta(minutes=minutes_before)
        if remind_dt > now_local_dt():
            cur.execute("""
                INSERT INTO reminder_notifications (
                    event_id, remind_at, label, sent, sent_at
                ) VALUES (?, ?, ?, 0, NULL)
            """, (event_id, remind_dt.isoformat(), label))
            created_times.append((label, remind_dt.strftime("%Y-%m-%d %H:%M")))

    conn.commit()
    conn.close()

    if not created_times:
        raise ValueError("這個事件沒有可建立的未來提醒。")

    return event_id, created_times


def create_slot_event(chat_id: int, user_id: int, event_date: str, slot: str, message: str, source_text: str):
    if slot not in SLOT_MAP:
        raise ValueError("不支援的時段。")

    event_id = str(uuid.uuid4())

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO reminder_events (
            event_id, chat_id, user_id, event_date, event_time, slot,
            message, source_text, created_at, is_active
        ) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, 1)
    """, (
        event_id, chat_id, user_id, event_date, slot,
        message, source_text, now_local_iso()
    ))

    created_times = []
    for t in SLOT_MAP[slot]:
        remind_dt = parse_local_datetime(event_date, t)
        if remind_dt > now_local_dt():
            cur.execute("""
                INSERT INTO reminder_notifications (
                    event_id, remind_at, label, sent, sent_at
                ) VALUES (?, ?, ?, 0, NULL)
            """, (event_id, remind_dt.isoformat(), "時段提醒"))
            created_times.append(remind_dt.strftime("%Y-%m-%d %H:%M"))

    conn.commit()
    conn.close()

    if not created_times:
        raise ValueError("這個時段已經沒有可建立的未來提醒。")

    return event_id, created_times


def cancel_events_by_text(chat_id: int, user_id: int, keyword: str, date_local: str | None = None):
    conn = get_conn()
    cur = conn.cursor()

    params = [chat_id, user_id, f"%{keyword}%"]
    sql = """
        SELECT event_id, event_date, event_time, slot, message
        FROM reminder_events
        WHERE chat_id = ?
          AND user_id = ?
          AND is_active = 1
          AND message LIKE ?
    """

    if date_local:
        sql += " AND event_date = ?"
        params.append(date_local)
    else:
        sql += " AND event_date >= ?"
        params.append(today_local_str())

    cur.execute(sql, params)
    rows = cur.fetchall()

    if not rows:
        conn.close()
        return 0, []

    event_ids = [row["event_id"] for row in rows]
    deleted_summaries = []

    for row in rows:
        if row["event_time"]:
            summary = f'{row["event_date"]} {row["event_time"]}｜{row["message"]}'
        else:
            summary = f'{row["event_date"]} {SLOT_LABEL_MAP.get(row["slot"], row["slot"])}｜{row["message"]}'
        deleted_summaries.append(summary)

    cur.executemany("DELETE FROM reminder_notifications WHERE event_id = ?", [(eid,) for eid in event_ids])
    cur.executemany("DELETE FROM reminder_events WHERE event_id = ?", [(eid,) for eid in event_ids])

    conn.commit()
    conn.close()

    return len(event_ids), deleted_summaries


def delete_event_by_code(chat_id: int, code: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT event_id, event_date, event_time, slot, message
        FROM reminder_events
        WHERE chat_id = ?
          AND is_active = 1
          AND substr(event_id, 1, 8) = ?
    """, (chat_id, code))
    row = cur.fetchone()

    if not row:
        conn.close()
        return False, None

    event_id = row["event_id"]
    if row["event_time"]:
        summary = f'{row["event_date"]} {row["event_time"]}｜{row["message"]}'
    else:
        summary = f'{row["event_date"]} {SLOT_LABEL_MAP.get(row["slot"], row["slot"])}｜{row["message"]}'

    cur.execute("DELETE FROM reminder_notifications WHERE event_id = ?", (event_id,))
    cur.execute("DELETE FROM reminder_events WHERE event_id = ?", (event_id,))

    conn.commit()
    conn.close()

    return True, summary


def list_active_events(chat_id: int):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            e.event_id,
            e.event_date,
            e.event_time,
            e.slot,
            e.message,
            MIN(CASE WHEN n.sent = 0 THEN n.remind_at END) AS next_remind_at,
            SUM(CASE WHEN n.sent = 0 THEN 1 ELSE 0 END) AS pending_count
        FROM reminder_events e
        LEFT JOIN reminder_notifications n ON e.event_id = n.event_id
        WHERE e.chat_id = ?
          AND e.is_active = 1
        GROUP BY e.event_id, e.event_date, e.event_time, e.slot, e.message
        ORDER BY e.event_date ASC, e.event_time ASC
    """, (chat_id,))
    rows = cur.fetchall()
    conn.close()
    return rows


# =========================
# OpenAI 解析
# =========================
REMINDER_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "intent": {
            "type": "string",
            "enum": ["create_reminder", "cancel_reminder", "unsupported"]
        },
        "date_local": {
            "type": ["string", "null"],
            "description": "YYYY-MM-DD，若使用者沒提到日期可為 null"
        },
        "time_local": {
            "type": ["string", "null"],
            "description": "HH:MM，若使用者沒提到明確時間可為 null"
        },
        "slot": {
            "type": ["string", "null"],
            "enum": ["morning", "noon", "afternoon", "evening", None]
        },
        "message": {
            "type": ["string", "null"],
            "description": "建立提醒時填事件內容；取消提醒時填用於搜尋的關鍵字"
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
        "date_local",
        "time_local",
        "slot",
        "message",
        "confidence",
        "reason"
    ]
}


def parse_user_text_with_ai(user_text: str):
    now_str = now_local_dt().strftime("%Y-%m-%d %H:%M")
    developer_prompt = f"""
你是一個 Telegram 提醒解析器，只能輸出符合 JSON schema 的資料，不要輸出其他文字。

目前時區：Asia/Taipei
目前本地時間：{now_str}

任務：
把使用者輸入解析成「建立提醒」或「取消提醒」。

規則：
1. intent 只能是：
   - create_reminder
   - cancel_reminder
   - unsupported

2. 若使用者是在建立提醒：
   - 有明確時間時，填 date_local 與 time_local
   - 沒有明確時間，但有時段（早上/中午/下午/晚上）時，填 slot
   - message 保留精簡事件內容，例如：吃早餐、開會、打球、寫報告

3. 若使用者是在取消提醒：
   - message 盡量提取成可搜尋的關鍵字，例如：
     取消明天早餐 -> 早餐
     取消今天開會 -> 開會
   - 若有日期就填 date_local，沒有可為 null
   - time_local 通常為 null

4. 若只有「早上吃早餐」這類沒有日期的建立提醒：
   - date_local 可為 null，程式會自己補最近可執行日期

5. slot 對應：
   - 早上 -> morning
   - 中午 -> noon
   - 下午 -> afternoon
   - 晚上 -> evening

6. 範例：
   - 明天早上7點吃早餐
     => create_reminder, date_local=明天日期, time_local=07:00, slot=null, message=吃早餐
   - 早上吃早餐
     => create_reminder, date_local=null, time_local=null, slot=morning, message=吃早餐
   - 取消明天早餐
     => cancel_reminder, date_local=明天日期, time_local=null, slot=null, message=早餐

7. 若資訊不足或不是提醒相關，intent = unsupported

8. confidence 範圍 0 到 1
9. reason 用一句簡短中文說明判斷
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

    return json.loads(response.output_text)


def validate_ai_result(data: dict):
    intent = data.get("intent")
    date_local = data.get("date_local")
    time_local = data.get("time_local")
    slot = data.get("slot")
    message = (data.get("message") or "").strip()

    if intent == "unsupported":
        return False, "我目前支援建立提醒或取消提醒，例如：明天早上7點吃早餐、取消明天早餐", None

    if not message:
        return False, "我沒有抓到提醒內容。", None

    # 建立提醒
    if intent == "create_reminder":
        if time_local:
            try:
                datetime.strptime(time_local, "%H:%M")
            except ValueError:
                return False, "時間格式解析失敗。", None

            if date_local:
                try:
                    datetime.strptime(date_local, "%Y-%m-%d")
                except ValueError:
                    return False, "日期格式解析失敗。", None
            else:
                date_local = choose_date_for_time(time_local)

            event_dt = parse_local_datetime(date_local, time_local)
            if event_dt <= now_local_dt():
                return False, "事件時間必須晚於現在。", None

            return True, "", {
                "intent": "create_exact",
                "event_date": date_local,
                "event_time": time_local,
                "message": message,
            }

        if slot:
            if slot not in SLOT_MAP:
                return False, "時段解析失敗。", None

            if date_local:
                try:
                    datetime.strptime(date_local, "%Y-%m-%d")
                except ValueError:
                    return False, "日期格式解析失敗。", None
            else:
                date_local = choose_date_for_slot(slot)

            return True, "", {
                "intent": "create_slot",
                "event_date": date_local,
                "slot": slot,
                "message": message,
            }

        return False, "我需要明確時間，或至少有早上/中午/下午/晚上。", None

    # 取消提醒
    if intent == "cancel_reminder":
        if date_local:
            try:
                datetime.strptime(date_local, "%Y-%m-%d")
            except ValueError:
                return False, "取消提醒的日期格式解析失敗。", None

        return True, "", {
            "intent": "cancel",
            "event_date": date_local,
            "message": message,
        }

    return False, "解析失敗。", None


# =========================
# 指令
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await owner_only(update):
        return

    text = (
        "✅ AI 提醒 bot 已啟動（v3.3）\n\n"
        "你可以直接傳：\n"
        "明天早上7點吃早餐\n"
        "明天下午3點開會\n"
        "早上打球\n"
        "取消明天早餐\n\n"
        "規則：\n"
        "1. 有明確時間：自動做 2小時前 / 1小時前 / 30分鐘前 / 當下提醒\n"
        "2. 只有時段：套用預設時段提醒\n\n"
        "其他指令：\n"
        "/list 查看事件\n"
        "/delete 事件代碼 刪除整組提醒\n"
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

    rows = list_active_events(update.effective_chat.id)
    if not rows:
        await update.message.reply_text("目前沒有事件提醒。")
        return

    lines = ["📋 事件清單"]
    for row in rows:
        code = row["event_id"][:8]
        pending_count = row["pending_count"] or 0

        if row["event_time"]:
            event_desc = f'{row["event_date"]} {row["event_time"]}'
        else:
            event_desc = f'{row["event_date"]} {SLOT_LABEL_MAP.get(row["slot"], row["slot"])}'

        if row["next_remind_at"]:
            next_dt = datetime.fromisoformat(row["next_remind_at"]).astimezone(tz)
            next_text = next_dt.strftime("%Y-%m-%d %H:%M")
        else:
            next_text = "無待送提醒"

        lines.append(
            f"{code}｜{event_desc}｜{row['message']}｜待送 {pending_count} 次｜下次 {next_text}"
        )

    await update.message.reply_text("\n".join(lines))


async def delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await owner_only(update):
        return

    if not context.args:
        await update.message.reply_text("請用：/delete 事件代碼\n例如：/delete 1a2b3c4d")
        return

    code = context.args[0].strip()
    ok, summary = delete_event_by_code(update.effective_chat.id, code)

    if ok:
        await update.message.reply_text(f"✅ 已刪除事件\n{summary}")
    else:
        await update.message.reply_text("找不到這個事件代碼。")


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

        # 建立：明確時間
        if normalized["intent"] == "create_exact":
            event_id, created_times = create_exact_event(
                chat_id=update.effective_chat.id,
                user_id=update.effective_user.id,
                event_date=normalized["event_date"],
                event_time=normalized["event_time"],
                message=normalized["message"],
                source_text=user_text,
            )

            lines = [
                "✅ 已建立整組提醒",
                f"事件代碼：{event_id[:8]}",
                f"事件：{normalized['event_date']} {normalized['event_time']}｜{normalized['message']}",
                "提醒時間：",
            ]
            for label, t in created_times:
                lines.append(f"- {label}：{t}")

            await update.message.reply_text("\n".join(lines))
            return

        # 建立：只有時段
        if normalized["intent"] == "create_slot":
            event_id, created_times = create_slot_event(
                chat_id=update.effective_chat.id,
                user_id=update.effective_user.id,
                event_date=normalized["event_date"],
                slot=normalized["slot"],
                message=normalized["message"],
                source_text=user_text,
            )

            lines = [
                "✅ 已建立整組提醒",
                f"事件代碼：{event_id[:8]}",
                f"事件：{normalized['event_date']} {SLOT_LABEL_MAP.get(normalized['slot'], normalized['slot'])}｜{normalized['message']}",
                "提醒時間：",
            ]
            for t in created_times:
                lines.append(f"- {t}")

            await update.message.reply_text("\n".join(lines))
            return

        # 取消
        if normalized["intent"] == "cancel":
            count, deleted_summaries = cancel_events_by_text(
                chat_id=update.effective_chat.id,
                user_id=update.effective_user.id,
                keyword=normalized["message"],
                date_local=normalized["event_date"],
            )

            if count == 0:
                await update.message.reply_text("找不到符合的事件提醒。")
                return

            lines = [f"✅ 已取消 {count} 組事件提醒："]
            lines.extend([f"- {x}" for x in deleted_summaries[:10]])
            if len(deleted_summaries) > 10:
                lines.append("...")

            await update.message.reply_text("\n".join(lines))
            return

        await update.message.reply_text("解析成功，但沒有執行到對應動作。")

    except RateLimitError as e:
        logger.exception("OpenAI RateLimitError")
        await update.message.reply_text(f"OpenAI 額度或速率限制：{str(e)[:250]}")
    except AuthenticationError as e:
        logger.exception("OpenAI AuthenticationError")
        await update.message.reply_text(f"OpenAI 金鑰有問題：{str(e)[:250]}")
    except BadRequestError as e:
        logger.exception("OpenAI BadRequestError")
        await update.message.reply_text(f"OpenAI 請求格式錯誤：{str(e)[:250]}")
    except (APIConnectionError, APITimeoutError) as e:
        logger.exception("OpenAI connection/timeout error")
        await update.message.reply_text(f"OpenAI 連線逾時或失敗：{str(e)[:250]}")
    except ValueError as e:
        logger.exception("ValueError in handle_text")
        await update.message.reply_text(str(e))
    except Exception as e:
        logger.exception("handle_text error")
        await update.message.reply_text(f"建立提醒失敗：{type(e).__name__}: {str(e)[:300]}")


# =========================
# 背景工作：送提醒
# =========================
async def reminder_worker(context: ContextTypes.DEFAULT_TYPE):
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                n.id,
                n.event_id,
                n.remind_at,
                n.label,
                e.chat_id,
                e.event_date,
                e.event_time,
                e.slot,
                e.message
            FROM reminder_notifications n
            JOIN reminder_events e ON n.event_id = e.event_id
            WHERE n.sent = 0
              AND e.is_active = 1
        """)
        rows = cur.fetchall()

        due_ids = []
        for row in rows:
            remind_dt = datetime.fromisoformat(row["remind_at"]).astimezone(tz)
            if remind_dt <= now_local_dt():
                try:
                    if row["event_time"]:
                        event_desc = f'{row["event_date"]} {row["event_time"]}'
                    else:
                        event_desc = f'{row["event_date"]} {SLOT_LABEL_MAP.get(row["slot"], row["slot"])}'

                    text = (
                        f"⏰ {row['label']}提醒\n"
                        f"事件：{row['message']}\n"
                        f"原定時間：{event_desc}"
                    )

                    await context.bot.send_message(chat_id=row["chat_id"], text=text)
                    due_ids.append(row["id"])
                except Exception:
                    logger.exception("send reminder failed: notification_id=%s", row["id"])

        if due_ids:
            cur.executemany("""
                UPDATE reminder_notifications
                SET sent = 1, sent_at = ?
                WHERE id = ?
            """, [(now_local_iso(), x) for x in due_ids])
            conn.commit()

        conn.close()

    except Exception:
        logger.exception("reminder_worker error")


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
def build_application():
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
