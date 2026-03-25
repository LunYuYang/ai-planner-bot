import os
import re
import json
import time
import uuid
import asyncio
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Flask
from telegram import Update, Bot
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running!"


# =========================
# Basic config
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ALLOWED_USERS = [7243450850]
TIMEZONE = ZoneInfo("Asia/Taipei")
DATA_FILE = "reminders.json"

MORNING_REMINDER_HOURS = [6, 8, 10]
MORNING_END_HOUR = 11  # 超過 11:00 視為早上時段結束


# =========================
# Storage helpers
# =========================
data_lock = threading.Lock()


def now_local() -> datetime:
    return datetime.now(TIMEZONE)


def ensure_data_file():
    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)


def load_reminders():
    ensure_data_file()
    with data_lock:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)


def save_reminders(reminders):
    with data_lock:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(reminders, f, ensure_ascii=False, indent=2)


def add_reminder(reminder):
    reminders = load_reminders()
    reminders.append(reminder)
    save_reminders(reminders)


def update_reminders(reminders):
    save_reminders(reminders)


# =========================
# Authorization
# =========================
def is_allowed(update: Update) -> bool:
    user = update.effective_user
    return bool(user and user.id in ALLOWED_USERS)


# =========================
# Parsing helpers
# =========================
def tomorrow_date_str():
    return (now_local() + timedelta(days=1)).strftime("%Y-%m-%d")


def format_dt(dt_str: str) -> str:
    dt = datetime.fromisoformat(dt_str)
    return dt.strftime("%m/%d %H:%M")


def to_local_iso(date_str: str, hour: int, minute: int = 0) -> str:
    dt = datetime.strptime(f"{date_str} {hour:02d}:{minute:02d}", "%Y-%m-%d %H:%M")
    dt = dt.replace(tzinfo=TIMEZONE)
    return dt.isoformat()


def build_fixed_reminder(task_text: str, date_str: str, hour: int, minute: int = 0):
    target_dt = datetime.strptime(f"{date_str} {hour:02d}:{minute:02d}", "%Y-%m-%d %H:%M")
    target_dt = target_dt.replace(tzinfo=TIMEZONE)

    remind_times = [
        target_dt - timedelta(hours=2),
        target_dt - timedelta(hours=1),
        target_dt - timedelta(minutes=30),
    ]

    return {
        "id": str(uuid.uuid4())[:8],
        "user_id": ALLOWED_USERS[0],
        "kind": "fixed",
        "task_text": task_text,
        "date": date_str,
        "period": "早上",
        "target_time": target_dt.isoformat(),
        "reminder_times": [dt.isoformat() for dt in remind_times],
        "sent_reminders": [],
        "status": "active",  # active/completed/cancelled/expired
        "created_at": now_local().isoformat(),
    }


def build_period_reminder(task_text: str, date_str: str):
    remind_times = [
        to_local_iso(date_str, 6, 0),
        to_local_iso(date_str, 8, 0),
        to_local_iso(date_str, 10, 0),
    ]
    end_dt = datetime.strptime(f"{date_str} {MORNING_END_HOUR:02d}:00", "%Y-%m-%d %H:%M")
    end_dt = end_dt.replace(tzinfo=TIMEZONE)

    return {
        "id": str(uuid.uuid4())[:8],
        "user_id": ALLOWED_USERS[0],
        "kind": "period",
        "task_text": task_text,
        "date": date_str,
        "period": "早上",
        "target_time": None,
        "reminder_times": remind_times,
        "sent_reminders": [],
        "status": "active",
        "created_at": now_local().isoformat(),
        "period_end": end_dt.isoformat(),
    }


def parse_add_reminder(text: str):
    """
    v1 only supports:
    1) 明天早上7點吃早餐
    2) 明天早上吃早餐
    """

    cleaned = re.sub(r"\s+", "", text)

    # 固定時間：明天早上7點吃早餐 / 明天早上07點吃早餐 / 明天早上7:30吃早餐
    m_fixed = re.match(r"^明天早上(\d{1,2})(?::|：)?(\d{0,2})點?(.*)$", cleaned)
    if m_fixed:
        hour = int(m_fixed.group(1))
        minute_str = m_fixed.group(2)
        minute = int(minute_str) if minute_str else 0
        task_text = m_fixed.group(3).strip()

        if not task_text:
            return {"ok": False, "message": "請補上提醒內容，例如：明天早上7點吃早餐"}

        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            return {"ok": False, "message": "時間格式不正確"}

        reminder = build_fixed_reminder(
            task_text=task_text,
            date_str=tomorrow_date_str(),
            hour=hour,
            minute=minute,
        )
        return {"ok": True, "reminder": reminder}

    # 時段型：明天早上吃早餐
    m_period = re.match(r"^明天早上(.*)$", cleaned)
    if m_period:
        task_text = m_period.group(1).strip()

        if not task_text:
            return {"ok": False, "message": "請補上提醒內容，例如：明天早上吃早餐"}

        reminder = build_period_reminder(
            task_text=task_text,
            date_str=tomorrow_date_str(),
        )
        return {"ok": True, "reminder": reminder}

    return {"ok": False, "message": None}


def parse_cancel_keyword(text: str):
    # 支援：取消 明天早餐 / 取消明天早餐 / 取消 早餐
    cleaned = re.sub(r"\s+", "", text)
    if not cleaned.startswith("取消"):
        return None

    body = cleaned[2:]
    date_filter = "tomorrow" if "明天" in body else None
    body = body.replace("明天", "")
    keyword = body.strip()

    return {
        "date_filter": date_filter,
        "keyword": keyword,
    }


def parse_complete_keyword(text: str):
    # 支援：完成 早餐 / 完成明天早餐
    cleaned = re.sub(r"\s+", "", text)
    if not cleaned.startswith("完成"):
        return None

    body = cleaned[2:]
    date_filter = "tomorrow" if "明天" in body else None
    body = body.replace("明天", "")
    keyword = body.strip()

    return {
        "date_filter": date_filter,
        "keyword": keyword,
    }


def is_same_target_date(reminder, date_filter):
    if date_filter == "tomorrow":
        return reminder.get("date") == tomorrow_date_str()
    return True


def match_keyword(reminder, keyword):
    if not keyword:
        return True
    return keyword in reminder.get("task_text", "")


def format_reminder_line(r):
    if r["kind"] == "fixed":
        target = format_dt(r["target_time"])
        return f"- [{r['id']}] 固定時間｜{target}｜{r['task_text']}｜{r['status']}"
    else:
        return f"- [{r['id']}] 早上時段｜{r['date']} 06:00/08:00/10:00｜{r['task_text']}｜{r['status']}"


# =========================
# Reminder business logic
# =========================
def expire_old_reminders():
    reminders = load_reminders()
    now_dt = now_local()
    changed = False

    for r in reminders:
        if r["status"] != "active":
            continue

        if r["kind"] == "fixed":
            target_dt = datetime.fromisoformat(r["target_time"])
            if now_dt > target_dt:
                r["status"] = "expired"
                changed = True

        elif r["kind"] == "period":
            period_end = datetime.fromisoformat(r["period_end"])
            if now_dt > period_end:
                r["status"] = "expired"
                changed = True

    if changed:
        update_reminders(reminders)


async def send_telegram_message(user_id: int, text: str):
    bot = Bot(token=BOT_TOKEN)
    await bot.send_message(chat_id=user_id, text=text)


def send_message_sync(user_id: int, text: str):
    try:
        asyncio.run(send_telegram_message(user_id, text))
    except Exception as e:
        print(f"[send_message_sync] error: {e}")


def check_and_send_due_reminders():
    reminders = load_reminders()
    now_dt = now_local()
    changed = False

    for r in reminders:
        if r["status"] != "active":
            continue

        # 先處理過期
        if r["kind"] == "fixed":
            target_dt = datetime.fromisoformat(r["target_time"])
            if now_dt > target_dt:
                r["status"] = "expired"
                changed = True
                continue

        elif r["kind"] == "period":
            period_end = datetime.fromisoformat(r["period_end"])
            if now_dt > period_end:
                r["status"] = "expired"
                changed = True
                continue

        # 檢查該不該發提醒
        for rt in r["reminder_times"]:
            if rt in r["sent_reminders"]:
                continue

            remind_dt = datetime.fromisoformat(rt)

            # v1: 每 20 秒檢查一次，只要現在時間 >= 提醒時間 就發
            if now_dt >= remind_dt:
                if r["kind"] == "fixed":
                    target = datetime.fromisoformat(r["target_time"])
                    delta_min = int((target - remind_dt).total_seconds() / 60)

                    if delta_min == 120:
                        lead_text = "2 小時後"
                    elif delta_min == 60:
                        lead_text = "1 小時後"
                    elif delta_min == 30:
                        lead_text = "30 分鐘後"
                    else:
                        lead_text = "稍後"

                    msg = (
                        f"【提醒】{lead_text}要 {r['task_text']}\n"
                        f"時間：{target.strftime('%m/%d %H:%M')}"
                    )
                else:
                    msg = (
                        f"【提醒】早上要 {r['task_text']}\n"
                        f"提醒時段：06:00 / 08:00 / 10:00\n"
                        f"回覆「完成 {r['task_text']}」可停止後續提醒"
                    )

                send_message_sync(r["user_id"], msg)
                r["sent_reminders"].append(rt)
                changed = True

    if changed:
        update_reminders(reminders)


def reminder_scheduler_loop():
    while True:
        try:
            expire_old_reminders()
            check_and_send_due_reminders()
        except Exception as e:
            print(f"[reminder_scheduler_loop] error: {e}")
        time.sleep(20)


# =========================
# Telegram handlers
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return
    await update.message.reply_text(
        "Bot is alive!\n"
        "目前支援：\n"
        "1. 明天早上7點吃早餐\n"
        "2. 明天早上吃早餐\n"
        "3. 取消 明天早餐\n"
        "4. 完成 早餐\n"
        "5. 我的提醒"
    )


async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return

    user = update.effective_user
    username = f"@{user.username}" if user.username else "No username set"

    await update.message.reply_text(
        f"Your user ID: {user.id}\n"
        f"Username: {username}\n"
        f"Name: {user.first_name}"
    )


async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return

    await update.message.reply_text(
        "提醒功能 v1 用法：\n\n"
        "【新增】\n"
        "- 明天早上7點吃早餐\n"
        "- 明天早上吃早餐\n\n"
        "【查看】\n"
        "- 我的提醒\n\n"
        "【取消】\n"
        "- 取消 明天早餐\n"
        "- 取消 早餐\n\n"
        "【完成】\n"
        "- 完成 早餐\n"
        "- 完成 明天早餐"
    )


async def list_reminders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return

    reminders = load_reminders()
    active = [r for r in reminders if r["status"] == "active"]

    if not active:
        await update.message.reply_text("目前沒有進行中的提醒。")
        return

    lines = ["目前進行中的提醒："]
    for r in active:
        lines.append(format_reminder_line(r))

    await update.message.reply_text("\n".join(lines))


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        return

    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()

    # 1) 查看提醒
    if text in ["我的提醒", "查看提醒", "明天有什麼"]:
        await list_reminders(update, context)
        return

    # 2) 取消提醒
    cancel_info = parse_cancel_keyword(text)
    if cancel_info is not None:
        reminders = load_reminders()
        count = 0

        for r in reminders:
            if r["status"] != "active":
                continue
            if not is_same_target_date(r, cancel_info["date_filter"]):
                continue
            if not match_keyword(r, cancel_info["keyword"]):
                continue

            r["status"] = "cancelled"
            count += 1

        update_reminders(reminders)

        if count == 0:
            await update.message.reply_text("找不到符合的提醒。")
        else:
            await update.message.reply_text(f"已取消 {count} 筆提醒。")
        return

    # 3) 完成提醒
    complete_info = parse_complete_keyword(text)
    if complete_info is not None:
        reminders = load_reminders()
        count = 0

        for r in reminders:
            if r["status"] != "active":
                continue
            if not is_same_target_date(r, complete_info["date_filter"]):
                continue
            if not match_keyword(r, complete_info["keyword"]):
                continue

            r["status"] = "completed"
            count += 1

        update_reminders(reminders)

        if count == 0:
            await update.message.reply_text("找不到符合的提醒可完成。")
        else:
            await update.message.reply_text(f"已完成 {count} 筆提醒，後續將不再提醒。")
        return

    # 4) 新增提醒
    parsed = parse_add_reminder(text)
    if parsed["ok"]:
        reminder = parsed["reminder"]
        add_reminder(reminder)

        if reminder["kind"] == "fixed":
            target = datetime.fromisoformat(reminder["target_time"]).strftime("%m/%d %H:%M")
            reminder_times = [datetime.fromisoformat(x).strftime("%H:%M") for x in reminder["reminder_times"]]
            await update.message.reply_text(
                f"已建立提醒：{target} {reminder['task_text']}\n"
                f"提醒時間：{'、'.join(reminder_times)}"
            )
        else:
            await update.message.reply_text(
                f"已建立提醒：明天早上 {reminder['task_text']}\n"
                f"提醒時間：06:00、08:00、10:00\n"
                f"完成後可輸入：完成 {reminder['task_text']}"
            )
        return

    # 5) 其他
    await update.message.reply_text(
        "目前看不懂這句。\n"
        "你可以試試：\n"
        "- 明天早上7點吃早餐\n"
        "- 明天早上吃早餐\n"
        "- 我的提醒\n"
        "- 取消 明天早餐\n"
        "- 完成 早餐"
    )


# =========================
# Flask + Main
# =========================
def run_flask():
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is missing")

    ensure_data_file()

    # Flask thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    # Reminder background thread
    scheduler_thread = threading.Thread(target=reminder_scheduler_loop, daemon=True)
    scheduler_thread.start()

    # Telegram bot
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("whoami", whoami))
    application.add_handler(CommandHandler("help", show_help))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    application.run_polling()
