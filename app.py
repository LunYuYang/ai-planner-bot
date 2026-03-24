import os
import re
import json
import traceback
import threading
from datetime import datetime, timedelta

from flask import Flask
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
GOOGLE_TOKEN_JSON = os.getenv("GOOGLE_TOKEN_JSON")

SCOPES = ["https://www.googleapis.com/auth/calendar"]

print("BOT_TOKEN loaded:", bool(BOT_TOKEN))
print("GOOGLE_CREDENTIALS_JSON loaded:", bool(GOOGLE_CREDENTIALS_JSON))
print("GOOGLE_TOKEN_JSON loaded:", bool(GOOGLE_TOKEN_JSON))

app_web = Flask(__name__)


@app_web.get("/")
def healthcheck():
    return {"status": "ok"}, 200


def get_calendar_service():
    creds = None

    if GOOGLE_TOKEN_JSON:
        token_info = json.loads(GOOGLE_TOKEN_JSON)
        creds = Credentials.from_authorized_user_info(token_info, SCOPES)

    if not creds:
        raise RuntimeError("找不到 GOOGLE_TOKEN_JSON，請到 Render 設定環境變數。")

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    service = build("calendar", "v3", credentials=creds)
    return service


def create_calendar_event(summary, start_dt, end_dt):
    service = get_calendar_service()

    event = {
        "summary": summary,
        "start": {
            "dateTime": start_dt.isoformat(),
            "timeZone": "Asia/Taipei",
        },
        "end": {
            "dateTime": end_dt.isoformat(),
            "timeZone": "Asia/Taipei",
        },
        "reminders": {
            "useDefault": False,
            "overrides": [
                {"method": "popup", "minutes": 30},
                {"method": "popup", "minutes": 15},
            ],
        },
    }

    created_event = service.events().insert(calendarId="primary", body=event).execute()
    return created_event


def preprocess_text(text):
    text = text.strip()
    text = re.sub(r"下午(\d{1,2})點", lambda m: f"{int(m.group(1)) + 12}點", text)
    text = re.sub(r"晚上(\d{1,2})點", lambda m: f"{int(m.group(1)) + 12}點", text)
    text = text.replace("早上", "")
    text = text.replace("上午", "")
    return text


def extract_time_and_task(text):
    patterns = [
        r"^(今天)(\d{1,2})點(?:(\d{1,2})分)?(.*)$",
        r"^(明天)(\d{1,2})點(?:(\d{1,2})分)?(.*)$",
        r"^(後天)(\d{1,2})點(?:(\d{1,2})分)?(.*)$",
        r"^(今晚)(\d{1,2})點(?:(\d{1,2})分)?(.*)$",
    ]

    for pattern in patterns:
        m = re.match(pattern, text)
        if m:
            day_word = m.group(1)
            hour = int(m.group(2))
            minute = int(m.group(3)) if m.group(3) else 0
            task = m.group(4).strip() or "未命名行程"
            return day_word, hour, minute, task

    # 全天事件規則
    if text in ["今天出差", "明天出差", "後天出差"]:
        return text[:2], "ALL_DAY", 0, "出差"

    return None, None, None, text


def build_datetime(day_word, hour, minute):
    now = datetime.now()

    if day_word == "今天":
        base_date = now.date()
    elif day_word == "明天":
        base_date = (now + timedelta(days=1)).date()
    elif day_word == "後天":
        base_date = (now + timedelta(days=2)).date()
    else:
        return None

    try:
        return datetime(
            year=base_date.year,
            month=base_date.month,
            day=base_date.day,
            hour=hour,
            minute=minute,
        )
    except ValueError:
        return None


def create_all_day_event(summary, day_word):
    now = datetime.now()

    if day_word == "今天":
        event_date = now.date()
    elif day_word == "明天":
        event_date = (now + timedelta(days=1)).date()
    elif day_word == "後天":
        event_date = (now + timedelta(days=2)).date()
    else:
        raise ValueError("未知日期")

    service = get_calendar_service()

    event = {
        "summary": summary,
        "start": {"date": event_date.isoformat()},
        "end": {"date": (event_date + timedelta(days=1)).isoformat()},
        "reminders": {
            "useDefault": False,
            "overrides": [{"method": "popup", "minutes": 480}],
        },
    }

    return service.events().insert(calendarId="primary", body=event).execute()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("你可以直接輸入行程，例如：明天下午2點看牙醫 / 明天出差")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    processed = preprocess_text(text)

    day_word, hour, minute, task = extract_time_and_task(processed)

    try:
        if hour == "ALL_DAY":
            created_event = create_all_day_event(task, day_word)
            await update.message.reply_text(
                f"✅ 已加入 Google Calendar（全天）\n"
                f"📌 內容：{task}\n"
                f"🔗 事件連結：{created_event.get('htmlLink', '')}"
            )
            return

        if day_word is not None:
            parsed_time = build_datetime(day_word, hour, minute)
            if parsed_time:
                end_time = parsed_time + timedelta(hours=1)
                created_event = create_calendar_event(task, parsed_time, end_time)
                await update.message.reply_text(
                    f"✅ 已加入 Google Calendar\n"
                    f"📅 時間：{parsed_time.strftime('%Y-%m-%d %H:%M')}\n"
                    f"📌 內容：{task}\n"
                    f"🔗 事件連結：{created_event.get('htmlLink', '')}"
                )
                return

        await update.message.reply_text("❗無法解析時間，請再試一次")
    except Exception:
        traceback.print_exc()
        await update.message.reply_text("❗建立 Google Calendar 事件失敗，請檢查 Render 環境變數。")


def run_bot():
    telegram_app = ApplicationBuilder().token(BOT_TOKEN).build()
    telegram_app.add_handler(CommandHandler("start", start))
    telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    telegram_app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()

    port = int(os.getenv("PORT", "10000"))
    app_web.run(host="0.0.0.0", port=port)