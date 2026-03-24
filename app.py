import os
import re
import traceback
from datetime import datetime, timedelta

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

SCOPES = ["https://www.googleapis.com/auth/calendar"]

print("BOT_TOKEN loaded:", bool(BOT_TOKEN))


def get_calendar_service():
    creds = None

    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)

        with open("token.json", "w", encoding="utf-8") as token:
            token.write(creds.to_json())

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

    return None, None, None, text


def build_datetime(day_word, hour, minute):
    now = datetime.now()

    if day_word == "今天":
        base_date = now.date()
    elif day_word == "明天":
        base_date = (now + timedelta(days=1)).date()
    elif day_word == "後天":
        base_date = (now + timedelta(days=2)).date()
    elif day_word == "今晚":
        base_date = now.date()
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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "你可以直接輸入行程，例如：明天下午2點看牙醫\n我會幫你寫進 Google Calendar。"
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    print("原始:", text)

    processed = preprocess_text(text)
    print("處理後:", processed)

    day_word, hour, minute, task = extract_time_and_task(processed)
    print("day_word:", day_word)
    print("hour:", hour)
    print("minute:", minute)
    print("task:", task)

    parsed_time = None
    if day_word is not None:
        parsed_time = build_datetime(day_word, hour, minute)

    print("解析結果:", parsed_time)

    if parsed_time:
        end_time = parsed_time + timedelta(hours=1)

        try:
            created_event = create_calendar_event(task, parsed_time, end_time)
            event_link = created_event.get("htmlLink", "")

            await update.message.reply_text(
                f"✅ 已加入 Google Calendar\n"
                f"📅 時間：{parsed_time.strftime('%Y-%m-%d %H:%M')}\n"
                f"📌 內容：{task}\n"
                f"🔗 事件連結：{event_link}"
            )
        except Exception as e:
            print("建立日曆事件失敗:", e)
            traceback.print_exc()
            await update.message.reply_text("❗解析成功，但寫入 Google Calendar 失敗，請檢查授權。")
    else:
        await update.message.reply_text("❗無法解析時間，請再試一次")


def main():
    try:
        print("Starting bot...")
        app = ApplicationBuilder().token(BOT_TOKEN).build()

        app.add_handler(CommandHandler("start", start))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

        print("Bot is running...")
        app.run_polling(drop_pending_updates=True)

    except Exception:
        print("=== ERROR ===")
        traceback.print_exc()


if __name__ == "__main__":
    main()