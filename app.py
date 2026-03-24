import os
import json
import threading
from datetime import datetime, timedelta

from flask import Flask
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GOOGLE_TOKEN_JSON = os.getenv("GOOGLE_TOKEN_JSON")

SCOPES = ["https://www.googleapis.com/auth/calendar"]

app_web = Flask(__name__)


@app_web.get("/")
def healthcheck():
    return {"status": "ok"}, 200


def get_calendar_service():
    creds = Credentials.from_authorized_user_info(
        json.loads(GOOGLE_TOKEN_JSON), SCOPES
    )

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    return build("calendar", "v3", credentials=creds)


def create_event(text):
    service = get_calendar_service()

    now = datetime.now() + timedelta(minutes=1)
    end = now + timedelta(hours=1)

    event = {
        "summary": text,
        "start": {"dateTime": now.isoformat(), "timeZone": "Asia/Taipei"},
        "end": {"dateTime": end.isoformat(), "timeZone": "Asia/Taipei"},
    }

    return service.events().insert(calendarId="primary", body=event).execute()


async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    event = create_event(text)

    await update.message.reply_text(f"已建立事件：{text}")


def run_bot():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT, handle))
    app.run_polling()


if __name__ == "__main__":
    threading.Thread(target=run_bot).start()

    port = int(os.getenv("PORT", 10000))
    app_web.run(host="0.0.0.0", port=port)