import os
import threading
from flask import Flask
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running!"

BOT_TOKEN = os.getenv("BOT_TOKEN")

# 先留空，等等拿到你的 user id 再填
ALLOWED_USERS = []

async def start(update, context):
    await update.message.reply_text("Bot is alive!")

async def whoami(update, context):
    user = update.effective_user
    await update.message.reply_text(
        f"Your user ID: {user.id}\n"
        f"Username: @{user.username}\n"
        f"Name: {user.first_name}"
    )

async def echo(update, context):
    await update.message.reply_text(update.message.text)

def run_flask():
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is missing")

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("whoami", whoami))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

    application.run_polling()
