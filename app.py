import os
import threading
from flask import Flask
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running!"

BOT_TOKEN = os.getenv("BOT_TOKEN")

# 🔐 你的 Telegram user ID
ALLOWED_USERS = [7243450850]

def is_allowed(update):
    user = update.effective_user
    return user and user.id in ALLOWED_USERS

# ===== Commands =====

async def start(update, context):
    if not is_allowed(update):
        return
    await update.message.reply_text("Bot is alive!")

async def whoami(update, context):
    if not is_allowed(update):
        return

    user = update.effective_user

    # 修正 username 顯示
    username = f"@{user.username}" if user.username else "No username set"

    await update.message.reply_text(
        f"Your user ID: {user.id}\n"
        f"Username: {username}\n"
        f"Name: {user.first_name}"
    )

async def echo(update, context):
    if not is_allowed(update):
        return
    await update.message.reply_text(update.message.text)

# ===== Flask for Render =====

def run_flask():
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

# ===== Main =====

if __name__ == "__main__":
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is missing")

    # Flask thread（讓 Render 不睡）
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    # Telegram bot
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("whoami", whoami))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

    application.run_polling()
