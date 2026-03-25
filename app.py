import os
from flask import Flask
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters
import asyncio

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running!"

BOT_TOKEN = os.getenv("BOT_TOKEN")

# ===== Telegram handlers =====
async def start(update, context):
    await update.message.reply_text("Bot is alive!")

async def echo(update, context):
    await update.message.reply_text(update.message.text)

# ===== 主程式 =====
async def main():
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

    print("Telegram bot started")
    await application.run_polling()

# ===== 啟動 =====
if __name__ == "__main__":
    import threading

    def run_flask():
        port = int(os.environ.get("PORT", 10000))
        app.run(host="0.0.0.0", port=port)

    threading.Thread(target=run_flask).start()

    asyncio.run(main())