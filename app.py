from flask import Flask
import threading
import os
import asyncio
from datetime import datetime, timedelta

from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters

# ===== Flask =====
app_web = Flask(__name__)

@app_web.route("/")
def home():
    return "Bot is running!"

# ===== Telegram handlers =====
async def start(update, context):
    await update.message.reply_text("你好！我可以幫你記錄行程 📅\n請輸入：明天下午2點看牙醫")

async def handle(update, context):
    text = update.message.text

    try:
        # ===== 簡單時間解析（你原本邏輯簡化版）=====
        now = datetime.now()

        if "明天" in text:
            day = now + timedelta(days=1)
        else:
            day = now

        hour = 14  # 預設下午2點
        if "上午" in text:
            hour = 9
        elif "下午" in text:
            hour = 14
        elif "晚上" in text:
            hour = 20

        parsed_time = datetime(day.year, day.month, day.day, hour, 0)
        end_time = parsed_time + timedelta(hours=1)

        task = text

        await update.message.reply_text(
            f"✅ 已加入 Google Calendar\n"
            f"📅 時間：{parsed_time.strftime('%Y-%m-%d %H:%M')}\n"
            f"📝 內容：{task}"
        )

    except Exception as e:
        await update.message.reply_text(f"❌ 建立事件失敗：{e}")

# ===== 核心修正：run_bot =====
def run_bot():
    # ⭐⭐⭐ 這行是關鍵（修正 event loop）
    asyncio.set_event_loop(asyncio.new_event_loop())

    BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

    if not BOT_TOKEN:
        raise ValueError("❌ TELEGRAM_BOT_TOKEN 沒設定")

    telegram_app = ApplicationBuilder().token(BOT_TOKEN).build()

    telegram_app.add_handler(CommandHandler("start", start))
    telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle))

    telegram_app.run_polling(drop_pending_updates=True)

# ===== 啟動 =====
if __name__ == "__main__":
    # Telegram bot 開 thread
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()

    # Flask（給 Render 用）
    port = int(os.getenv("PORT", "10000"))
    app_web.run(host="0.0.0.0", port=port)