import os

# ===== 基本設定 =====
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

TIMEZONE = os.getenv("TIMEZONE", "Asia/Taipei")
TZ = os.getenv("TZ", "Asia/Taipei")

# ===== Webhook =====
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
WEBHOOK_SECRET_PATH = os.getenv("WEBHOOK_SECRET_PATH", "webhook")

# ===== 資料庫 =====
DB_PATH = os.getenv("DB_PATH", "data.db")

# ===== 新聞 =====
DEFAULT_NEWS_CATEGORY = os.getenv("DEFAULT_NEWS_CATEGORY", "business")
DEFAULT_NEWS_LIMIT = int(os.getenv("DEFAULT_NEWS_LIMIT", "5"))
NEWS_PUSH_TIME = os.getenv("NEWS_PUSH_TIME", "09:00")

ENABLE_CHINESE_SUMMARY = os.getenv("ENABLE_CHINESE_SUMMARY", "false").lower() == "true"
SUMMARY_ONLY_FOR_DAILY_PUSH = os.getenv("SUMMARY_ONLY_FOR_DAILY_PUSH", "true").lower() == "true"

# ===== OpenAI =====
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# ===== Telegram =====
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
