import os


def _getenv_str(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.strip() if isinstance(value, str) else default


def _getenv_int(name: str, default: int = 0) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return default


def _getenv_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "true" if default else "false")
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


BOT_TOKEN = _getenv_str("BOT_TOKEN")
OWNER_ID = _getenv_int("OWNER_ID", 0)

TIMEZONE = _getenv_str("TIMEZONE", "Asia/Taipei")

RENDER_EXTERNAL_URL = _getenv_str("RENDER_EXTERNAL_URL")
WEBHOOK_SECRET_PATH = _getenv_str("WEBHOOK_SECRET_PATH", "webhook")

# PostgreSQL 版主要吃 DATABASE_URL；DB_PATH 只作 fallback 保留
DB_PATH = _getenv_str("DB_PATH", "")

DEFAULT_NEWS_CATEGORY = _getenv_str("DEFAULT_NEWS_CATEGORY", "business")
DEFAULT_NEWS_LIMIT = _getenv_int("DEFAULT_NEWS_LIMIT", 5)
NEWS_PUSH_TIME = _getenv_str("NEWS_PUSH_TIME", "09:00")

ENABLE_CHINESE_SUMMARY = _getenv_bool("ENABLE_CHINESE_SUMMARY", False)

OPENAI_API_KEY = _getenv_str("OPENAI_API_KEY")
OPENAI_MODEL = _getenv_str("OPENAI_MODEL", "gpt-4o-mini")

TELEGRAM_CHAT_ID = _getenv_str("TELEGRAM_CHAT_ID")