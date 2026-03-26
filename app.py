import os
import re
import json
import html
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import requests
import feedparser
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.base import JobLookupError
from zoneinfo import ZoneInfo
from openai import OpenAI


# =========================
# 基本設定
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
WEBHOOK_SECRET_PATH = os.getenv("WEBHOOK_SECRET_PATH", "telegram").strip()
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
TIMEZONE = os.getenv("TIMEZONE", os.getenv("TZ", "Asia/Taipei")).strip()

NEWS_PUSH_TIME = os.getenv("NEWS_PUSH_TIME", "08:00").strip()
DEFAULT_NEWS_LIMIT = int(os.getenv("DEFAULT_NEWS_LIMIT", "5"))
DEFAULT_NEWS_CATEGORY = os.getenv("DEFAULT_NEWS_CATEGORY", "all").strip().lower()

TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DATA_DIR = os.getenv("DATA_DIR", "data")
CHAT_FILE = os.path.join(DATA_DIR, "chat_ids.json")
DB_PATH = os.getenv("DB_PATH", os.path.join(DATA_DIR, "bot.db")).strip()

HTTP_TIMEOUT = 20

# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
ENABLE_CHINESE_SUMMARY = os.getenv("ENABLE_CHINESE_SUMMARY", "true").strip().lower() == "true"

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN in environment variables.")

os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

TZINFO = ZoneInfo(TIMEZONE)

app = Flask(__name__)
scheduler = BackgroundScheduler(timezone=TZINFO)

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


# =========================
# 免費 RSS 新聞來源
# =========================
RSS_FEEDS = {
    "tech": [
        "https://news.google.com/rss/headlines/section/topic/TECHNOLOGY?hl=en-US&gl=US&ceid=US:en",
        "https://feeds.npr.org/1019/rss.xml",
    ],
    "business": [
        "https://news.google.com/rss/headlines/section/topic/BUSINESS?hl=en-US&gl=US&ceid=US:en",
    ],
}


# =========================
# DB
# =========================
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                remind_at TEXT NOT NULL,
                message TEXT NOT NULL,
                sent INTEGER NOT NULL DEFAULT 0,
                canceled INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """
        )

        # 舊資料庫升級兼容
        cols = conn.execute("PRAGMA table_info(reminders)").fetchall()
        col_names = {row[1] for row in cols}
        if "canceled" not in col_names:
            conn.execute("ALTER TABLE reminders ADD COLUMN canceled INTEGER NOT NULL DEFAULT 0")

        conn.commit()
    finally:
        conn.close()


def save_reminder(chat_id: int, remind_at: datetime, message: str) -> int:
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            INSERT INTO reminders (chat_id, remind_at, message, sent, canceled, created_at)
            VALUES (?, ?, ?, 0, 0, ?)
            """,
            (
                chat_id,
                remind_at.isoformat(),
                message,
                datetime.now(TZINFO).isoformat(),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def mark_reminder_sent(reminder_id: int) -> None:
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE reminders SET sent = 1 WHERE id = ?",
            (reminder_id,)
        )
        conn.commit()
    finally:
        conn.close()


def cancel_reminder(reminder_id: int, chat_id: int) -> bool:
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            UPDATE reminders
            SET canceled = 1
            WHERE id = ? AND chat_id = ? AND sent = 0 AND canceled = 0
            """,
            (reminder_id, chat_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def get_pending_reminders() -> List[sqlite3.Row]:
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            SELECT id, chat_id, remind_at, message
            FROM reminders
            WHERE sent = 0 AND canceled = 0
            ORDER BY remind_at ASC
            """
        )
        return cur.fetchall()
    finally:
        conn.close()


def get_user_pending_reminders(chat_id: int) -> List[sqlite3.Row]:
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            SELECT id, chat_id, remind_at, message
            FROM reminders
            WHERE chat_id = ? AND sent = 0 AND canceled = 0
            ORDER BY remind_at ASC
            """,
            (chat_id,),
        )
        return cur.fetchall()
    finally:
        conn.close()


# =========================
# chat_id 儲存
# =========================
def load_chat_ids() -> List[int]:
    if not os.path.exists(CHAT_FILE):
        return []

    try:
        with open(CHAT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [int(x) for x in data]
        return []
    except Exception as e:
        logger.exception("Failed to load chat ids: %s", e)
        return []


def save_chat_ids(chat_ids: List[int]) -> None:
    try:
        with open(CHAT_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(list(set(chat_ids))), f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.exception("Failed to save chat ids: %s", e)


def register_chat_id(chat_id: int) -> None:
    chat_ids = load_chat_ids()
    if chat_id not in chat_ids:
        chat_ids.append(chat_id)
        save_chat_ids(chat_ids)
        logger.info("Registered new chat_id: %s", chat_id)


def get_all_target_chat_ids() -> List[int]:
    ids: List[int] = []

    if TELEGRAM_CHAT_ID:
        try:
            ids.append(int(TELEGRAM_CHAT_ID))
        except ValueError:
            logger.warning("Invalid TELEGRAM_CHAT_ID: %s", TELEGRAM_CHAT_ID)

    if OWNER_ID:
        ids.append(OWNER_ID)

    ids.extend(load_chat_ids())

    # 私人模式：最後仍只保留 OWNER_ID / TELEGRAM_CHAT_ID
    final_ids = set(ids)
    if OWNER_ID:
        final_ids = {OWNER_ID} | ({int(TELEGRAM_CHAT_ID)} if TELEGRAM_CHAT_ID.isdigit() else set()) | final_ids

    return sorted(list(final_ids))


# =========================
# Telegram API
# =========================
def telegram_api(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    resp = requests.post(url, json=payload, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")

    return data


def send_message(chat_id: int, text: str, disable_web_page_preview: bool = True) -> None:
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": disable_web_page_preview,
    }
    telegram_api("sendMessage", payload)


def set_webhook() -> None:
    if not RENDER_EXTERNAL_URL:
        logger.warning("RENDER_EXTERNAL_URL not set. Skip setWebhook.")
        return

    webhook_url = f"{RENDER_EXTERNAL_URL}/{WEBHOOK_SECRET_PATH}"
    payload = {"url": webhook_url}
    data = telegram_api("setWebhook", payload)
    logger.info("Webhook set result: %s", data)


# =========================
# 文字工具
# =========================
def clean_html_text(raw: str) -> str:
    if not raw:
        return ""
    text = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def trim_text(text: str, max_len: int = 110) -> str:
    text = text.strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def normalize_title(title: str) -> str:
    title = title.lower().strip()
    title = re.sub(r"\s+", " ", title)
    title = re.sub(r"\s*-\s*[^-]+$", "", title)
    return title


def parse_published_ts(entry: Dict[str, Any]) -> float:
    for key in ("published_parsed", "updated_parsed"):
        time_struct = entry.get(key)
        if time_struct:
            try:
                dt = datetime(*time_struct[:6], tzinfo=ZoneInfo("UTC"))
                return dt.timestamp()
            except Exception:
                pass
    return 0.0


def extract_source_name(feed_title: str, entry: Dict[str, Any]) -> str:
    source_name = ""

    if isinstance(entry.get("source"), dict):
        source_name = entry["source"].get("title", "") or ""

    if not source_name and " - " in entry.get("title", ""):
        source_name = entry["title"].rsplit(" - ", 1)[-1].strip()

    if not source_name:
        source_name = feed_title or "News"

    return source_name


def build_raw_summary(entry: Dict[str, Any], source_name: str) -> str:
    candidates = [
        entry.get("summary", ""),
        entry.get("description", ""),
    ]

    for raw in candidates:
        cleaned = clean_html_text(raw)
        title_clean = clean_html_text(entry.get("title", ""))
        if cleaned and cleaned != title_clean:
            return trim_text(cleaned, 220)

    return f"Latest report from {source_name}. Open the link for full details."


# =========================
# OpenAI 中文摘要
# =========================
def summarize_to_chinese(title: str, raw_summary: str, source_name: str) -> str:
    fallback = trim_text(raw_summary, 110)

    if not ENABLE_CHINESE_SUMMARY:
        return fallback

    if not client:
        logger.warning("OPENAI_API_KEY not set, fallback to raw summary.")
        return fallback

    try:
        prompt = f"""
請將以下科技或商業新聞整理成繁體中文摘要。

要求：
1. 使用繁體中文
2. 30~60字左右
3. 精簡、自然、像新聞快報
4. 不要加入未提供的推測
5. 優先保留公司、產品、商業/科技重點
6. 只輸出摘要，不要加「摘要：」

新聞標題：
{title}

新聞內容：
{raw_summary}

新聞來源：
{source_name}
""".strip()

        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "你是擅長整理國際科技與商業新聞的繁體中文編輯，輸出精簡、清楚、自然。"
                },
                {
                    "role": "user",
                    "content": prompt
                },
            ],
            temperature=0.3,
            max_tokens=120,
        )

        content = (resp.choices[0].message.content or "").strip()
        content = re.sub(r"^摘要[:：]\s*", "", content)
        content = re.sub(r"\s+", " ", content).strip()

        if not content:
            return fallback

        return trim_text(content, 110)

    except Exception as e:
        logger.exception("Chinese summary failed: %s", e)
        return fallback


# =========================
# 新聞抓取
# =========================
def fetch_rss_items(feed_url: str, category: str) -> List[Dict[str, Any]]:
    parsed = feedparser.parse(feed_url)
    feed_title = clean_html_text(parsed.feed.get("title", ""))

    items: List[Dict[str, Any]] = []

    for entry in parsed.entries:
        title_raw = clean_html_text(entry.get("title", "")).strip()
        if not title_raw:
            continue

        link = entry.get("link", "").strip()
        source_name = extract_source_name(feed_title, entry)
        raw_summary = build_raw_summary(entry, source_name)
        zh_summary = summarize_to_chinese(title_raw, raw_summary, source_name)
        published_ts = parse_published_ts(entry)

        items.append(
            {
                "title": title_raw,
                "title_norm": normalize_title(title_raw),
                "link": link,
                "summary": zh_summary,
                "source": source_name,
                "published_ts": published_ts,
                "category": category,
            }
        )

    return items


def fetch_news(category: str = "all", limit: int = DEFAULT_NEWS_LIMIT) -> List[Dict[str, Any]]:
    if category == "all":
        selected_categories = ["tech", "business"]
    elif category in ("tech", "business"):
        selected_categories = [category]
    else:
        selected_categories = ["tech", "business"]

    all_items: List[Dict[str, Any]] = []
    seen_titles = set()
    seen_links = set()

    ordered_categories = sorted(
        selected_categories,
        key=lambda x: 0 if x == "tech" else 1
    )

    for cat in ordered_categories:
        for feed_url in RSS_FEEDS.get(cat, []):
            try:
                items = fetch_rss_items(feed_url, cat)
                for item in items:
                    title_key = item["title_norm"]
                    link_key = item["link"].strip().lower()

                    if title_key in seen_titles:
                        continue
                    if link_key and link_key in seen_links:
                        continue

                    seen_titles.add(title_key)
                    if link_key:
                        seen_links.add(link_key)

                    all_items.append(item)
            except Exception as e:
                logger.exception("Failed to parse feed %s: %s", feed_url, e)

    all_items.sort(key=lambda x: x.get("published_ts", 0), reverse=True)

    if category == "all":
        tech_items = [x for x in all_items if x["category"] == "tech"]
        biz_items = [x for x in all_items if x["category"] == "business"]

        mixed: List[Dict[str, Any]] = []
        mixed.extend(tech_items[: min(3, len(tech_items))])

        remaining = limit - len(mixed)
        if remaining > 0:
            mixed.extend(biz_items[:remaining])

        remaining = limit - len(mixed)
        if remaining > 0:
            used = {x["title_norm"] for x in mixed}
            for item in tech_items[3:]:
                if item["title_norm"] not in used:
                    mixed.append(item)
                    used.add(item["title_norm"])
                if len(mixed) >= limit:
                    break

        if len(mixed) < limit:
            used = {x["title_norm"] for x in mixed}
            for item in biz_items:
                if item["title_norm"] not in used:
                    mixed.append(item)
                    used.add(item["title_norm"])
                if len(mixed) >= limit:
                    break

        return mixed[:limit]

    return all_items[:limit]


# =========================
# 訊息格式
# =========================
def format_news_message(items: List[Dict[str, Any]], category: str = "all") -> str:
    now_str = datetime.now(TZINFO).strftime("%Y-%m-%d %H:%M")

    if category == "tech":
        title = "🧠 今日科技新聞"
    elif category == "business":
        title = "💼 今日商業新聞"
    else:
        title = "🗞️ 今日科技 / 商業新聞"

    if not items:
        return (
            f"<b>{title}</b>\n"
            f"更新時間：{html.escape(now_str)}\n\n"
            "目前抓不到新聞，請稍後再試。"
        )

    lines = [
        f"<b>{title}</b>",
        f"更新時間：{html.escape(now_str)}",
        "",
    ]

    for idx, item in enumerate(items, start=1):
        title_text = html.escape(item["title"])
        summary_text = html.escape(item["summary"])
        source_text = html.escape(item["source"])
        link = item["link"]

        block = [
            f"<b>{idx}. {title_text}</b>",
            f"中文摘要：{summary_text}",
            f"來源：{source_text}",
        ]

        if link:
            safe_link = html.escape(link, quote=True)
            block.append(f'<a href="{safe_link}">閱讀原文</a>')

        lines.append("\n".join(block))
        lines.append("")

    message = "\n".join(lines).strip()
    if len(message) > 3900:
        message = message[:3890] + "\n…"

    return message


def parse_news_command(text: str) -> Dict[str, Any]:
    parts = text.strip().split()
    category = DEFAULT_NEWS_CATEGORY
    limit = DEFAULT_NEWS_LIMIT

    if len(parts) >= 2:
        arg1 = parts[1].lower()
        if arg1 in ("tech", "technology", "科技"):
            category = "tech"
        elif arg1 in ("business", "biz", "商業", "商務"):
            category = "business"
        elif arg1.isdigit():
            limit = max(1, min(10, int(arg1)))

    if len(parts) >= 3:
        arg2 = parts[2].lower()
        if arg2.isdigit():
            limit = max(1, min(10, int(arg2)))

    return {"category": category, "limit": limit}


# =========================
# 提醒功能
# =========================
def parse_relative_reminder(text: str) -> Optional[Dict[str, Any]]:
    """
    支援：
    - 30分鐘後提醒我打球
    - 10分鐘後開會
    - 2小時後提醒我喝水
    """
    raw = text.strip()
    now = datetime.now(TZINFO)

    m = re.match(r"^\s*(\d+)\s*(分鐘|分|min|mins|minute|minutes|小時|hr|hrs|hour|hours)\s*後\s*(提醒我)?\s*(.+?)\s*$", raw, re.IGNORECASE)
    if not m:
        return None

    amount_str, unit, _, msg = m.groups()
    amount = int(amount_str)

    if amount <= 0:
        return None

    unit = unit.lower()
    if unit in ("分鐘", "分", "min", "mins", "minute", "minutes"):
        remind_at = now + timedelta(minutes=amount)
    else:
        remind_at = now + timedelta(hours=amount)

    return {"remind_at": remind_at, "message": msg.strip()}


def parse_absolute_reminder(text: str) -> Optional[Dict[str, Any]]:
    """
    支援：
    - 2026-03-27 14:30 開會
    - 今天下午5點打球
    - 明天早上8點提醒我開會
    """
    raw = text.strip()
    now = datetime.now(TZINFO)

    m = re.match(r"^\s*(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})\s+(.+?)\s*$", raw)
    if m:
        date_str, hour_str, minute_str, msg = m.groups()
        dt = datetime.strptime(f"{date_str} {hour_str}:{minute_str}", "%Y-%m-%d %H:%M")
        dt = dt.replace(tzinfo=TZINFO)
        if dt <= now:
            return None
        return {"remind_at": dt, "message": msg.strip()}

    m = re.match(
        r"^\s*(今天|明天)\s*(早上|上午|中午|下午|晚上)?\s*(\d{1,2})(?:[:：點](\d{1,2}))?\s*(?:分)?\s*(提醒我)?\s*(.+?)\s*$",
        raw
    )
    if m:
        day_word, period, hour_str, minute_str, _, msg = m.groups()
        base_date = now.date() if day_word == "今天" else (now + timedelta(days=1)).date()

        hour = int(hour_str)
        minute = int(minute_str) if minute_str is not None else 0

        if period in ("下午", "晚上") and hour < 12:
            hour += 12
        elif period == "中午":
            if hour != 12 and hour < 11:
                hour += 12
        elif period in ("早上", "上午") and hour == 12:
            hour = 0

        try:
            dt = datetime(
                base_date.year,
                base_date.month,
                base_date.day,
                hour,
                minute,
                tzinfo=TZINFO
            )
        except ValueError:
            return None

        if dt <= now:
            return None

        return {"remind_at": dt, "message": msg.strip()}

    return None


def parse_chinese_reminder(text: str) -> Optional[Dict[str, Any]]:
    return parse_relative_reminder(text) or parse_absolute_reminder(text)


def reminder_job_id(reminder_id: int) -> str:
    return f"reminder_{reminder_id}"


def schedule_one_reminder(reminder_id: int, chat_id: int, remind_at: datetime, message: str) -> None:
    job_id = reminder_job_id(reminder_id)

    def _send():
        try:
            send_message(chat_id, f"⏰ <b>提醒時間到</b>\n\n{html.escape(message)}")
            mark_reminder_sent(reminder_id)
            logger.info("Reminder sent: id=%s chat_id=%s", reminder_id, chat_id)
        except Exception as e:
            logger.exception("Failed to send reminder id=%s: %s", reminder_id, e)

    try:
        try:
            scheduler.remove_job(job_id)
        except JobLookupError:
            pass

        scheduler.add_job(
            _send,
            trigger="date",
            run_date=remind_at,
            id=job_id,
            replace_existing=True,
            misfire_grace_time=3600,
        )
        logger.info("Reminder scheduled: id=%s at %s", reminder_id, remind_at.isoformat())
    except Exception as e:
        logger.exception("Failed to schedule reminder id=%s: %s", reminder_id, e)


def load_pending_reminders_into_scheduler() -> None:
    rows = get_pending_reminders()
    now = datetime.now(TZINFO)

    for row in rows:
        reminder_id = int(row["id"])
        chat_id = int(row["chat_id"])
        remind_at = datetime.fromisoformat(row["remind_at"])
        if remind_at.tzinfo is None:
            remind_at = remind_at.replace(tzinfo=TZINFO)

        if remind_at <= now:
            try:
                send_message(chat_id, f"⏰ <b>延遲提醒</b>\n\n{html.escape(row['message'])}")
                mark_reminder_sent(reminder_id)
                logger.info("Late reminder sent immediately: id=%s", reminder_id)
            except Exception as e:
                logger.exception("Failed to send late reminder id=%s: %s", reminder_id, e)
            continue

        schedule_one_reminder(reminder_id, chat_id, remind_at, row["message"])


# =========================
# 指令 / 功能
# =========================
def send_daily_news() -> None:
    logger.info("Running scheduled daily news push...")
    chat_ids = get_all_target_chat_ids()

    if not chat_ids:
        logger.warning("No chat ids found. Skip daily news push.")
        return

    try:
        items = fetch_news(category=DEFAULT_NEWS_CATEGORY, limit=DEFAULT_NEWS_LIMIT)
        message = format_news_message(items, category=DEFAULT_NEWS_CATEGORY)

        for chat_id in chat_ids:
            try:
                send_message(chat_id, message)
                logger.info("Daily news sent to %s", chat_id)
            except Exception as e:
                logger.exception("Failed to send daily news to %s: %s", chat_id, e)
    except Exception as e:
        logger.exception("Daily news job failed: %s", e)


def handle_start(chat_id: int) -> None:
    register_chat_id(chat_id)

    msg = (
        "<b>✅ v3.5 最終整合版已啟用</b>\n\n"
        "可用功能：\n"
        "/news\n"
        "/news tech\n"
        "/news business\n"
        "/list\n"
        "/cancel 1\n"
        "/help\n\n"
        "提醒範例：\n"
        "今天下午5點打球\n"
        "明天早上8點提醒我開會\n"
        "2026-03-27 14:30 開會\n"
        "30分鐘後提醒我喝水\n\n"
        "每日新聞固定於早上 8:00 推播。"
    )
    send_message(chat_id, msg)


def handle_help(chat_id: int) -> None:
    msg = (
        "<b>指令說明</b>\n\n"
        "/start\n"
        "/help\n"
        "/news\n"
        "/news tech\n"
        "/news business\n"
        "/list\n"
        "/cancel 編號\n\n"
        "提醒可直接輸入：\n"
        "今天下午5點打球\n"
        "明天早上8點提醒我開會\n"
        "2026-03-27 14:30 開會\n"
        "30分鐘後提醒我喝水"
    )
    send_message(chat_id, msg)


def handle_news(chat_id: int, text: str) -> None:
    register_chat_id(chat_id)
    args = parse_news_command(text)
    items = fetch_news(category=args["category"], limit=args["limit"])
    msg = format_news_message(items, category=args["category"])
    send_message(chat_id, msg)


def handle_list(chat_id: int) -> None:
    reminders = get_user_pending_reminders(chat_id)
    if not reminders:
        send_message(chat_id, "目前沒有未完成提醒。")
        return

    lines = ["<b>📌 未完成提醒</b>", ""]
    for row in reminders[:20]:
        remind_at = datetime.fromisoformat(row["remind_at"])
        if remind_at.tzinfo is None:
            remind_at = remind_at.replace(tzinfo=TZINFO)
        lines.append(
            f"#{row['id']}｜{html.escape(remind_at.strftime('%Y-%m-%d %H:%M'))}\n"
            f"{html.escape(row['message'])}"
        )
        lines.append("")

    send_message(chat_id, "\n".join(lines).strip())


def handle_cancel(chat_id: int, text: str) -> None:
    m = re.match(r"^/cancel\s+(\d+)\s*$", text.strip())
    if not m:
        send_message(chat_id, "用法：/cancel 1")
        return

    reminder_id = int(m.group(1))
    ok = cancel_reminder(reminder_id, chat_id)
    if not ok:
        send_message(chat_id, f"找不到可取消的提醒 #{reminder_id}")
        return

    try:
        scheduler.remove_job(reminder_job_id(reminder_id))
    except JobLookupError:
        pass
    except Exception as e:
        logger.exception("Failed to remove reminder job id=%s: %s", reminder_id, e)

    send_message(chat_id, f"✅ 已取消提醒 #{reminder_id}")


def handle_unknown(chat_id: int) -> None:
    msg = (
        "我目前支援：\n"
        "/start\n"
        "/help\n"
        "/news\n"
        "/news tech\n"
        "/news business\n"
        "/list\n"
        "/cancel 1\n\n"
        "也可以直接輸入提醒，例如：\n"
        "今天下午5點打球"
    )
    send_message(chat_id, msg)


def try_handle_existing_reminder_logic(chat_id: int, text: str) -> bool:
    parsed = parse_chinese_reminder(text)
    if not parsed:
        return False

    remind_at: datetime = parsed["remind_at"]
    reminder_text: str = parsed["message"]

    reminder_id = save_reminder(chat_id, remind_at, reminder_text)
    schedule_one_reminder(reminder_id, chat_id, remind_at, reminder_text)

    confirm = (
        f"✅ 已設定提醒\n"
        f"編號：#{reminder_id}\n"
        f"時間：{html.escape(remind_at.strftime('%Y-%m-%d %H:%M'))}\n"
        f"內容：{html.escape(reminder_text)}"
    )
    send_message(chat_id, confirm)
    return True


# =========================
# 排程
# =========================
def schedule_jobs() -> None:
    if not scheduler.running:
        scheduler.start()

    try:
        scheduler.remove_job("daily_news_job")
    except JobLookupError:
        pass
    except Exception as e:
        logger.exception("Failed removing old daily_news_job: %s", e)

    hour, minute = NEWS_PUSH_TIME.split(":")
    scheduler.add_job(
        send_daily_news,
        trigger="cron",
        hour=int(hour),
        minute=int(minute),
        id="daily_news_job",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )

    logger.info("Scheduler started. Daily news at %s (%s)", NEWS_PUSH_TIME, TIMEZONE)


# =========================
# Flask routes
# =========================
@app.route("/", methods=["GET"])
def home():
    return jsonify(
        {
            "ok": True,
            "service": "telegram-bot-private-news-reminder-final",
            "timezone": TIMEZONE,
            "news_push_time": NEWS_PUSH_TIME,
            "owner_id_set": bool(OWNER_ID),
            "openai_model": OPENAI_MODEL,
            "chinese_summary_enabled": ENABLE_CHINESE_SUMMARY,
        }
    )


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True}), 200


@app.route(f"/{WEBHOOK_SECRET_PATH}", methods=["POST"])
def telegram_webhook():
    update = request.get_json(silent=True) or {}

    try:
        message = update.get("message") or update.get("edited_message")
        if not message:
            return jsonify({"ok": True})

        chat_id = int(message["chat"]["id"])
        text = (message.get("text") or "").strip()

        # 私人模式：只允許 OWNER_ID
        if OWNER_ID and chat_id != OWNER_ID:
            logger.info("Blocked non-owner: %s", chat_id)
            return jsonify({"ok": True})

        if not text:
            return jsonify({"ok": True})

        logger.info("Incoming message from %s: %s", chat_id, text)

        if text.startswith("/start"):
            handle_start(chat_id)
        elif text.startswith("/help"):
            handle_help(chat_id)
        elif text.startswith("/news"):
            handle_news(chat_id, text)
        elif text.startswith("/list"):
            handle_list(chat_id)
        elif text.startswith("/cancel"):
            handle_cancel(chat_id, text)
        else:
            handled = try_handle_existing_reminder_logic(chat_id, text)
            if not handled:
                handle_unknown(chat_id)

        return jsonify({"ok": True})

    except Exception as e:
        logger.exception("Webhook handler error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


# =========================
# 啟動
# =========================
def bootstrap() -> None:
    init_db()

    try:
        set_webhook()
    except Exception as e:
        logger.exception("set_webhook failed: %s", e)

    try:
        schedule_jobs()
        load_pending_reminders_into_scheduler()
    except Exception as e:
        logger.exception("scheduler bootstrap failed: %s", e)


bootstrap()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
