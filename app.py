from config import *
from db import init_db, get_conn
from telegram_api import send_message
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

# v3.6：只有每日推播做中文摘要
ENABLE_CHINESE_SUMMARY = os.getenv("ENABLE_CHINESE_SUMMARY", "true").strip().lower() == "true"
SUMMARY_ONLY_FOR_DAILY_PUSH = os.getenv("SUMMARY_ONLY_FOR_DAILY_PUSH", "true").strip().lower() == "true"

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
            CREATE TABLE IF NOT EXISTS reminder_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                event_time TEXT NOT NULL,
                message TEXT NOT NULL,
                keyword TEXT NOT NULL,
                canceled INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reminder_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                notify_time TEXT NOT NULL,
                notify_type TEXT NOT NULL,
                label TEXT NOT NULL,
                sent INTEGER NOT NULL DEFAULT 0,
                canceled INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY(event_id) REFERENCES reminder_events(id)
            )
            """
        )

        conn.commit()
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

    final_ids = set(ids)
    if OWNER_ID:
        extra = {int(TELEGRAM_CHAT_ID)} if TELEGRAM_CHAT_ID.isdigit() else set()
        final_ids = {OWNER_ID} | extra | final_ids

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





def set_webhook() -> None:
    if not RENDER_EXTERNAL_URL:
        logger.warning("RENDER_EXTERNAL_URL not set. Skip setWebhook.")
        return

    webhook_url = f"{RENDER_EXTERNAL_URL}/{WEBHOOK_SECRET_PATH}"
    payload = {"url": webhook_url}
    data = telegram_api("setWebhook", payload)
    logger.info("Webhook set result: %s", data)


# =========================
# 共用文字工具
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
        published_ts = parse_published_ts(entry)

        items.append(
            {
                "title": title_raw,
                "title_norm": normalize_title(title_raw),
                "link": link,
                "raw_summary": raw_summary,
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


def enrich_news_with_chinese_summary(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []

    for item in items:
        new_item = dict(item)
        new_item["summary"] = summarize_to_chinese(
            title=item["title"],
            raw_summary=item["raw_summary"],
            source_name=item["source"],
        )
        enriched.append(new_item)

    return enriched


def format_news_message(
    items: List[Dict[str, Any]],
    category: str = "all",
    include_summary: bool = True
) -> str:
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
        source_text = html.escape(item["source"])
        link = item["link"]

        block = [
            f"<b>{idx}. {title_text}</b>",
        ]

        if include_summary:
            summary_value = item.get("summary") or item.get("raw_summary") or ""
            block.append(f"摘要：{html.escape(summary_value)}")

        block.append(f"來源：{source_text}")

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
        elif arg1 in ("all", "全部"):
            category = "all"
        elif arg1.isdigit():
            limit = max(1, min(10, int(arg1)))

    if len(parts) >= 3:
        arg2 = parts[2].lower()
        if arg2.isdigit():
            limit = max(1, min(10, int(arg2)))

    return {"category": category, "limit": limit}


# =========================
# 提醒功能：事件型
# =========================
ADVANCE_REMINDER_RULES = [
    ("2h", "- 2小時前", timedelta(hours=2)),
    ("1h", "- 1小時前", timedelta(hours=1)),
    ("30m", "- 30分鐘前", timedelta(minutes=30)),
    ("event", "- 事件時間", timedelta(seconds=0)),
]


def normalize_keyword_for_event(message: str) -> str:
    text = re.sub(r"\s+", "", message.strip().lower())
    return text[:30] if text else "event"


def parse_relative_reminder(text: str) -> Optional[Dict[str, Any]]:
    raw = text.strip()
    now = datetime.now(TZINFO)

    m = re.match(
        r"^\s*(\d+)\s*(分鐘|分|min|mins|minute|minutes|小時|hr|hrs|hour|hours)\s*後\s*(提醒我)?\s*(.+?)\s*$",
        raw,
        re.IGNORECASE
    )
    if not m:
        return None

    amount_str, unit, _, msg = m.groups()
    amount = int(amount_str)
    if amount <= 0:
        return None

    unit = unit.lower()
    if unit in ("分鐘", "分", "min", "mins", "minute", "minutes"):
        event_time = now + timedelta(minutes=amount)
    else:
        event_time = now + timedelta(hours=amount)

    return {"event_time": event_time, "message": msg.strip()}


def parse_absolute_reminder(text: str) -> Optional[Dict[str, Any]]:
    raw = text.strip()
    now = datetime.now(TZINFO)

    # 1️⃣ 完整日期
    m = re.match(r"^\s*(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})\s+(.+?)\s*$", raw)
    if m:
        date_str, hour_str, minute_str, msg = m.groups()
        dt = datetime.strptime(f"{date_str} {hour_str}:{minute_str}", "%Y-%m-%d %H:%M")
        dt = dt.replace(tzinfo=TZINFO)
        if dt <= now:
            return None
        return {"event_time": dt, "message": msg.strip()}

    # 2️⃣ 中文時間
    m = re.match(
        r"^\s*(今天|明天|昨天)?\s*"
        r"(早上|上午|中午|下午|晚上)?\s*"
        r"(\d{1,2})"
        r"(?:(?:\s*[:：]\s*(\d{1,2}))|(?:\s*點\s*(半|(\d{1,2}))?))?"
        r"\s*(?:分)?\s*"
        r"(提醒我)?\s*(.+?)\s*$",
        raw
    )
    if m:
        day_word, period, hour_str, minute_str_colon, half_flag, minute_str_dot, _, msg = m.groups()

        if day_word == "明天":
            base_date = (now + timedelta(days=1)).date()
        elif day_word == "昨天":
            return None
        else:
            base_date = now.date()

        hour = int(hour_str)

        if minute_str_colon is not None:
            minute = int(minute_str_colon)
        elif half_flag == "半":
            minute = 30
        elif minute_str_dot is not None:
            minute = int(minute_str_dot)
        else:
            minute = 0

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

        return {"event_time": dt, "message": msg.strip()}

    return None


def parse_chinese_reminder(text: str) -> Optional[Dict[str, Any]]:
    return parse_relative_reminder(text) or parse_absolute_reminder(text)


def save_event_with_notifications(chat_id: int, event_time: datetime, message: str) -> Dict[str, Any]:
    conn = get_conn()
    try:
        keyword = normalize_keyword_for_event(message)
        now_iso = datetime.now(TZINFO).isoformat()

        cur = conn.execute(
            """
            INSERT INTO reminder_events (chat_id, event_time, message, keyword, canceled, created_at)
            VALUES (?, ?, ?, ?, 0, ?)
            """,
            (chat_id, event_time.isoformat(), message, keyword, now_iso)
        )
        event_id = int(cur.lastrowid)

        notifications = []
        for notify_type, label, delta in ADVANCE_REMINDER_RULES:
            notify_time = event_time - delta if delta.total_seconds() > 0 else event_time

            if notify_time <= datetime.now(TZINFO) and notify_type != "event":
                continue

            cur2 = conn.execute(
                """
                INSERT INTO reminder_notifications
                (event_id, chat_id, notify_time, notify_type, label, sent, canceled, created_at)
                VALUES (?, ?, ?, ?, ?, 0, 0, ?)
                """,
                (
                    event_id,
                    chat_id,
                    notify_time.isoformat(),
                    notify_type,
                    label,
                    now_iso,
                )
            )
            notifications.append(
                {
                    "notification_id": int(cur2.lastrowid),
                    "notify_time": notify_time,
                    "notify_type": notify_type,
                    "label": label,
                }
            )

        conn.commit()
        return {
            "event_id": event_id,
            "event_time": event_time,
            "message": message,
            "keyword": keyword,
            "notifications": notifications,
        }
    finally:
        conn.close()


def get_pending_notifications() -> List[sqlite3.Row]:
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            SELECT
                rn.id,
                rn.event_id,
                rn.chat_id,
                rn.notify_time,
                rn.notify_type,
                rn.label,
                re.event_time,
                re.message
            FROM reminder_notifications rn
            JOIN reminder_events re ON rn.event_id = re.id
            WHERE rn.sent = 0 AND rn.canceled = 0 AND re.canceled = 0
            ORDER BY rn.notify_time ASC
            """
        )
        return cur.fetchall()
    finally:
        conn.close()


def get_user_pending_events(chat_id: int) -> List[sqlite3.Row]:
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            SELECT id, event_time, message, keyword
            FROM reminder_events
            WHERE chat_id = ? AND canceled = 0
            ORDER BY event_time ASC
            """,
            (chat_id,)
        )
        return cur.fetchall()
    finally:
        conn.close()


def mark_notification_sent(notification_id: int) -> None:
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE reminder_notifications SET sent = 1 WHERE id = ?",
            (notification_id,)
        )
        conn.commit()
    finally:
        conn.close()


def cancel_event_by_id(event_id: int, chat_id: int) -> bool:
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            UPDATE reminder_events
            SET canceled = 1
            WHERE id = ? AND chat_id = ? AND canceled = 0
            """,
            (event_id, chat_id)
        )
        if cur.rowcount <= 0:
            conn.commit()
            return False

        conn.execute(
            """
            UPDATE reminder_notifications
            SET canceled = 1
            WHERE event_id = ?
            """,
            (event_id,)
        )
        conn.commit()
        return True
    finally:
        conn.close()


def find_latest_event_by_keyword(chat_id: int, keyword: str) -> Optional[sqlite3.Row]:
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            SELECT id, event_time, message, keyword
            FROM reminder_events
            WHERE chat_id = ?
              AND canceled = 0
              AND (
                    lower(message) LIKE ?
                    OR lower(keyword) LIKE ?
                  )
            ORDER BY event_time DESC
            LIMIT 1
            """,
            (chat_id, f"%{keyword.lower()}%", f"%{keyword.lower()}%")
        )
        return cur.fetchone()
    finally:
        conn.close()


def notification_job_id(notification_id: int) -> str:
    return f"notify_{notification_id}"


def build_notification_text(label: str, event_time: datetime, message: str, event_id: int) -> str:
    return (
        "⏰ <b>提醒通知</b>\n"
        f"{html.escape(event_time.strftime('%Y-%m-%d %H:%M'))}｜{html.escape(message)}"
    )


def schedule_one_notification(
    notification_id: int,
    event_id: int,
    chat_id: int,
    notify_time: datetime,
    label: str,
    event_time: datetime,
    message: str
) -> None:
    job_id = notification_job_id(notification_id)

    def _send():
        try:
            text = build_notification_text(
                label=label,
                event_time=event_time,
                message=message,
                event_id=event_id,
            )
            send_message(chat_id, text)
            mark_notification_sent(notification_id)
            logger.info("Notification sent: id=%s event_id=%s", notification_id, event_id)
        except Exception as e:
            logger.exception("Failed to send notification id=%s: %s", notification_id, e)

    try:
        try:
            scheduler.remove_job(job_id)
        except JobLookupError:
            pass

        scheduler.add_job(
            _send,
            trigger="date",
            run_date=notify_time,
            id=job_id,
            replace_existing=True,
            misfire_grace_time=3600,
        )
    except Exception as e:
        logger.exception("Failed to schedule notification id=%s: %s", notification_id, e)


def load_pending_notifications_into_scheduler() -> None:
    rows = get_pending_notifications()
    now = datetime.now(TZINFO)

    for row in rows:
        notification_id = int(row["id"])
        event_id = int(row["event_id"])
        chat_id = int(row["chat_id"])

        notify_time = datetime.fromisoformat(row["notify_time"])
        if notify_time.tzinfo is None:
            notify_time = notify_time.replace(tzinfo=TZINFO)

        event_time = datetime.fromisoformat(row["event_time"])
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=TZINFO)

        label = row["label"]
        message = row["message"]

        if notify_time <= now:
            try:
                text = build_notification_text(label, event_time, message, event_id)
                send_message(chat_id, text)
                mark_notification_sent(notification_id)
                logger.info("Late notification sent immediately: id=%s", notification_id)
            except Exception as e:
                logger.exception("Failed to send late notification id=%s: %s", notification_id, e)
            continue

        schedule_one_notification(
            notification_id=notification_id,
            event_id=event_id,
            chat_id=chat_id,
            notify_time=notify_time,
            label=label,
            event_time=event_time,
            message=message,
        )


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

        if ENABLE_CHINESE_SUMMARY:
            items = enrich_news_with_chinese_summary(items)

        message = format_news_message(
            items,
            category=DEFAULT_NEWS_CATEGORY,
            include_summary=True
        )

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
        "<b>✅ Bot 已啟用</b>\n\n"
        "可用功能：\n"
        "/news\n"
        "/news tech\n"
        "/news business\n"
        "/list\n"
        "/cancel 事件代碼\n"
        "/help\n\n"
        "提醒可直接輸入：\n"
        "晚上7點半打球\n"
        "明天晚上7點半打球\n"
        "2026-03-27 14:30 開會\n"
        "30分鐘後提醒我喝水"
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
        "/cancel 事件代碼\n\n"
        "提醒輸入範例：\n"
        "晚上7點半打球\n"
        "明天晚上7點半打球\n"
        "30分鐘後提醒我喝水\n\n"
        "取消範例：\n"
        "/cancel 12\n"
        "取消打球"
    )
    send_message(chat_id, msg)


def handle_news(chat_id: int, text: str) -> None:
    register_chat_id(chat_id)
    args = parse_news_command(text)
    items = fetch_news(category=args["category"], limit=args["limit"])

    if ENABLE_CHINESE_SUMMARY and not SUMMARY_ONLY_FOR_DAILY_PUSH:
        items = enrich_news_with_chinese_summary(items)
        msg = format_news_message(items, category=args["category"], include_summary=True)
    else:
        msg = format_news_message(items, category=args["category"], include_summary=False)

    send_message(chat_id, msg)


def handle_list(chat_id: int) -> None:
    rows = get_user_pending_events(chat_id)
    if not rows:
        send_message(chat_id, "目前沒有未取消事件提醒。")
        return

    lines = ["<b>📌 未取消事件提醒</b>", ""]
    for row in rows[:20]:
        event_time = datetime.fromisoformat(row["event_time"])
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=TZINFO)

        lines.append(
            f"事件代碼：<b>{row['id']}</b>\n"
            f"{html.escape(event_time.strftime('%Y-%m-%d %H:%M'))}｜{html.escape(row['message'])}"
        )
        lines.append("")

    send_message(chat_id, "\n".join(lines).strip())


def handle_cancel(chat_id: int, text: str) -> None:
    m = re.match(r"^/cancel\s+(\d+)\s*$", text.strip())
    if not m:
        send_message(chat_id, "用法：/cancel 事件代碼")
        return

    event_id = int(m.group(1))
    ok = cancel_event_by_id(event_id, chat_id)
    if not ok:
        send_message(chat_id, f"找不到可取消的事件代碼 #{event_id}")
        return

    pending = get_pending_notifications()
    for row in pending:
        if int(row["event_id"]) == event_id:
            try:
                scheduler.remove_job(notification_job_id(int(row["id"])))
            except JobLookupError:
                pass
            except Exception as e:
                logger.exception("remove job failed: %s", e)

    send_message(chat_id, "✅ 已取消提醒")


def handle_cancel_by_keyword(chat_id: int, text: str) -> bool:
    m = re.match(r"^\s*取消\s*(.+?)\s*$", text)
    if not m:
        return False

    keyword = m.group(1).strip()
    if not keyword:
        return False

    row = find_latest_event_by_keyword(chat_id, keyword)
    if not row:
        send_message(chat_id, f"找不到符合「{html.escape(keyword)}」的未取消事件。")
        return True

    event_id = int(row["id"])
    event_time = datetime.fromisoformat(row["event_time"])
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=TZINFO)

    ok = cancel_event_by_id(event_id, chat_id)
    if not ok:
        send_message(chat_id, "取消失敗，請稍後再試。")
        return True

    pending = get_pending_notifications()
    for n in pending:
        if int(n["event_id"]) == event_id:
            try:
                scheduler.remove_job(notification_job_id(int(n["id"])))
            except JobLookupError:
                pass
            except Exception as e:
                logger.exception("remove job failed: %s", e)

    msg = (
        "✅ 已取消提醒\n"
        f"{html.escape(event_time.strftime('%Y-%m-%d %H:%M'))}｜{html.escape(row['message'])}"
    )
    send_message(chat_id, msg)
    return True


def handle_unknown(chat_id: int) -> None:
    msg = (
        "我目前支援：\n"
        "/start\n"
        "/help\n"
        "/news\n"
        "/news tech\n"
        "/news business\n"
        "/list\n"
        "/cancel 事件代碼\n\n"
        "也可以直接輸入提醒，例如：\n"
        "晚上7點半打球\n"
        "明天晚上7點半打球\n\n"
        "取消也可直接輸入：\n"
        "取消打球"
    )
    send_message(chat_id, msg)


def try_handle_event_reminder(chat_id: int, text: str) -> bool:
    parsed = parse_chinese_reminder(text)
    if not parsed:
        return False

    event_time: datetime = parsed["event_time"]
    message: str = parsed["message"]

    result = save_event_with_notifications(chat_id, event_time, message)

    for n in result["notifications"]:
        schedule_one_notification(
            notification_id=n["notification_id"],
            event_id=result["event_id"],
            chat_id=chat_id,
            notify_time=n["notify_time"],
            label=n["label"],
            event_time=event_time,
            message=message,
        )

    lines = [
        "✅ 已建立提醒",
        f"{html.escape(event_time.strftime('%Y-%m-%d %H:%M'))}｜{html.escape(message)}",
    ]

    send_message(chat_id, "\n".join(lines))
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
            "service": "telegram-bot-private-news-reminder-v3_6",
            "timezone": TIMEZONE,
            "news_push_time": NEWS_PUSH_TIME,
            "owner_id_set": bool(OWNER_ID),
            "openai_model": OPENAI_MODEL,
            "chinese_summary_enabled": ENABLE_CHINESE_SUMMARY,
            "summary_only_for_daily_push": SUMMARY_ONLY_FOR_DAILY_PUSH,
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
            if handle_cancel_by_keyword(chat_id, text):
                return jsonify({"ok": True})

            handled = try_handle_event_reminder(chat_id, text)
            if not handled:
                send_message(chat_id, "⚠️ 時間已過或格式錯誤，請重新輸入")

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
        load_pending_notifications_into_scheduler()
    except Exception as e:
        logger.exception("scheduler bootstrap failed: %s", e)


bootstrap()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
