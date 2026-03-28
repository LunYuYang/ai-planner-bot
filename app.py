import os
import re
import json
import html
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

import requests
import feedparser
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.base import JobLookupError
from zoneinfo import ZoneInfo
from openai import OpenAI

from config import (
    BOT_TOKEN,
    OWNER_ID,
    TIMEZONE,
    RENDER_EXTERNAL_URL,
    WEBHOOK_SECRET_PATH,
    DB_PATH,
    DEFAULT_NEWS_CATEGORY,
    DEFAULT_NEWS_LIMIT,
    NEWS_PUSH_TIME,
    ENABLE_CHINESE_SUMMARY,
    OPENAI_API_KEY,
    OPENAI_MODEL,
    TELEGRAM_CHAT_ID,
)
from db import init_db, get_conn
from telegram_api import send_message


HTTP_TIMEOUT = 20

DATA_DIR = os.getenv("DATA_DIR", os.path.dirname(DB_PATH) or ".").strip()
CHAT_FILE = os.path.join(DATA_DIR, "chat_ids.json")

CWA_API_KEY = os.getenv("CWA_API_KEY", "").strip()
DEFAULT_WEATHER_CITY = os.getenv("DEFAULT_WEATHER_CITY", "臺南市").strip()
WEATHER_PUSH_TIME = os.getenv("WEATHER_PUSH_TIME", "").strip()

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


def row_get(row: Any, key: str, index: int = 0, default: Any = None) -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[index]
    except Exception:
        return default


def parse_db_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZINFO)

    return dt.astimezone(TZINFO)


RSS_FEEDS = {
    "tech": [
        "https://news.google.com/rss/headlines/section/topic/TECHNOLOGY?hl=en-US&gl=US&ceid=US:en",
        "https://feeds.npr.org/1019/rss.xml",
        "https://news.google.com/rss/search?q=AI+OR+artificial+intelligence+OR+OpenAI+OR+NVIDIA+OR+Google+DeepMind&hl=en-US&gl=US&ceid=US:en",
    ],
    "business": [
        "https://news.google.com/rss/headlines/section/topic/BUSINESS?hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=finance+OR+stock+OR+market+OR+economy+OR+earnings+OR+Federal+Reserve&hl=en-US&gl=US&ceid=US:en",
    ],
}


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
    return sorted(list(set(ids)))


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
    webhook_url = f"{RENDER_EXTERNAL_URL.rstrip('/')}/{WEBHOOK_SECRET_PATH}"
    payload = {"url": webhook_url}
    data = telegram_api("setWebhook", payload)
    logger.info("Webhook set result: %s", data)


def clean_html_text(raw: str) -> str:
    if not raw:
        return ""
    text = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def trim_text(text: str, max_len: int = 110) -> str:
    text = (text or "").strip()
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
    candidates = [entry.get("summary", ""), entry.get("description", "")]
    for raw in candidates:
        cleaned = clean_html_text(raw)
        title_clean = clean_html_text(entry.get("title", ""))
        if cleaned and cleaned != title_clean:
            return trim_text(cleaned, 220)
    return f"Latest report from {source_name}. Open the link for full details."


def summarize_to_chinese(title: str, raw_summary: str, source_name: str) -> str:
    fallback = trim_text(raw_summary, 110)

    if not ENABLE_CHINESE_SUMMARY:
        return fallback

    if not client:
        logger.warning("OPENAI_API_KEY not set, fallback to raw summary.")
        return fallback

    try:
        prompt = f"""
請將以下科技、AI、商業或財經新聞整理成繁體中文摘要。

要求：
1. 使用繁體中文
2. 30~60字左右
3. 精簡、自然、像新聞快報
4. 不要加入未提供的推測
5. 優先保留公司、產品、產業與商業重點
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
                {"role": "system", "content": "你是擅長整理國際科技、AI、商業與財經新聞的繁體中文編輯，輸出精簡、清楚、自然。"},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            max_tokens=120,
        )

        content = (resp.choices[0].message.content or "").strip()
        content = re.sub(r"^摘要[:：]\s*", "", content)
        content = re.sub(r"\s+", " ", content).strip()
        return trim_text(content, 110) if content else fallback
    except Exception as e:
        logger.exception("Chinese summary failed: %s", e)
        return fallback


def fetch_rss_items(feed_url: str, category: str) -> List[Dict[str, Any]]:
    parsed = feedparser.parse(feed_url)
    feed_title = clean_html_text(parsed.feed.get("title", ""))

    items: List[Dict[str, Any]] = []
    for entry in parsed.entries[:20]:
        title = clean_html_text(entry.get("title", ""))
        link = entry.get("link", "")
        if not title or not link:
            continue

        source_name = extract_source_name(feed_title, entry)
        raw_summary = build_raw_summary(entry, source_name)
        zh_summary = summarize_to_chinese(title, raw_summary, source_name)

        items.append(
            {
                "category": category,
                "title": title,
                "link": link,
                "source": source_name,
                "summary": zh_summary,
                "published_ts": parse_published_ts(entry),
            }
        )
    return items


def dedupe_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best_map: Dict[str, Dict[str, Any]] = {}
    for item in items:
        key = normalize_title(item["title"])
        current = best_map.get(key)
        if not current or item["published_ts"] > current["published_ts"]:
            best_map[key] = item
    return list(best_map.values())


def get_news(category: str, limit: int) -> List[Dict[str, Any]]:
    category = (category or DEFAULT_NEWS_CATEGORY).lower().strip()
    if category not in RSS_FEEDS:
        category = DEFAULT_NEWS_CATEGORY

    collected: List[Dict[str, Any]] = []
    for feed_url in RSS_FEEDS[category]:
        try:
            collected.extend(fetch_rss_items(feed_url, category))
        except Exception as e:
            logger.exception("Fetch RSS failed (%s): %s", feed_url, e)

    deduped = dedupe_items(collected)
    deduped.sort(key=lambda x: x.get("published_ts", 0), reverse=True)
    return deduped[:limit]


def format_news_message(items: List[Dict[str, Any]], title: str = "今日新聞") -> str:
    if not items:
        return f"{title}\n\n目前抓不到新聞，請稍後再試。"

    lines = [title, ""]
    for idx, item in enumerate(items, start=1):
        lines.append(f"{idx}. {item['title']}")
        lines.append(f"來源：{item['source']}")
        lines.append(f"重點：{item['summary']}")
        lines.append(item["link"])
        lines.append("")
    return "\n".join(lines).strip()


def push_news_to_all(category: Optional[str] = None, limit: Optional[int] = None) -> None:
    category = (category or DEFAULT_NEWS_CATEGORY).lower().strip()
    limit = limit or DEFAULT_NEWS_LIMIT

    items = get_news(category, limit)
    title = "早安，這是今天的新聞整理"
    msg = format_news_message(items, title=title)

    for chat_id in get_all_target_chat_ids():
        try:
            send_message(chat_id, msg)
        except Exception as e:
            logger.exception("Push news failed to %s: %s", chat_id, e)


def weather_location_name(city: str) -> str:
    return city if city else DEFAULT_WEATHER_CITY


def fetch_weather_data(city: str) -> Dict[str, Any]:
    city = weather_location_name(city)
    if not CWA_API_KEY:
        raise RuntimeError("Missing CWA_API_KEY")

    url = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/F-C0032-001"
    params = {
        "Authorization": CWA_API_KEY,
        "format": "JSON",
        "locationName": city,
    }
    resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    locations = data.get("records", {}).get("location", [])
    if not locations:
        raise RuntimeError(f"查無天氣資料：{city}")

    location = locations[0]
    weather_elements = location.get("weatherElement", [])

    result = {
        "city": city,
        "wx": [],
        "pop": [],
        "minT": [],
        "maxT": [],
    }

    for element in weather_elements:
        name = element.get("elementName")
        times = element.get("time", [])
        parsed_times = []
        for t in times:
            parsed_times.append(
                {
                    "start": t.get("startTime", ""),
                    "end": t.get("endTime", ""),
                    "value": ((t.get("parameter", {}) or {}).get("parameterName", "")),
                }
            )
        if name in result:
            result[name] = parsed_times

    return result


def build_weather_message(city: str) -> str:
    data = fetch_weather_data(city)
    city_name = data["city"]

    lines = [f"{city_name} 天氣預報", ""]
    for idx in range(min(3, len(data["wx"]))):
        start = data["wx"][idx]["start"]
        end = data["wx"][idx]["end"]
        wx = data["wx"][idx]["value"]
        pop = data["pop"][idx]["value"] if idx < len(data["pop"]) else ""
        min_t = data["minT"][idx]["value"] if idx < len(data["minT"]) else ""
        max_t = data["maxT"][idx]["value"] if idx < len(data["maxT"]) else ""

        lines.append(f"時段：{start} ~ {end}")
        lines.append(f"天氣：{wx}")
        lines.append(f"降雨機率：{pop}%")
        if min_t and max_t:
            lines.append(f"溫度：{min_t}~{max_t}°C")
        lines.append("")

    return "\n".join(lines).strip()


def push_weather_to_all(city: Optional[str] = None) -> None:
    city = city or DEFAULT_WEATHER_CITY
    try:
        msg = "早安，這是今天的天氣資訊\n\n" + build_weather_message(city)
    except Exception as e:
        logger.exception("Push weather failed when building message: %s", e)
        msg = f"早安，今天天氣資訊暫時抓取失敗：{e}"

    for chat_id in get_all_target_chat_ids():
        try:
            send_message(chat_id, msg)
        except Exception as e:
            logger.exception("Push weather failed to %s: %s", chat_id, e)


CHINESE_DIGITS = {
    "零": 0,
    "〇": 0,
    "一": 1,
    "二": 2,
    "兩": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
}


def now_local() -> datetime:
    return datetime.now(TZINFO)


def strip_reminder_prefix(text: str) -> str:
    text = text.strip()
    prefixes = ["提醒我", "提醒", "記得", "幫我提醒", "請提醒我", "請提醒"]
    for prefix in prefixes:
        if text.startswith(prefix):
            return text[len(prefix):].strip()
    return text


def remove_leading_connectors(text: str) -> str:
    text = text.strip()
    while True:
        new_text = re.sub(r"^(要|去|來|再|一下|一下要|一下去)\s*", "", text)
        if new_text == text:
            break
        text = new_text.strip()
    return text


def is_valid_message_text(message: str) -> bool:
    return bool(message and message.strip())


def normalize_space(text: str) -> str:
    text = text.replace("：", ":")
    text = text.replace("，", " ")
    text = text.replace("。", " ")
    text = text.replace("、", " ")
    text = text.replace("（", "(").replace("）", ")")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def chinese_num_to_int(text: str) -> Optional[int]:
    text = text.strip()
    if not text:
        return None

    if text.isdigit():
        return int(text)

    if text == "十":
        return 10

    if all(ch in CHINESE_DIGITS for ch in text):
        value = 0
        for ch in text:
            value = value * 10 + CHINESE_DIGITS[ch]
        return value

    if "十" in text:
        parts = text.split("十")
        if len(parts) != 2:
            return None

        left, right = parts
        if left == "":
            tens = 1
        elif left in CHINESE_DIGITS:
            tens = CHINESE_DIGITS[left]
        else:
            return None

        if right == "":
            ones = 0
        elif right in CHINESE_DIGITS:
            ones = CHINESE_DIGITS[right]
        else:
            return None

        return tens * 10 + ones

    return None


def parse_time_of_day(hour: int, minute: int, meridiem: Optional[str]) -> Tuple[int, int]:
    if meridiem in ("下午", "晚上"):
        if hour < 12:
            hour += 12
    elif meridiem == "中午":
        if hour != 12:
            hour += 12
    elif meridiem == "凌晨":
        if hour == 12:
            hour = 0
    elif meridiem in ("早上", "上午", "清晨"):
        if hour == 12:
            hour = 0
    return hour, minute


def parse_relative_reminder(text: str) -> Optional[Dict[str, Any]]:
    original = text.strip()
    text = strip_reminder_prefix(original)
    text = normalize_space(text)

    patterns = [
        r"^(?P<num>\d+|[零〇一二兩三四五六七八九十]+)\s*分鐘後(?P<msg>.+)$",
        r"^(?P<num>\d+|[零〇一二兩三四五六七八九十]+)\s*分後(?P<msg>.+)$",
        r"^(?P<num>\d+|[零〇一二兩三四五六七八九十]+)\s*小時後(?P<msg>.+)$",
        r"^(?P<num>\d+|[零〇一二兩三四五六七八九十]+)\s*個小時後(?P<msg>.+)$",
    ]

    for pattern in patterns:
        m = re.match(pattern, text)
        if not m:
            continue

        num = chinese_num_to_int(m.group("num"))
        msg = remove_leading_connectors(m.group("msg").strip())
        if not num or not is_valid_message_text(msg):
            return None

        if "分鐘" in pattern or "分後" in pattern:
            target_time = now_local() + timedelta(minutes=num)
        else:
            target_time = now_local() + timedelta(hours=num)

        return {
            "event_time": target_time,
            "message": msg,
            "keyword": msg[:40],
            "source_text": original,
        }

    return None


def parse_absolute_reminder(text: str) -> Optional[Dict[str, Any]]:
    original = text.strip()
    text = strip_reminder_prefix(original)
    text = normalize_space(text)

    base_date = now_local().date()

    day_offset = None
    for token, offset in [("今天", 0), ("明天", 1), ("昨天", -1), ("後天", 2)]:
        if token in text:
            day_offset = offset
            text = text.replace(token, " ")
            break

    if day_offset is None:
        if re.search(r"\d{1,2}\s*/\s*\d{1,2}", text):
            pass
        elif re.search(r"\d{1,2}\s*月\s*\d{1,2}\s*日", text):
            pass
        else:
            day_offset = 0

    target_date = base_date + timedelta(days=day_offset or 0)

    md_match = re.search(r"(?P<month>\d{1,2})\s*/\s*(?P<day>\d{1,2})", text)
    if md_match:
        month = int(md_match.group("month"))
        day = int(md_match.group("day"))
        year = base_date.year
        try:
            candidate = datetime(year, month, day, tzinfo=TZINFO).date()
            if candidate < base_date:
                candidate = datetime(year + 1, month, day, tzinfo=TZINFO).date()
            target_date = candidate
            text = text.replace(md_match.group(0), " ")
        except ValueError:
            return None
    else:
        md_match = re.search(r"(?P<month>\d{1,2})\s*月\s*(?P<day>\d{1,2})\s*日?", text)
        if md_match:
            month = int(md_match.group("month"))
            day = int(md_match.group("day"))
            year = base_date.year
            try:
                candidate = datetime(year, month, day, tzinfo=TZINFO).date()
                if candidate < base_date:
                    candidate = datetime(year + 1, month, day, tzinfo=TZINFO).date()
                target_date = candidate
                text = text.replace(md_match.group(0), " ")
            except ValueError:
                return None

    text = re.sub(r"(在|於)\s*", " ", text)

    time_pattern = (
        r"(?P<meridiem>凌晨|早上|上午|中午|下午|晚上)?\s*"
        r"(?P<hour>\d{1,2})"
        r"(?:\s*[:點]\s*(?P<minute>\d{1,2}))?"
        r"\s*(?:分)?"
    )
    m = re.search(time_pattern, text)
    if not m:
        return None

    meridiem = m.group("meridiem")
    hour = int(m.group("hour"))
    minute = int(m.group("minute") or 0)

    if hour > 23 or minute > 59:
        return None

    if meridiem:
        hour, minute = parse_time_of_day(hour, minute, meridiem)

    event_time = datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        hour,
        minute,
        tzinfo=TZINFO,
    )

    if day_offset == 0 and not re.search(r"(今天|明天|昨天|後天|\d{1,2}\s*/\s*\d{1,2}|\d{1,2}\s*月\s*\d{1,2}\s*日?)", original):
        if event_time < now_local():
            event_time += timedelta(days=1)

    msg = (text[:m.start()] + " " + text[m.end():]).strip()
    msg = remove_leading_connectors(msg)
    msg = re.sub(r"\s+", " ", msg).strip()

    if not is_valid_message_text(msg):
        return None

    return {
        "event_time": event_time,
        "message": msg,
        "keyword": msg[:40],
        "source_text": original,
    }


def parse_chinese_reminder(text: str) -> Optional[Dict[str, Any]]:
    return parse_relative_reminder(text) or parse_absolute_reminder(text)


def split_multi_reminder_text(text: str) -> List[str]:
    normalized = text.replace("；", ";")
    normalized = re.sub(r"\s*;\s*", "\n", normalized)
    parts = [part.strip() for part in normalized.splitlines()]
    return [part for part in parts if part]


def parse_multiple_chinese_reminders(text: str) -> Tuple[List[Tuple[int, str, Dict[str, Any]]], List[Tuple[int, str]]]:
    parts = split_multi_reminder_text(text)
    if not parts:
        return [], []

    success_items: List[Tuple[int, str, Dict[str, Any]]] = []
    failed_items: List[Tuple[int, str]] = []

    for idx, part in enumerate(parts, start=1):
        parsed = parse_chinese_reminder(part)
        if parsed:
            success_items.append((idx, part, parsed))
        else:
            failed_items.append((idx, part))

    return success_items, failed_items


def get_notification_offsets(event_time: datetime) -> List[int]:
    now = now_local()
    offsets = []

    for delta in [timedelta(hours=2), timedelta(hours=1), timedelta(minutes=30)]:
        notify_time = event_time - delta
        if notify_time > now:
            offsets.append(int((event_time - notify_time).total_seconds()))

    offsets.append(0)
    return sorted(list(set(offsets)), reverse=True)


def migrate_tables() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'reminder_events'
                """
            )
            existing_columns = {row_get(row, "column_name") for row in cur.fetchall()}

            if "completed_at" not in existing_columns:
                cur.execute(
                    "ALTER TABLE reminder_events ADD COLUMN completed_at TIMESTAMP NULL"
                )
                logger.info("Added completed_at column to reminder_events")

            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'reminder_notifications'
                """
            )
            notification_columns = {row_get(row, "column_name") for row in cur.fetchall()}

            if "notify_at" not in notification_columns:
                cur.execute(
                    "ALTER TABLE reminder_notifications ADD COLUMN notify_at TIMESTAMP NULL"
                )
                logger.info("Added notify_at column to reminder_notifications")

            if "offset_seconds" not in notification_columns:
                cur.execute(
                    "ALTER TABLE reminder_notifications ADD COLUMN offset_seconds INTEGER NOT NULL DEFAULT 0"
                )
                logger.info("Added offset_seconds column to reminder_notifications")

            if "sent" not in notification_columns:
                cur.execute(
                    "ALTER TABLE reminder_notifications ADD COLUMN sent BOOLEAN NOT NULL DEFAULT FALSE"
                )
                logger.info("Added sent column to reminder_notifications")

        conn.commit()


def save_event_and_notifications(chat_id: int, event_time: datetime, message: str, keyword: str) -> Tuple[int, List[Tuple[int, datetime, int]]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO reminder_events (chat_id, event_time, message, keyword, canceled, created_at)
                VALUES (%s, %s, %s, %s, FALSE, NOW())
                RETURNING id
                """,
                (chat_id, event_time.replace(tzinfo=None), message, keyword),
            )
            event_row = cur.fetchone()
            event_id = int(row_get(event_row, "id", 0))

            offsets = get_notification_offsets(event_time)
            notification_rows: List[Tuple[int, datetime, int]] = []

            for offset_seconds in offsets:
                notify_at = event_time - timedelta(seconds=offset_seconds)
                cur.execute(
                    """
                    INSERT INTO reminder_notifications
                    (event_id, notify_at, offset_seconds, sent, created_at)
                    VALUES (%s, %s, %s, FALSE, NOW())
                    ON CONFLICT (event_id, offset_seconds) DO NOTHING
                    RETURNING id
                    """,
                    (
                        event_id,
                        notify_at.replace(tzinfo=None),
                        offset_seconds,
                    ),
                )
                inserted = cur.fetchone()
                if inserted:
                    notification_id = int(row_get(inserted, "id", 0))
                else:
                    cur.execute(
                        """
                        SELECT id
                        FROM reminder_notifications
                        WHERE event_id = %s AND offset_seconds = %s
                        """,
                        (event_id, offset_seconds),
                    )
                    existing = cur.fetchone()
                    notification_id = int(row_get(existing, "id", 0))

                notification_rows.append((notification_id, notify_at, offset_seconds))

        conn.commit()
        return event_id, notification_rows


def offset_label(offset_seconds: int) -> str:
    if offset_seconds == 7200:
        return "前2小時"
    if offset_seconds == 3600:
        return "前1小時"
    if offset_seconds == 1800:
        return "前30分鐘"
    return "時間到"


def schedule_single_notification(notification_id: int, event_id: int, chat_id: int, event_time: datetime, message: str, offset_seconds: int, notify_at: datetime) -> None:
    job_id = f"notif_{notification_id}"
    scheduler.add_job(
        send_event_notification,
        "date",
        run_date=notify_at,
        id=job_id,
        replace_existing=True,
        kwargs={
            "notification_id": notification_id,
            "event_id": event_id,
            "chat_id": chat_id,
            "event_time_iso": event_time.isoformat(),
            "message": message,
            "offset_seconds": offset_seconds,
        },
        misfire_grace_time=3600,
    )


def remove_scheduler_jobs_for_event(event_id: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM reminder_notifications
                WHERE event_id = %s
                """,
                (event_id,),
            )
            rows = cur.fetchall()

    for row in rows:
        notification_id = int(row_get(row, "id", 0))
        job_id = f"notif_{notification_id}"
        try:
            scheduler.remove_job(job_id)
        except JobLookupError:
            pass
        except Exception as e:
            logger.exception("Failed to remove job %s: %s", job_id, e)


def load_pending_notifications_into_scheduler() -> None:
    now_naive = now_local().replace(tzinfo=None)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    rn.id,
                    rn.event_id,
                    re.chat_id,
                    re.event_time,
                    re.message,
                    rn.offset_seconds,
                    rn.notify_at
                FROM reminder_notifications rn
                JOIN reminder_events re ON rn.event_id = re.id
                WHERE re.canceled = FALSE
                  AND rn.sent = FALSE
                  AND rn.notify_at > %s
                ORDER BY rn.notify_at ASC
                """,
                (now_naive,),
            )
            rows = cur.fetchall()

    for row in rows:
        try:
            notification_id = int(row_get(row, "id", 0))
            event_id = int(row_get(row, "event_id", 1))
            chat_id = int(row_get(row, "chat_id", 2))
            event_time = parse_db_datetime(row_get(row, "event_time", 3))
            message = row_get(row, "message", 4, "")
            offset_seconds = int(row_get(row, "offset_seconds", 5))
            notify_at = parse_db_datetime(row_get(row, "notify_at", 6))

            schedule_single_notification(
                notification_id=notification_id,
                event_id=event_id,
                chat_id=chat_id,
                event_time=event_time,
                message=message,
                offset_seconds=offset_seconds,
                notify_at=notify_at,
            )
        except Exception as e:
            logger.exception("Failed loading pending notification row=%s error=%s", row, e)


def get_event_list(chat_id: int) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    event_time,
                    message,
                    keyword,
                    canceled,
                    created_at,
                    completed_at
                FROM reminder_events
                WHERE chat_id = %s
                  AND canceled = FALSE
                ORDER BY event_time ASC, id ASC
                """,
                (chat_id,),
            )
            rows = cur.fetchall()

    result = []
    for row in rows:
        result.append(
            {
                "id": int(row_get(row, "id", 0)),
                "event_time": parse_db_datetime(row_get(row, "event_time", 1)),
                "message": row_get(row, "message", 2, ""),
                "keyword": row_get(row, "keyword", 3, "") or "",
                "canceled": bool(row_get(row, "canceled", 4, False)),
                "created_at": parse_db_datetime(row_get(row, "created_at", 5)),
                "completed_at": parse_db_datetime(row_get(row, "completed_at", 6)) if row_get(row, "completed_at", 6) else None,
            }
        )
    return result


def format_event_line(index: int, item: Dict[str, Any]) -> str:
    event_time = item["event_time"].strftime("%m/%d %H:%M")
    return f"{index}. {event_time} {item['message']}"


def cancel_event_by_index(chat_id: int, index: int) -> Optional[Dict[str, Any]]:
    items = get_event_list(chat_id)
    if index < 1 or index > len(items):
        return None

    target = items[index - 1]
    event_id = target["id"]

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE reminder_events
                SET canceled = TRUE
                WHERE id = %s
                """,
                (event_id,),
            )
        conn.commit()

    remove_scheduler_jobs_for_event(event_id)
    return target


def cancel_event_by_keyword(chat_id: int, keyword: str) -> Optional[Dict[str, Any]]:
    keyword = keyword.strip()
    if not keyword:
        return None

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    re.id,
                    re.event_time,
                    re.message,
                    re.keyword
                FROM reminder_events re
                WHERE re.chat_id = %s
                  AND re.canceled = FALSE
                  AND (
                        re.message ILIKE %s
                        OR COALESCE(re.keyword, '') ILIKE %s
                  )
                ORDER BY re.event_time ASC, re.id ASC
                LIMIT 1
                """,
                (chat_id, f"%{keyword}%", f"%{keyword}%"),
            )
            row = cur.fetchone()

            if not row:
                return None

            event_id = int(row_get(row, "id", 0))
            cur.execute(
                """
                UPDATE reminder_events
                SET canceled = TRUE
                WHERE id = %s
                """,
                (event_id,),
            )
        conn.commit()

    remove_scheduler_jobs_for_event(event_id)

    return {
        "id": event_id,
        "event_time": parse_db_datetime(row_get(row, "event_time", 1)),
        "message": row_get(row, "message", 2, ""),
        "keyword": row_get(row, "keyword", 3, "") or "",
    }


def is_notification_already_sent(notification_id: int) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sent
                FROM reminder_notifications
                WHERE id = %s
                """,
                (notification_id,),
            )
            row = cur.fetchone()

    if not row:
        return True
    return bool(row_get(row, "sent", 0, False))


def claim_notification_for_send(notification_id: int) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE reminder_notifications
                SET sent = TRUE
                WHERE id = %s
                  AND sent = FALSE
                RETURNING id
                """,
                (notification_id,),
            )
            row = cur.fetchone()
        conn.commit()
    return bool(row)


def revert_notification_claim(notification_id: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE reminder_notifications
                SET sent = FALSE
                WHERE id = %s
                """,
                (notification_id,),
            )
        conn.commit()


def check_and_mark_event_completed(event_id: int) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS remaining
                FROM reminder_notifications
                WHERE event_id = %s
                  AND sent = FALSE
                """,
                (event_id,),
            )
            row = cur.fetchone()
            remaining = int(row_get(row, "remaining", 0, 0))

            if remaining == 0:
                cur.execute(
                    """
                    UPDATE reminder_events
                    SET completed_at = COALESCE(completed_at, NOW())
                    WHERE id = %s
                    """,
                    (event_id,),
                )
                conn.commit()
                return True

        conn.commit()
    return False


def cleanup_completed_event(event_id: int) -> None:
    logger.info("Event completed and waiting for daily cleanup: event_id=%s", event_id)


def cleanup_old_data() -> None:
    cutoff = now_local().replace(tzinfo=None) - timedelta(days=1)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT re.id
                FROM reminder_events re
                WHERE re.completed_at IS NOT NULL
                  AND re.completed_at < %s
                  AND NOT EXISTS (
                        SELECT 1
                        FROM reminder_notifications rn
                        WHERE rn.event_id = re.id
                          AND rn.sent = FALSE
                  )
                """,
                (cutoff,),
            )
            rows = cur.fetchall()

            if not rows:
                conn.commit()
                logger.info("Daily cleanup finished: no old completed reminder data to delete")
                return

            event_ids = [int(row_get(row, "id", 0)) for row in rows]

            cur.execute(
                "DELETE FROM reminder_notifications WHERE event_id = ANY(%