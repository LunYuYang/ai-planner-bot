import os
import re
import json
import html
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
    for entry in parsed.entries:
        title_raw = clean_html_text(entry.get("title", "")).strip()
        if not title_raw:
            continue

        items.append(
            {
                "title": title_raw,
                "title_norm": normalize_title(title_raw),
                "link": entry.get("link", "").strip(),
                "raw_summary": build_raw_summary(entry, extract_source_name(feed_title, entry)),
                "source": extract_source_name(feed_title, entry),
                "published_ts": parse_published_ts(entry),
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

    ordered_categories = sorted(selected_categories, key=lambda x: 0 if x == "tech" else 1)

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


def format_news_message(items: List[Dict[str, Any]], category: str = "all", include_summary: bool = True) -> str:
    now_str = datetime.now(TZINFO).strftime("%Y-%m-%d %H:%M")

    if category == "tech":
        title = "🧠 今日科技 / AI 新聞"
    elif category == "business":
        title = "💼 今日商業 / 財經新聞"
    else:
        title = "🗞️ 今日科技 / AI / 商業 / 財經新聞"

    if not items:
        return f"{title}\n更新時間：{now_str}\n\n目前抓不到新聞，請稍後再試。"

    lines = [title, f"更新時間：{now_str}", ""]

    for idx, item in enumerate(items, start=1):
        block = [f"{idx}. {item['title']}"]
        if include_summary:
            block.append(f"摘要：{item.get('summary') or item.get('raw_summary') or ''}")
        block.append(f"來源：{item['source']}")
        if item["link"]:
            block.append(item["link"])
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
        if arg1 in ("tech", "technology", "ai", "科技"):
            category = "tech"
        elif arg1 in ("business", "biz", "finance", "財經", "商業", "商務"):
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


def fetch_weather(city: str = DEFAULT_WEATHER_CITY) -> Optional[Dict[str, Any]]:
    if not CWA_API_KEY:
        logger.warning("CWA_API_KEY not set.")
        return None

    url = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/F-C0032-001"
    params = {"Authorization": CWA_API_KEY, "format": "JSON", "locationName": city}

    try:
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()

        locations = data.get("records", {}).get("location", [])
        if not locations:
            return None

        loc = locations[0]
        weather_elements = {item["elementName"]: item["time"] for item in loc.get("weatherElement", [])}

        wx_list = weather_elements.get("Wx", [])
        pop_list = weather_elements.get("PoP", [])
        min_list = weather_elements.get("MinT", [])
        max_list = weather_elements.get("MaxT", [])
        ci_list = weather_elements.get("CI", [])

        if not wx_list:
            return None

        return {
            "city": loc.get("locationName", city),
            "start": wx_list[0].get("startTime", ""),
            "end": wx_list[0].get("endTime", ""),
            "weather": wx_list[0].get("parameter", {}).get("parameterName", ""),
            "pop": pop_list[0].get("parameter", {}).get("parameterName", "") if pop_list else "",
            "min_temp": min_list[0].get("parameter", {}).get("parameterName", "") if min_list else "",
            "max_temp": max_list[0].get("parameter", {}).get("parameterName", "") if max_list else "",
            "comfort": ci_list[0].get("parameter", {}).get("parameterName", "") if ci_list else "",
        }
    except Exception as e:
        logger.exception("Weather fetch failed: %s", e)
        return None


def format_weather_message(weather: Optional[Dict[str, Any]]) -> str:
    if not weather:
        if not CWA_API_KEY:
            return "目前尚未設定 CWA_API_KEY，無法取得天氣。"
        return "天氣資料取得失敗，請稍後再試。"

    city = weather.get("city", DEFAULT_WEATHER_CITY)
    weather_text = weather.get("weather", "")
    pop = weather.get("pop", "")
    min_temp = weather.get("min_temp", "")
    max_temp = weather.get("max_temp", "")
    comfort = weather.get("comfort", "")
    start = weather.get("start", "")
    end = weather.get("end", "")

    tip = ""
    try:
        pop_value = int(pop)
        if pop_value >= 50:
            tip = "提醒：降雨機率偏高，建議帶傘。"
        elif pop_value >= 20:
            tip = "提醒：可能有局部降雨，外出可備傘。"
        else:
            tip = "提醒：降雨機率不高。"
    except Exception:
        pass

    lines = [
        f"🌤️ {city} 天氣預報",
        f"時段：{start} ~ {end}",
        f"天氣：{weather_text}",
        f"溫度：{min_temp} ~ {max_temp}°C",
        f"降雨機率：{pop}%",
    ]
    if comfort:
        lines.append(f"體感：{comfort}")
    if tip:
        lines.append(tip)
    return "\n".join(lines)


def handle_weather(chat_id: int, text: str = "") -> None:
    register_chat_id(chat_id)
    city = DEFAULT_WEATHER_CITY
    parts = text.strip().split(maxsplit=1)
    if len(parts) >= 2 and parts[1].strip():
        city = parts[1].strip()
    send_message(chat_id, format_weather_message(fetch_weather(city)))


def send_daily_weather() -> None:
    logger.info("Running scheduled daily weather push...")
    chat_ids = get_all_target_chat_ids()
    if not chat_ids:
        logger.warning("No chat ids found. Skip daily weather push.")
        return
    try:
        message = format_weather_message(fetch_weather(DEFAULT_WEATHER_CITY))
        for chat_id in chat_ids:
            try:
                send_message(chat_id, message)
                logger.info("Daily weather sent to %s", chat_id)
            except Exception as e:
                logger.exception("Failed to send daily weather to %s: %s", chat_id, e)
    except Exception as e:
        logger.exception("Daily weather job failed: %s", e)


ADVANCE_REMINDER_RULES = [
    ("1h", "- 1小時前", timedelta(hours=1)),
    ("event", "- 事件時間", timedelta(seconds=0)),
]

CHINESE_NUMBER_MAP = {
    "零": 0, "〇": 0, "○": 0, "Ｏ": 0,
    "一": 1, "二": 2, "兩": 2, "三": 3, "四": 4,
    "五": 5, "六": 6, "七": 7, "八": 8, "九": 9,
}


def normalize_keyword_for_event(message: str) -> str:
    text = re.sub(r"\s+", "", message.strip().lower())
    return text[:30] if text else "event"


def normalize_message_for_compare(message: str) -> str:
    text = message.strip().lower()
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[，。,\.！!？?、~～\-—_]+", "", text)
    return text


def chinese_numeral_to_int(text: str) -> Optional[int]:
    s = text.strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    if s == "十":
        return 10
    if "十" in s:
        parts = s.split("十")
        if len(parts) != 2:
            return None
        left, right = parts
        tens = 1 if left == "" else CHINESE_NUMBER_MAP.get(left)
        if tens is None:
            return None
        ones = 0 if right == "" else CHINESE_NUMBER_MAP.get(right)
        if ones is None:
            return None
        return tens * 10 + ones
    value = 0
    for ch in s:
        if ch not in CHINESE_NUMBER_MAP:
            return None
        value = value * 10 + CHINESE_NUMBER_MAP[ch]
    return value


def replace_chinese_number_in_match(match: re.Match) -> str:
    value = chinese_numeral_to_int(match.group(1))
    return match.group(0) if value is None else str(value)


def normalize_chinese_time_text(text: str) -> str:
    normalized = text.strip()
    normalized = re.sub(
        r"([零〇○Ｏ一二兩三四五六七八九十\d]+)(?=\s*(分鐘|分|min|mins|minute|minutes|小時|hr|hrs|hour|hours)\s*後)",
        replace_chinese_number_in_match,
        normalized,
        flags=re.IGNORECASE,
    )
    normalized = re.sub(r"([零〇○Ｏ一二兩三四五六七八九十\d]+)(?=\s*點)", replace_chinese_number_in_match, normalized)
    normalized = re.sub(r"([零〇○Ｏ一二兩三四五六七八九十\d]+)(?=\s*分)", replace_chinese_number_in_match, normalized)
    normalized = re.sub(r"(?<=[:：])\s*([零〇○Ｏ一二兩三四五六七八九十\d]+)", replace_chinese_number_in_match, normalized)
    return normalized


def parse_relative_reminder(text: str) -> Optional[Dict[str, Any]]:
    raw = normalize_chinese_time_text(text.strip())
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
    event_time = now + (timedelta(minutes=amount) if unit in ("分鐘", "分", "min", "mins", "minute", "minutes") else timedelta(hours=amount))
    return {"event_time": event_time, "message": msg.strip()}


def parse_absolute_reminder(text: str) -> Optional[Dict[str, Any]]:
    raw = normalize_chinese_time_text(text.strip())
    now = datetime.now(TZINFO)

    m = re.match(r"^\s*(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})\s+(.+?)\s*$", raw)
    if m:
        date_str, hour_str, minute_str, msg = m.groups()
        dt = datetime.strptime(f"{date_str} {hour_str}:{minute_str}", "%Y-%m-%d %H:%M").replace(tzinfo=TZINFO)
        if dt <= now:
            return None
        return {"event_time": dt, "message": msg.strip()}

    m = re.match(
        r"^\s*(今天|明天|昨天)?\s*(早上|上午|中午|下午|晚上)?\s*(\d{1,2})(?:(?:\s*[:：]\s*(\d{1,2}))|(?:\s*點\s*(半|(\d{1,2}))?))?\s*(?:分)?\s*(提醒我)?\s*(.+?)\s*$",
        raw
    )
    if not m:
        return None

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
        dt = datetime(base_date.year, base_date.month, base_date.day, hour, minute, tzinfo=TZINFO)
    except ValueError:
        return None

    if dt <= now:
        return None

    return {"event_time": dt, "message": msg.strip()}


def parse_chinese_reminder(text: str) -> Optional[Dict[str, Any]]:
    return parse_relative_reminder(text) or parse_absolute_reminder(text)


def get_notification_ids_by_event(event_id: int) -> List[int]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM reminder_notifications
                WHERE event_id = %s AND sent = 0
                """,
                (event_id,)
            )
            return [int(row["id"]) for row in cur.fetchall()]
    finally:
        conn.close()


def is_notification_active(notification_id: int) -> bool:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT rn.sent, rn.canceled, re.canceled AS event_canceled
                FROM reminder_notifications rn
                JOIN reminder_events re ON rn.event_id = re.id
                WHERE rn.id = %s
                """,
                (notification_id,)
            )
            row = cur.fetchone()
            if not row:
                return False
            return int(row["sent"]) == 0 and int(row["canceled"]) == 0 and int(row["event_canceled"]) == 0
    finally:
        conn.close()


def find_duplicate_event(chat_id: int, event_time: datetime, message: str) -> Optional[Dict[str, Any]]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, event_time, message, keyword
                FROM reminder_events
                WHERE chat_id = %s
                  AND event_time = %s
                  AND canceled = 0
                ORDER BY id DESC
                """,
                (chat_id, event_time)
            )
            rows = cur.fetchall()

        target_message = normalize_message_for_compare(message)
        for row in rows:
            if normalize_message_for_compare(row["message"]) == target_message:
                return row
        return None
    finally:
        conn.close()


def save_event_with_notifications(chat_id: int, event_time: datetime, message: str) -> Dict[str, Any]:
    conn = get_conn()
    try:
        keyword = normalize_keyword_for_event(message)
        now_dt = datetime.now(TZINFO)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO reminder_events (chat_id, event_time, message, keyword, canceled, created_at)
                VALUES (%s, %s, %s, %s, 0, %s)
                RETURNING id
                """,
                (chat_id, event_time, message, keyword, now_dt)
            )
            event_id = int(cur.fetchone()["id"])

            notifications = []
            for notify_type, label, delta in ADVANCE_REMINDER_RULES:
                notify_time = event_time - delta if delta.total_seconds() > 0 else event_time
                if notify_time <= datetime.now(TZINFO) and notify_type != "event":
                    continue

                cur.execute(
                    """
                    INSERT INTO reminder_notifications
                    (event_id, chat_id, notify_time, notify_type, label, sent, canceled, created_at)
                    VALUES (%s, %s, %s, %s, %s, 0, 0, %s)
                    RETURNING id
                    """,
                    (event_id, chat_id, notify_time, notify_type, label, now_dt)
                )
                notification_id = int(cur.fetchone()["id"])
                notifications.append(
                    {
                        "notification_id": notification_id,
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


def get_pending_notifications() -> List[Dict[str, Any]]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
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


def get_due_unsent_notifications() -> List[Dict[str, Any]]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
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
                WHERE rn.sent = 0
                  AND rn.canceled = 0
                  AND re.canceled = 0
                  AND rn.notify_time <= %s
                ORDER BY rn.notify_time ASC
                """,
                (datetime.now(TZINFO),)
            )
            return cur.fetchall()
    finally:
        conn.close()


def get_user_pending_events(chat_id: int) -> List[Dict[str, Any]]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT
                    re.id,
                    re.event_time,
                    re.message,
                    re.keyword
                FROM reminder_events re
                JOIN reminder_notifications rn
                  ON rn.event_id = re.id
                WHERE re.chat_id = %s
                  AND re.canceled = 0
                  AND rn.canceled = 0
                  AND rn.sent = 0
                ORDER BY re.event_time ASC
                """,
                (chat_id,)
            )
            return cur.fetchall()
    finally:
        conn.close()


def event_has_pending_notifications(event_id: int) -> bool:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM reminder_notifications
                WHERE event_id = %s
                  AND canceled = 0
                  AND sent = 0
                LIMIT 1
                """,
                (event_id,)
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def cleanup_completed_event(event_id: int) -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM reminder_notifications
                WHERE event_id = %s
                  AND canceled = 0
                  AND sent = 0
                LIMIT 1
                """,
                (event_id,)
            )
            still_pending = cur.fetchone() is not None
            if still_pending:
                conn.commit()
                return

            cur.execute(
                "DELETE FROM reminder_notifications WHERE event_id = %s",
                (event_id,)
            )
            cur.execute(
                "DELETE FROM reminder_events WHERE id = %s",
                (event_id,)
            )

        conn.commit()
        logger.info("Cleaned up completed event: event_id=%s", event_id)
    finally:
        conn.close()


def cleanup_all_completed_events() -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT re.id
                FROM reminder_events re
                LEFT JOIN reminder_notifications rn
                  ON rn.event_id = re.id
                 AND rn.canceled = 0
                 AND rn.sent = 0
                WHERE rn.id IS NULL
                """
            )
            rows = cur.fetchall()

        for row in rows:
            cleanup_completed_event(int(row["id"]))
    finally:
        conn.close()


def mark_notification_sent(notification_id: int) -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE reminder_notifications SET sent = 1 WHERE id = %s",
                (notification_id,)
            )
        conn.commit()
    finally:
        conn.close()


def cancel_event_by_id(event_id: int, chat_id: int) -> bool:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE reminder_events
                SET canceled = 1
                WHERE id = %s AND chat_id = %s AND canceled = 0
                """,
                (event_id, chat_id)
            )
            if cur.rowcount <= 0:
                conn.commit()
                return False

            cur.execute(
                """
                UPDATE reminder_notifications
                SET canceled = 1
                WHERE event_id = %s
                """,
                (event_id,)
            )

        conn.commit()
        cleanup_completed_event(event_id)
        return True
    finally:
        conn.close()


def find_latest_event_by_keyword(chat_id: int, keyword: str) -> Optional[Dict[str, Any]]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, event_time, message, keyword
                FROM reminder_events
                WHERE chat_id = %s
                  AND canceled = 0
                  AND (
                        lower(message) LIKE %s
                        OR lower(keyword) LIKE %s
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
    if label == "- 1小時前":
        return f"⏰ 提醒通知\n還有1小時：{event_time.strftime('%Y-%m-%d %H:%M')}｜{message}"
    return f"⏰ 提醒通知\n現在時間到：{event_time.strftime('%Y-%m-%d %H:%M')}｜{message}"


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
            if not is_notification_active(notification_id):
                logger.info("Skip inactive notification id=%s", notification_id)
                return
            send_message(chat_id, build_notification_text(label, event_time, message, event_id))
            mark_notification_sent(notification_id)
            cleanup_completed_event(event_id)
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


def remove_scheduled_jobs_for_event(event_id: int) -> None:
    for notification_id in get_notification_ids_by_event(event_id):
        try:
            scheduler.remove_job(notification_job_id(notification_id))
        except JobLookupError:
            pass
        except Exception as e:
            logger.exception("Failed removing notification job id=%s: %s", notification_id, e)


def catch_up_missed_notifications() -> None:
    try:
        rows = get_due_unsent_notifications()
        if not rows:
            cleanup_all_completed_events()
            return

        logger.info("Catch-up scan found %s due unsent notifications", len(rows))

        for row in rows:
            notification_id = int(row["id"])
            event_id = int(row["event_id"])
            chat_id = int(row["chat_id"])

            if not is_notification_active(notification_id):
                logger.info("Skip inactive catch-up notification id=%s", notification_id)
                continue

            event_time = parse_db_datetime(row["event_time"])
            label = row["label"]
            message = row["message"]

            send_message(chat_id, build_notification_text(label, event_time, message, event_id))
            mark_notification_sent(notification_id)
            cleanup_completed_event(event_id)

            try:
                scheduler.remove_job(notification_job_id(notification_id))
            except JobLookupError:
                pass
            except Exception as e:
                logger.exception("Failed removing catch-up job id=%s: %s", notification_id, e)

            logger.info("Catch-up notification sent: id=%s event_id=%s", notification_id, event_id)

        cleanup_all_completed_events()
    except Exception as e:
        logger.exception("Catch-up missed notifications failed: %s", e)


def load_pending_notifications_into_scheduler() -> None:
    rows = get_pending_notifications()
    now = datetime.now(TZINFO)

    for row in rows:
        notify_time = parse_db_datetime(row["notify_time"])
        if notify_time <= now:
            continue

        schedule_one_notification(
            notification_id=int(row["id"]),
            event_id=int(row["event_id"]),
            chat_id=int(row["chat_id"]),
            notify_time=notify_time,
            label=row["label"],
            event_time=parse_db_datetime(row["event_time"]),
            message=row["message"],
        )


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
        message = format_news_message(items, category=DEFAULT_NEWS_CATEGORY, include_summary=True)
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
        "✅ Bot 已啟用\n\n"
        "可用功能：\n"
        "/news\n/news tech\n/news business\n/weather\n/list\n/cancel 事件代碼\n/help\n\n"
        "提醒可直接輸入：\n"
        "晚上7點半打球\n今天早上七點吃早餐\n明天晚上七點半打球\n"
        "2026-03-27 14:30 開會\n30分鐘後提醒我喝水\n兩小時後提醒我洗衣服"
    )
    send_message(chat_id, msg)


def handle_help(chat_id: int) -> None:
    msg = (
        "指令說明\n\n"
        "/start\n/help\n/news\n/news tech\n/news business\n/weather\n/weather 臺北市\n/list\n/cancel 事件代碼\n\n"
        "提醒輸入範例：\n"
        "晚上7點半打球\n今天早上七點吃早餐\n明天晚上七點半打球\n30分鐘後提醒我喝水\n兩小時後提醒我洗衣服\n\n"
        "取消範例：\n/cancel 12\n取消打球"
    )
    send_message(chat_id, msg)


def handle_news(chat_id: int, text: str) -> None:
    register_chat_id(chat_id)
    args = parse_news_command(text)
    items = fetch_news(category=args["category"], limit=args["limit"])
    if ENABLE_CHINESE_SUMMARY:
        items = enrich_news_with_chinese_summary(items)
    send_message(chat_id, format_news_message(items, category=args["category"], include_summary=True))


def handle_list(chat_id: int) -> None:
    rows = get_user_pending_events(chat_id)
    if not rows:
        send_message(chat_id, "目前沒有未取消事件提醒。")
        return

    lines = ["📌 目前所有未取消提醒", ""]
    for row in rows[:50]:
        event_time = parse_db_datetime(row["event_time"])
        lines.append(f"事件代碼：{row['id']}\n{event_time.strftime('%Y-%m-%d %H:%M')}｜{row['message']}")
        lines.append("")
    send_message(chat_id, "\n".join(lines).strip())


def handle_cancel(chat_id: int, text: str) -> None:
    m = re.match(r"^/cancel\s+(\d+)\s*$", text.strip())
    if not m:
        send_message(chat_id, "用法：/cancel 事件代碼")
        return

    event_id = int(m.group(1))
    remove_scheduled_jobs_for_event(event_id)
    ok = cancel_event_by_id(event_id, chat_id)
    if not ok:
        send_message(chat_id, f"找不到可取消的事件代碼 #{event_id}")
        return
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
        send_message(chat_id, f"找不到符合「{keyword}」的未取消事件。")
        return True

    event_id = int(row["id"])
    event_time = parse_db_datetime(row["event_time"])

    remove_scheduled_jobs_for_event(event_id)
    ok = cancel_event_by_id(event_id, chat_id)
    if not ok:
        send_message(chat_id, "取消失敗，請稍後再試。")
        return True

    send_message(chat_id, f"✅ 已取消提醒\n{event_time.strftime('%Y-%m-%d %H:%M')}｜{row['message']}")
    return True


def try_handle_event_reminder(chat_id: int, text: str) -> bool:
    parsed = parse_chinese_reminder(text)
    if not parsed:
        return False

    event_time = parsed["event_time"]
    message = parsed["message"]

    replaced = False
    duplicate = find_duplicate_event(chat_id, event_time, message)
    if duplicate:
        old_event_id = int(duplicate["id"])
        remove_scheduled_jobs_for_event(old_event_id)
        if cancel_event_by_id(old_event_id, chat_id):
            replaced = True

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

    lines = ["✅ 已更新提醒" if replaced else "✅ 已建立提醒"]
    if replaced:
        lines.append("已覆蓋先前相同提醒")
    lines.append(f"{event_time.strftime('%Y-%m-%d %H:%M')}｜{message}")
    lines.append("提醒時間：前1小時、事件當下")
    send_message(chat_id, "\n".join(lines))
    return True


def handle_unknown(chat_id: int) -> None:
    msg = (
        "我目前支援：\n"
        "/start\n/help\n/news\n/news tech\n/news business\n/weather\n/list\n/cancel 事件代碼\n\n"
        "也可以直接輸入：\nnews\nnew\nweather\n天氣\n\n"
        "也可以直接輸入提醒，例如：\n"
        "晚上7點半打球\n今天早上七點吃早餐\n明天晚上七點半打球\n兩小時後提醒我喝水\n\n"
        "取消也可直接輸入：\n取消打球"
    )
    send_message(chat_id, msg)


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

    if WEATHER_PUSH_TIME:
        try:
            scheduler.remove_job("daily_weather_job")
        except JobLookupError:
            pass
        except Exception as e:
            logger.exception("Failed removing old daily_weather_job: %s", e)

        try:
            w_hour, w_minute = WEATHER_PUSH_TIME.split(":")
            scheduler.add_job(
                send_daily_weather,
                trigger="cron",
                hour=int(w_hour),
                minute=int(w_minute),
                id="daily_weather_job",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=3600,
            )
            logger.info("Daily weather scheduled at %s (%s)", WEATHER_PUSH_TIME, TIMEZONE)
        except Exception as e:
            logger.exception("Failed scheduling daily weather: %s", e)

    try:
        scheduler.remove_job("catch_up_notifications_job")
    except JobLookupError:
        pass
    except Exception as e:
        logger.exception("Failed removing old catch_up_notifications_job: %s", e)

    scheduler.add_job(
        catch_up_missed_notifications,
        trigger="interval",
        minutes=1,
        id="catch_up_notifications_job",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )

    logger.info("Scheduler started. Daily news at %s (%s)", NEWS_PUSH_TIME, TIMEZONE)
    logger.info("Catch-up scan scheduled every 1 minute")


@app.route("/", methods=["GET"])
def home():
    return jsonify(
        {
            "ok": True,
            "service": "telegram-bot-private-news-reminder",
            "timezone": TIMEZONE,
            "news_push_time": NEWS_PUSH_TIME,
            "weather_push_time": WEATHER_PUSH_TIME,
            "owner_id_set": bool(OWNER_ID),
            "openai_model": OPENAI_MODEL,
            "chinese_summary_enabled": ENABLE_CHINESE_SUMMARY,
            "weather_city": DEFAULT_WEATHER_CITY,
            "weather_enabled": bool(CWA_API_KEY),
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
        lowered = text.lower().strip()

        if text.startswith("/start"):
            handle_start(chat_id)
        elif text.startswith("/help"):
            handle_help(chat_id)
        elif text.startswith("/news") or lowered in ("news", "new"):
            handle_news(chat_id, text if text.startswith("/news") else "/news")
        elif text.startswith("/weather") or lowered in ("weather", "天氣"):
            handle_weather(chat_id, text if text.startswith("/weather") else "/weather")
        elif text.startswith("/list"):
            handle_list(chat_id)
        elif text.startswith("/cancel"):
            handle_cancel(chat_id, text)
        else:
            if handle_cancel_by_keyword(chat_id, text):
                return jsonify({"ok": True})
            if not try_handle_event_reminder(chat_id, text):
                handle_unknown(chat_id)

        return jsonify({"ok": True})

    except Exception as e:
        logger.exception("Webhook handler error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


def bootstrap() -> None:
    init_db()

    try:
        set_webhook()
    except Exception as e:
        logger.exception("set_webhook failed: %s", e)

    try:
        schedule_jobs()
        load_pending_notifications_into_scheduler()
        catch_up_missed_notifications()
        cleanup_all_completed_events()
    except Exception as e:
        logger.exception("scheduler bootstrap failed: %s", e)


bootstrap()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
