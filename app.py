import os
import re
import json
import html
import logging
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import quote

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
DAILY_CLEANUP_TIME = os.getenv("DAILY_CLEANUP_TIME", "03:30").strip()
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()

PENDING_FOOD_FILE = os.path.join(DATA_DIR, "pending_food_requests.json")

WEATHER_CITY_ALIASES = {
    "臺北市": ["臺北市", "台北市", "臺北", "台北", "taipei", "taipei city"],
    "新北市": ["新北市", "新北", "newtaipei", "new taipei", "newtaipeicity", "new taipei city"],
    "桃園市": ["桃園市", "桃園", "taoyuan", "taoyuan city"],
    "臺中市": ["臺中市", "台中市", "臺中", "台中", "taichung", "taichung city"],
    "臺南市": ["臺南市", "台南市", "臺南", "台南", "tainan", "tainan city"],
    "高雄市": ["高雄市", "高雄", "kaohsiung", "kaohsiung city"],
    "基隆市": ["基隆市", "基隆", "keelung", "keelung city"],
    "新竹市": ["新竹市", "新竹", "hsinchu", "hsinchu city"],
    "嘉義市": ["嘉義市", "嘉義", "chiayi", "chiayi city"],
    "新竹縣": ["新竹縣", "hsinchu county"],
    "苗栗縣": ["苗栗縣", "苗栗", "miaoli", "miaoli county"],
    "彰化縣": ["彰化縣", "彰化", "changhua", "changhua county"],
    "南投縣": ["南投縣", "南投", "nantou", "nantou county"],
    "雲林縣": ["雲林縣", "雲林", "yunlin", "yunlin county"],
    "嘉義縣": ["嘉義縣", "chiayi county"],
    "屏東縣": ["屏東縣", "屏東", "pingtung", "pingtung county"],
    "宜蘭縣": ["宜蘭縣", "宜蘭", "yilan", "yilan county"],
    "花蓮縣": ["花蓮縣", "花蓮", "hualien", "hualien county"],
    "臺東縣": ["臺東縣", "台東縣", "臺東", "台東", "taitung", "taitung county"],
    "澎湖縣": ["澎湖縣", "澎湖", "penghu", "penghu county"],
    "金門縣": ["金門縣", "金門", "kinmen", "kinmen county"],
    "連江縣": ["連江縣", "連江", "馬祖", "matsu", "lienchiang", "lienchiang county"],
}

WEATHER_CITY_COORDS = {
    "臺北市": (25.0375, 121.5637),
    "新北市": (25.0129, 121.4657),
    "桃園市": (24.9937, 121.3010),
    "臺中市": (24.1477, 120.6736),
    "臺南市": (22.9999, 120.2269),
    "高雄市": (22.6273, 120.3014),
    "基隆市": (25.1276, 121.7392),
    "新竹市": (24.8138, 120.9675),
    "嘉義市": (23.4801, 120.4491),
    "新竹縣": (24.8387, 121.0177),
    "苗栗縣": (24.5602, 120.8214),
    "彰化縣": (24.0800, 120.5389),
    "南投縣": (23.9609, 120.9719),
    "雲林縣": (23.7092, 120.4313),
    "嘉義縣": (23.4518, 120.2555),
    "屏東縣": (22.5519, 120.5488),
    "宜蘭縣": (24.7021, 121.7378),
    "花蓮縣": (23.9872, 121.6015),
    "臺東縣": (22.7583, 121.1444),
    "澎湖縣": (23.5710, 119.5797),
    "金門縣": (24.4368, 118.3186),
    "連江縣": (26.1602, 119.9517),
}

WEATHER_QUERY_TOKENS = ("天氣", "weather", "forecast", "氣溫", "溫度")
WEATHER_WEEKDAY_MAP = {"一": 0, "二": 1, "三": 2, "四": 3, "五": 4, "六": 5, "日": 6, "天": 6, "七": 6}

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


AI_ROUTER_ENABLED = os.getenv("AI_ROUTER_ENABLED", "true").strip().lower() == "true"

AI_ROUTER_SYSTEM_PROMPT = """
你是一個 Telegram 個人助理機器人的意圖路由器。
你的工作不是直接回答問題，而是把使用者輸入解析成固定 JSON。

支援的 intent：
- food
- weather
- news
- reminder
- unknown

支援的 action：
- search
- create
- cancel
- list
- summary
- unknown

請務必：
1. 僅輸出 JSON
2. 不要加 markdown code block
3. 沒有把握時，intent 用 unknown
4. reminder 的 time_text 保留使用者原話，不要自行改寫成你猜的時間
5. food 若沒有明確地點 location 就填 null，requires_location=true
6. weather 若沒提地點 location 可填 null
7. news topic 可用：ai / tech / business / semiconductor / crypto / all
8. food meal_type 可用：breakfast / lunch / dinner / late_night / snack / fine_dining / generic
9. reply_style 可用：short / normal

JSON schema:
{
  "intent": "food|weather|news|reminder|unknown",
  "action": "search|create|cancel|list|summary|unknown",
  "entities": {
    "meal_type": null,
    "price_min": null,
    "price_max": null,
    "radius_km": null,
    "location": null,
    "requires_location": null,
    "date_text": null,
    "topic": null,
    "time_range": null,
    "format": null,
    "time_text": null,
    "message": null,
    "operation": null
  },
  "reply_style": "normal"
}
""".strip()

AI_ROUTER_DEFAULT = {
    "intent": "unknown",
    "action": "unknown",
    "entities": {
        "meal_type": None,
        "price_min": None,
        "price_max": None,
        "radius_km": None,
        "location": None,
        "requires_location": None,
        "date_text": None,
        "topic": None,
        "time_range": None,
        "format": None,
        "time_text": None,
        "message": None,
        "operation": None,
    },
    "reply_style": "normal",
}


def ai_router_default() -> Dict[str, Any]:
    return json.loads(json.dumps(AI_ROUTER_DEFAULT))


def safe_json_loads(text: str) -> Optional[Dict[str, Any]]:
    text = (text or "").strip()
    if not text:
        return None
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text).strip()
        text = re.sub(r"```$", "", text).strip()
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def merge_ai_router_default(obj: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged = ai_router_default()
    if not obj:
        return merged

    merged["intent"] = str(obj.get("intent") or merged["intent"])
    merged["action"] = str(obj.get("action") or merged["action"])
    merged["reply_style"] = str(obj.get("reply_style") or merged["reply_style"])

    entities = obj.get("entities") or {}
    if isinstance(entities, dict):
        for key in merged["entities"].keys():
            if key in entities:
                merged["entities"][key] = entities[key]

    return merged


def heuristic_intent_router(text: str) -> Dict[str, Any]:
    raw = (text or "").strip()
    normalized = normalize_food_text(raw) if raw else ""

    # news
    if any(token in normalized for token in ["新聞", "news", "ai新聞", "科技新聞", "財經新聞", "商業新聞"]):
        topic = "all"
        lowered = raw.lower()
        if "ai" in lowered or "人工智慧" in raw or "科技" in raw:
            topic = "ai"
        elif "財經" in raw or "商業" in raw or "business" in lowered or "finance" in lowered:
            topic = "business"
        elif "半導體" in raw:
            topic = "semiconductor"
        elif "加密" in raw or "幣圈" in raw or "crypto" in lowered:
            topic = "crypto"
        return merge_ai_router_default({
            "intent": "news",
            "action": "summary",
            "entities": {"topic": topic, "time_range": "today", "format": "short"},
        })

    # reminder list/cancel
    if "提醒" in raw:
        if any(token in raw for token in ["有哪些提醒", "目前提醒", "查看提醒", "列出提醒", "提醒列表"]):
            return merge_ai_router_default({
                "intent": "reminder",
                "action": "list",
                "entities": {"operation": "list"},
            })
        if raw.startswith("取消") or "取消提醒" in raw:
            msg = re.sub(r"^\s*取消\s*", "", raw).strip() or None
            return merge_ai_router_default({
                "intent": "reminder",
                "action": "cancel",
                "entities": {"operation": "cancel", "message": msg},
            })
        # 例如 提醒我20:00要離開學校 / 提醒我 明天晚上七點 打球
        m = re.match(r"^\s*提醒我\s*(.+?)\s*要\s*(.+?)\s*$", raw)
        if m:
            return merge_ai_router_default({
                "intent": "reminder",
                "action": "create",
                "entities": {"operation": "create", "time_text": m.group(1).strip(), "message": m.group(2).strip()},
            })
        m = re.match(r"^\s*提醒我\s*(.+?)\s+(.+?)\s*$", raw)
        if m:
            return merge_ai_router_default({
                "intent": "reminder",
                "action": "create",
                "entities": {"operation": "create", "time_text": m.group(1).strip(), "message": m.group(2).strip()},
            })

    return ai_router_default()


def parse_user_intent_with_gpt(text: str, memory: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not AI_ROUTER_ENABLED:
        return ai_router_default()

    heuristic = heuristic_intent_router(text)
    if heuristic.get("intent") != "unknown":
        return heuristic

    if not client:
        return ai_router_default()

    memory_text = json.dumps(memory or {}, ensure_ascii=False)
    user_prompt = (
        f"使用者輸入：{text}\n"
        f"上下文記憶：{memory_text}\n"
        "請只輸出 JSON。"
    )

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": AI_ROUTER_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
            max_tokens=300,
            response_format={"type": "json_object"},
        )
        content = (resp.choices[0].message.content or "").strip()
        parsed = safe_json_loads(content)
        return merge_ai_router_default(parsed)
    except Exception as e:
        logger.exception("AI router parse failed: %s", e)
        return ai_router_default()


def build_food_text_from_entities(entities: Dict[str, Any]) -> str:
    parts: List[str] = []
    meal_map = {
        "breakfast": "早餐",
        "lunch": "午餐",
        "dinner": "晚餐",
        "late_night": "宵夜",
        "snack": "小吃",
        "fine_dining": "高級餐廳",
        "generic": "美食",
    }
    meal_type = entities.get("meal_type")
    parts.append(meal_map.get(meal_type, "美食"))

    price_min = entities.get("price_min")
    price_max = entities.get("price_max")
    if price_min is not None and price_max is not None:
        parts.append(f"{price_min}-{price_max}")
    elif price_min is not None:
        parts.append(f"{price_min}以上")
    elif price_max is not None:
        parts.append(f"{price_max}以下")

    location = entities.get("location")
    if location:
        parts.insert(0, str(location))

    radius_km = entities.get("radius_km")
    if radius_km:
        parts.append(f"{radius_km}公里")

    return " ".join(str(x) for x in parts if x)


def build_weather_text_from_entities(entities: Dict[str, Any]) -> str:
    location = entities.get("location")
    date_text = entities.get("date_text")
    if location and date_text:
        return f"{location}{date_text}天氣"
    if location:
        return f"{location}天氣"
    if date_text:
        return f"{date_text}天氣"
    return "天氣"


def build_news_command_from_entities(entities: Dict[str, Any]) -> str:
    topic = str(entities.get("topic") or "all").lower()
    topic_map = {
        "ai": "tech",
        "tech": "tech",
        "business": "business",
        "semiconductor": "tech",
        "crypto": "business",
        "all": "all",
    }
    mapped = topic_map.get(topic, "all")
    return "/news" if mapped == "all" else f"/news {mapped}"


def build_reminder_text_from_entities(entities: Dict[str, Any]) -> str:
    operation = str(entities.get("operation") or "create").lower()
    if operation == "list":
        return "/list"
    if operation == "cancel":
        message = str(entities.get("message") or "").strip()
        return f"取消 {message}" if message else "/cancel_all"

    time_text = str(entities.get("time_text") or "").strip()
    message = str(entities.get("message") or "").strip()
    if time_text and message:
        return f"{time_text} {message}"
    return ""


def route_message_with_ai(text: str, memory: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    parsed = parse_user_intent_with_gpt(text=text, memory=memory)
    entities = parsed.get("entities") or {}
    dispatch = {
        "intent": parsed.get("intent", "unknown"),
        "action": parsed.get("action", "unknown"),
        "reply_style": parsed.get("reply_style", "normal"),
        "entities": entities,
        "forward_text": None,
    }

    intent = dispatch["intent"]
    if intent == "food":
        dispatch["forward_text"] = build_food_text_from_entities(entities)
    elif intent == "weather":
        dispatch["forward_text"] = build_weather_text_from_entities(entities)
    elif intent == "news":
        dispatch["forward_text"] = build_news_command_from_entities(entities)
    elif intent == "reminder":
        dispatch["forward_text"] = build_reminder_text_from_entities(entities)

    return dispatch


def handle_ai_router(chat_id: int, text: str) -> bool:
    dispatch = route_message_with_ai(text)
    logger.info("AI router result: %s", dispatch)
    intent = dispatch.get("intent")
    forward_text = (dispatch.get("forward_text") or "").strip()
    entities = dispatch.get("entities") or {}

    if intent == "food" and forward_text:
        handle_food(chat_id, forward_text)
        return True

    if intent == "weather" and forward_text:
        handle_weather(chat_id, forward_text)
        return True

    if intent == "news" and forward_text:
        handle_news(chat_id, forward_text)
        return True

    if intent == "reminder":
        operation = str(entities.get("operation") or dispatch.get("action") or "create").lower()
        if operation == "list":
            handle_list(chat_id)
            return True
        if operation == "cancel":
            if forward_text == "/cancel_all":
                handle_cancel_all(chat_id)
                return True
            if forward_text and handle_cancel_by_keyword(chat_id, forward_text):
                return True
            return False
        if forward_text and try_handle_event_reminder(chat_id, forward_text):
            return True

    return False



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




def normalize_weather_text(text: str) -> str:
    text = (text or "").strip().lower()
    text = text.replace("　", " ")
    text = text.replace("臺", "台")
    text = re.sub(r"\s+", "", text)
    return text


def resolve_weather_city(text: str) -> Optional[str]:
    normalized = normalize_weather_text(text)
    if not normalized:
        return None

    for canonical, aliases in WEATHER_CITY_ALIASES.items():
        normalized_aliases = {normalize_weather_text(alias) for alias in aliases}
        if normalized in normalized_aliases:
            return canonical

    for canonical, aliases in WEATHER_CITY_ALIASES.items():
        normalized_aliases = sorted(
            {normalize_weather_text(alias) for alias in aliases},
            key=len,
            reverse=True,
        )
        for alias in normalized_aliases:
            if alias and alias in normalized:
                return canonical

    return None


def is_weather_query(text: str) -> bool:
    raw = (text or "").strip()
    if raw.startswith("/weather"):
        return True

    normalized = normalize_weather_text(raw)
    if normalized in ("weather", "天氣"):
        return True

    return any(token in normalized for token in WEATHER_QUERY_TOKENS)


def extract_weather_city(text: str) -> str:
    raw = (text or "").strip()
    return resolve_weather_city(raw) or DEFAULT_WEATHER_CITY


def normalize_food_text(text: str) -> str:
    text = (text or "").strip().lower()
    text = text.replace("　", " ")
    text = text.replace("臺", "台")
    text = re.sub(r"\s+", "", text)
    return text


FOOD_KEYWORDS = {
    "breakfast": ["早餐", "早午餐", "brunch", "breakfast", "morning food"],
    "lunch": ["午餐", "中餐", "午飯", "lunch"],
    "dinner": ["晚餐", "dinner", "supper"],
    "late_night": ["宵夜", "消夜", "late night", "midnight food", "midnight snack"],
    "snack": ["小吃", "麵店", "面店", "noodle", "snack"],
    "fine_dining": ["高級餐廳", "高級料理", "fine dining", "restaurant", "約會餐廳"],
    "generic": ["美食", "餐廳", "吃飯", "food", "eat", "dining"],
}

FOOD_TRIGGER_PATTERNS = [kw for kws in FOOD_KEYWORDS.values() for kw in kws] + [
    "附近美食", "附近早餐", "附近午餐", "附近晚餐", "附近宵夜", "nearby food", "nearby breakfast",
    "nearby lunch", "nearby dinner", "nearby late night"
]

DEFAULT_FOOD_RADIUS_METERS = 3000
DEFAULT_FOOD_LIMIT = 5
DEFAULT_FOOD_MIN_RATING = 4.5
DEFAULT_FOOD_MAX_RATING = 4.8
DEFAULT_FOOD_MIN_REVIEWS = 50


def load_pending_food_requests() -> Dict[str, Any]:
    if not os.path.exists(PENDING_FOOD_FILE):
        return {}
    try:
        return json.loads(Path(PENDING_FOOD_FILE).read_text(encoding="utf-8"))
    except Exception as e:
        logger.exception("Failed to load pending food requests: %s", e)
        return {}


def save_pending_food_requests(data: Dict[str, Any]) -> None:
    try:
        Path(PENDING_FOOD_FILE).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.exception("Failed to save pending food requests: %s", e)


def set_pending_food_request(chat_id: int, payload: Dict[str, Any]) -> None:
    data = load_pending_food_requests()
    data[str(chat_id)] = payload
    save_pending_food_requests(data)


def get_pending_food_request(chat_id: int) -> Optional[Dict[str, Any]]:
    return load_pending_food_requests().get(str(chat_id))


def clear_pending_food_request(chat_id: int) -> None:
    data = load_pending_food_requests()
    if str(chat_id) in data:
        data.pop(str(chat_id), None)
        save_pending_food_requests(data)


def is_food_query(text: str) -> bool:
    normalized = normalize_food_text(text)
    if not normalized:
        return False
    if normalized.startswith("/food"):
        return True
    return any(normalize_food_text(token) in normalized for token in FOOD_TRIGGER_PATTERNS)


def detect_food_mode(text: str) -> str:
    normalized = normalize_food_text(text)
    priority = ["breakfast", "lunch", "dinner", "late_night", "snack", "fine_dining", "generic"]
    for mode in priority:
        for kw in FOOD_KEYWORDS[mode]:
            if normalize_food_text(kw) in normalized:
                return mode
    return "generic"


def parse_distance_meters(text: str) -> int:
    m = re.search(r"(\d+(?:\.\d+)?)\s*(公里|km|KM)", text or "", re.IGNORECASE)
    if not m:
        return DEFAULT_FOOD_RADIUS_METERS
    km = float(m.group(1))
    km = max(0.5, min(20.0, km))
    return int(km * 1000)


def parse_budget_twd(text: str) -> Dict[str, Optional[int]]:
    raw = (text or "").replace(",", "")
    m = re.search(r"(\d{2,5})\s*[~～\-到至]\s*(\d{2,5})", raw)
    if m:
        low, high = int(m.group(1)), int(m.group(2))
        if low > high:
            low, high = high, low
        return {"min_twd": low, "max_twd": high}

    m = re.search(r"(\d{2,5})\s*(以下|以內|內|under)", raw, re.IGNORECASE)
    if m:
        return {"min_twd": None, "max_twd": int(m.group(1))}

    m = re.search(r"(\d{2,5})\s*(以上|起|up)", raw, re.IGNORECASE)
    if m:
        return {"min_twd": int(m.group(1)), "max_twd": None}

    return {"min_twd": None, "max_twd": None}


def twd_to_google_price_level(value: Optional[int], is_max: bool = False) -> Optional[int]:
    if value is None:
        return None
    if value <= 150:
        return 0
    if value <= 400:
        return 1
    if value <= 900:
        return 2
    if value <= 1800:
        return 3
    return 4


def parse_price_levels(text: str) -> Dict[str, Optional[int]]:
    budget = parse_budget_twd(text)
    return {
        "minprice": twd_to_google_price_level(budget.get("min_twd"), is_max=False),
        "maxprice": twd_to_google_price_level(budget.get("max_twd"), is_max=True),
        "budget": budget,
    }


def detect_explicit_location(text: str) -> Tuple[Optional[str], Optional[Tuple[float, float]]]:
    canonical = resolve_weather_city(text)
    if canonical and canonical in WEATHER_CITY_COORDS:
        return canonical, WEATHER_CITY_COORDS[canonical]
    return None, None


def parse_food_query(text: str) -> Dict[str, Any]:
    mode = detect_food_mode(text)
    radius_meters = parse_distance_meters(text)
    price_info = parse_price_levels(text)
    explicit_city, coords = detect_explicit_location(text)

    if explicit_city and coords:
        requires_location = False
        location_label = explicit_city
    else:
        requires_location = True
        location_label = None

    return {
        "mode": mode,
        "radius_meters": radius_meters,
        "minprice": price_info["minprice"],
        "maxprice": price_info["maxprice"],
        "budget": price_info["budget"],
        "explicit_city": explicit_city,
        "coords": coords,
        "requires_location": requires_location,
        "raw_text": text.strip(),
        "location_label": location_label,
    }


def build_food_keyword_groups(mode: str) -> List[str]:
    if mode == "breakfast":
        return ["早餐", "早午餐", "breakfast", "brunch"]
    if mode == "lunch":
        return ["午餐", "lunch", "便當", "餐廳"]
    if mode == "dinner":
        return ["晚餐", "dinner", "餐廳", "燒肉", "火鍋"]
    if mode == "late_night":
        return ["宵夜", "消夜", "late night food", "night market food"]
    if mode == "snack":
        return ["小吃", "麵店", "noodle", "street food"]
    if mode == "fine_dining":
        return ["fine dining", "高級餐廳", "牛排館", "omakase"]
    return ["美食", "小吃", "麵店", "restaurant", "fine dining"]


def food_mode_title(mode: str) -> str:
    return {
        "breakfast": "早餐 / 早午餐",
        "lunch": "午餐",
        "dinner": "晚餐",
        "late_night": "宵夜",
        "snack": "小吃 / 麵店",
        "fine_dining": "高級餐廳",
        "generic": "美食",
    }.get(mode, "美食")



def google_maps_place_link(place_id: str, name: str = "") -> str:
    if name:
        return f"https://www.google.com/maps/search/?api=1&query={quote(name)}&query_place_id={place_id}"
    return f"https://www.google.com/maps/search/?api=1&query=Google&query_place_id={place_id}"


def search_nearby_places(lat: float, lng: float, mode: str, radius_meters: int, minprice: Optional[int], maxprice: Optional[int]) -> List[Dict[str, Any]]:
    if not GOOGLE_MAPS_API_KEY:
        return []

    endpoint = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    groups = build_food_keyword_groups(mode)
    seen = set()
    results: List[Dict[str, Any]] = []
    fine_count = 0

    for keyword in groups:
        params = {
            "key": GOOGLE_MAPS_API_KEY,
            "location": f"{lat},{lng}",
            "radius": radius_meters,
            "keyword": keyword,
            "language": "zh-TW",
        }
        if minprice is not None:
            params["minprice"] = minprice
        if maxprice is not None:
            params["maxprice"] = maxprice

        try:
            resp = requests.get(endpoint, params=params, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            payload = resp.json()
            for item in payload.get("results", []):
                place_id = item.get("place_id")
                rating = float(item.get("rating") or 0)
                user_ratings_total = int(item.get("user_ratings_total") or 0)
                if (
                    not place_id
                    or place_id in seen
                    or rating < DEFAULT_FOOD_MIN_RATING
                    or rating > DEFAULT_FOOD_MAX_RATING
                    or user_ratings_total < DEFAULT_FOOD_MIN_REVIEWS
                ):
                    continue

                price_level = item.get("price_level")
                name = item.get("name", "未知店家")
                lowered = normalize_food_text(name)
                is_fine = price_level is not None and int(price_level) >= 3 or any(x in lowered for x in ["fine", "omakase", "牛排", "鐵板燒", "法式", "無菜單"])
                if mode == "generic" and is_fine:
                    if fine_count >= 2:
                        continue
                    fine_count += 1

                seen.add(place_id)
                results.append({
                    "name": name,
                    "rating": rating,
                    "user_ratings_total": user_ratings_total,
                    "price_level": price_level,
                    "address": item.get("vicinity") or item.get("formatted_address") or "",
                    "place_id": place_id,
                    "types": item.get("types") or [],
                    "maps_link": google_maps_place_link(place_id, name),
                })

                if len(results) >= DEFAULT_FOOD_LIMIT:
                    results.sort(key=lambda x: (x["rating"], x["user_ratings_total"]), reverse=True)
                    return results
        except Exception as e:
            logger.exception("Nearby food search failed for keyword=%s: %s", keyword, e)

    results.sort(key=lambda x: (x["rating"], x["user_ratings_total"]), reverse=True)
    return results[:DEFAULT_FOOD_LIMIT]


def format_price_level(level: Any) -> str:
    try:
        level = int(level)
    except Exception:
        return "價格未知"
    mapping = {0: "$", 1: "$$", 2: "$$$", 3: "$$$$", 4: "$$$$$"}
    return mapping.get(level, "價格未知")


def format_budget_hint(budget: Dict[str, Optional[int]]) -> str:
    low = budget.get("min_twd")
    high = budget.get("max_twd")
    if low and high:
        return f"預算：約 NT${low}~{high}"
    if high:
        return f"預算：NT${high} 以下"
    if low:
        return f"預算：NT${low} 以上"
    return "預算：不限"


def format_food_results_message(mode: str, location_label: str, radius_meters: int, budget: Dict[str, Optional[int]], places: List[Dict[str, Any]]) -> str:
    title = food_mode_title(mode)
    if not places:
        return (
            f"🍽️ {location_label} {title}推薦\n"
            f"範圍：約 {radius_meters // 1000} 公里｜{format_budget_hint(budget)}\n\n"
            f"目前找不到符合條件的店家。\n條件：評分 {DEFAULT_FOOD_MIN_RATING}~{DEFAULT_FOOD_MAX_RATING}、評論數 {DEFAULT_FOOD_MIN_REVIEWS}+。\n可以放寬價格或距離再試一次。"
        )

    lines = [
        f"🍽️ {location_label} {title}推薦",
        f"範圍：約 {radius_meters // 1000} 公里｜{format_budget_hint(budget)}",
        f"篩選：Google 評分 {DEFAULT_FOOD_MIN_RATING}~{DEFAULT_FOOD_MAX_RATING}｜評論數 {DEFAULT_FOOD_MIN_REVIEWS}+",
        "",
    ]
    for idx, item in enumerate(places, start=1):
        lines.append(f"{idx}. {item['name']}")
        lines.append(f"評分：{item['rating']:.1f}（{item['user_ratings_total']} 則）｜{format_price_level(item.get('price_level'))}")
        if item.get("address"):
            lines.append(f"地址：{item['address']}")
        lines.append(item["maps_link"])
        lines.append("")
    return "\n".join(lines).strip()


def send_location_request_for_food(chat_id: int, query_info: Dict[str, Any]) -> None:
    set_pending_food_request(chat_id, query_info)
    title = food_mode_title(query_info.get("mode", "generic"))
    km = max(1, round(query_info.get("radius_meters", DEFAULT_FOOD_RADIUS_METERS) / 1000))
    budget_hint = format_budget_hint(query_info.get("budget") or {"min_twd": None, "max_twd": None})
    payload = {
        "chat_id": chat_id,
        "text": (
            f"📍 這次要找 {title}。\n"
            f"目前沒有指定城市，我需要你的定位來查附近約 {km} 公里內的店家。\n"
            f"{budget_hint}｜篩選 Google 評分 {DEFAULT_FOOD_MIN_RATING}~{DEFAULT_FOOD_MAX_RATING}｜評論數 {DEFAULT_FOOD_MIN_REVIEWS}+\n\n"
            "請按下方按鈕分享目前位置。"
        ),
        "reply_markup": {
            "keyboard": [[{"text": "📍 分享目前位置", "request_location": True}]],
            "resize_keyboard": True,
            "one_time_keyboard": True,
        },
    }
    telegram_api("sendMessage", payload)


def handle_food_location_message(chat_id: int, location: Dict[str, Any]) -> None:
    pending = get_pending_food_request(chat_id)
    if not pending:
        send_message(chat_id, "已收到定位，但目前沒有待查詢的美食需求。可以直接輸入像是：附近美食、早餐、晚餐。")
        return

    lat = float(location.get("latitude"))
    lng = float(location.get("longitude"))
    places = search_nearby_places(
        lat=lat,
        lng=lng,
        mode=pending.get("mode", "generic"),
        radius_meters=int(pending.get("radius_meters", DEFAULT_FOOD_RADIUS_METERS)),
        minprice=pending.get("minprice"),
        maxprice=pending.get("maxprice"),
    )
    clear_pending_food_request(chat_id)
    payload = {"remove_keyboard": True}
    try:
        telegram_api("sendMessage", {
            "chat_id": chat_id,
            "text": format_food_results_message(
                pending.get("mode", "generic"),
                "你附近",
                int(pending.get("radius_meters", DEFAULT_FOOD_RADIUS_METERS)),
                pending.get("budget") or {"min_twd": None, "max_twd": None},
                places,
            ),
            "reply_markup": payload,
            "disable_web_page_preview": True,
        })
    except Exception:
        send_message(chat_id, format_food_results_message(
            pending.get("mode", "generic"),
            "你附近",
            int(pending.get("radius_meters", DEFAULT_FOOD_RADIUS_METERS)),
            pending.get("budget") or {"min_twd": None, "max_twd": None},
            places,
        ))


def handle_food(chat_id: int, text: str) -> None:
    register_chat_id(chat_id)
    if not GOOGLE_MAPS_API_KEY:
        send_message(chat_id, "目前尚未設定 GOOGLE_MAPS_API_KEY，所以還不能查美食地圖。")
        return

    query = parse_food_query(text)
    if query["requires_location"]:
        send_location_request_for_food(chat_id, query)
        return

    lat, lng = query["coords"]
    places = search_nearby_places(
        lat=lat,
        lng=lng,
        mode=query["mode"],
        radius_meters=query["radius_meters"],
        minprice=query["minprice"],
        maxprice=query["maxprice"],
    )
    send_message(
        chat_id,
        format_food_results_message(
            query["mode"],
            query["location_label"] or "指定地點",
            query["radius_meters"],
            query["budget"],
            places,
        ),
    )


def start_of_week(date_obj) -> datetime.date:
    return date_obj - timedelta(days=date_obj.weekday())


def weather_max_supported_date(today) -> datetime.date:
    return start_of_week(today) + timedelta(days=20)


def format_weekday(date_obj) -> str:
    names = "一二三四五六日"
    return names[date_obj.weekday()]


def normalize_weather_query_for_parsing(text: str) -> str:
    normalized = (text or "").strip()
    normalized = normalized.replace("　", " ")
    normalized = normalized.replace("臺", "台")
    normalized = normalized.replace("禮拜", "週")
    normalized = normalized.replace("星期", "週")
    normalized = re.sub(r"\s+", "", normalized)
    return normalized


def parse_md_date_token(token: str, today) -> Optional[datetime.date]:
    if not token:
        return None

    token = token.strip()
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})", token)
    if not m:
        m = re.fullmatch(r"(\d{1,2})月(\d{1,2})日?", token)
    if not m:
        return None

    month = int(m.group(1))
    day = int(m.group(2))

    for year in (today.year, today.year + 1):
        try:
            candidate = datetime(year, month, day).date()
        except ValueError:
            return None
        if candidate >= today:
            return candidate

    return None


def parse_relative_date_token(token: str, today) -> Optional[datetime.date]:
    token = token.strip()
    if token in ("今天", "今日", "現在"):
        return today
    if token in ("明天", "明日"):
        return today + timedelta(days=1)
    if token in ("後天",):
        return today + timedelta(days=2)

    m = re.fullmatch(r"(\d+)天後", token)
    if m:
        return today + timedelta(days=int(m.group(1)))

    return None


def parse_weekday_token(token: str, today) -> Optional[datetime.date]:
    token = token.strip()
    if not token:
        return None

    weekend_match = re.fullmatch(r"(這|这|本|今|下下|下|隔)?週末", token)
    if weekend_match:
        prefix = weekend_match.group(1) or ""
        week_offset = 0
        if prefix == "下":
            week_offset = 1
        elif prefix in ("下下", "隔"):
            week_offset = 2
        return start_of_week(today) + timedelta(days=5 + 7 * week_offset)

    weekday_match = re.fullmatch(r"(這|这|本|今|下下|下|隔)?週([一二三四五六日天七])", token)
    if not weekday_match:
        return None

    prefix = weekday_match.group(1) or ""
    weekday = WEATHER_WEEKDAY_MAP[weekday_match.group(2)]

    if prefix in ("這", "这", "本", "今"):
        week_offset = 0
    elif prefix == "下":
        week_offset = 1
    elif prefix in ("下下", "隔"):
        week_offset = 2
    else:
        this_week_candidate = start_of_week(today) + timedelta(days=weekday)
        if this_week_candidate >= today:
            return this_week_candidate
        return this_week_candidate + timedelta(days=7)

    return start_of_week(today) + timedelta(days=weekday + 7 * week_offset)


def parse_date_token(token: str, today) -> Optional[datetime.date]:
    token = token.strip()
    if not token:
        return None

    return (
        parse_relative_date_token(token, today)
        or parse_md_date_token(token, today)
        or parse_weekday_token(token, today)
    )


def extract_date_fragment(text: str) -> str:
    normalized = normalize_weather_query_for_parsing(text)
    city = resolve_weather_city(normalized)
    if city:
        aliases = sorted(
            {normalize_weather_text(alias) for alias in WEATHER_CITY_ALIASES.get(city, [])},
            key=len,
            reverse=True,
        )
        for alias in aliases:
            if alias:
                normalized = normalized.replace(alias, "")

    for token in ("/weather", "weather", "forecast", "天氣", "氣溫", "溫度", "查詢", "預報"):
        normalized = normalized.replace(token, "")
    return normalized.strip()


def parse_weather_date_range(text: str) -> Optional[Tuple[datetime.date, datetime.date, str]]:
    today = datetime.now(TZINFO).date()
    fragment = extract_date_fragment(text)

    if not fragment:
        return None

    if fragment in ("一周", "一週", "整周", "整週", "本周", "本週", "這周", "這週"):
        end = min(today + timedelta(days=6), weather_max_supported_date(today))
        return today, end, "一週"

    m = re.fullmatch(r"(這|这|本|今|下下|下|隔)?週六日", fragment)
    if m:
        prefix = m.group(1) or ""
        if prefix in ("這", "这", "本", "今"):
            week_offset = 0
            label = "這週六日"
        elif prefix == "下":
            week_offset = 1
            label = "下週六日"
        elif prefix in ("下下", "隔"):
            week_offset = 2
            label = "隔週六日"
        else:
            week_offset = 0
            label = "這週六日"
        start = start_of_week(today) + timedelta(days=5 + 7 * week_offset)
        end = start + timedelta(days=1)
        return start, end, label

    m = re.fullmatch(r"(這|这|本|今|下下|下|隔)?週末", fragment)
    if m:
        prefix = m.group(1) or ""
        if prefix in ("這", "这", "本", "今"):
            week_offset = 0
            label = "這週末"
        elif prefix == "下":
            week_offset = 1
            label = "下週末"
        elif prefix in ("下下", "隔"):
            week_offset = 2
            label = "隔週末"
        else:
            week_offset = 0
            label = "這週末"
        start = start_of_week(today) + timedelta(days=5 + 7 * week_offset)
        end = start + timedelta(days=1)
        return start, end, label

    range_match = re.fullmatch(r"(.+?)(?:~|－|-|到|至)(.+)", fragment)
    if range_match:
        left_token = range_match.group(1).strip()
        right_token = range_match.group(2).strip()
        start = parse_date_token(left_token, today)
        end = parse_date_token(right_token, today)
        if start and end:
            if end < start:
                start, end = end, start
            return start, end, f"{left_token}~{right_token}"

    single = parse_date_token(fragment, today)
    if single:
        return single, single, fragment

    return None


def weather_query_limit_message(city: str, max_date) -> str:
    return (
        f"目前 {city} 的進階日期查詢最遠支援到 {max_date.strftime('%Y-%m-%d')}（週{format_weekday(max_date)}）。\n"
        "可用範例：下週五天氣、下週六日天氣、3/30~4/5 天氣。"
    )


def weather_code_to_zh(code: Any) -> str:
    try:
        code = int(code)
    except Exception:
        return "未知"

    mapping = {
        0: "晴朗",
        1: "大致晴朗",
        2: "晴時多雲",
        3: "多雲",
        45: "有霧",
        48: "霧淞",
        51: "毛毛雨",
        53: "短暫毛雨",
        55: "較強毛雨",
        56: "凍雨",
        57: "強凍雨",
        61: "小雨",
        63: "雨",
        65: "大雨",
        66: "凍雨",
        67: "強凍雨",
        71: "小雪",
        73: "降雪",
        75: "大雪",
        77: "雪粒",
        80: "陣雨",
        81: "短暫陣雨",
        82: "強陣雨",
        85: "陣雪",
        86: "強陣雪",
        95: "雷雨",
        96: "雷雨伴冰雹",
        99: "強雷雨伴冰雹",
    }
    return mapping.get(code, "未知")


def fetch_weather_range(city: str, start_date, end_date) -> Optional[List[Dict[str, Any]]]:
    coords = WEATHER_CITY_COORDS.get(city)
    if not coords:
        logger.warning("No coordinates configured for city: %s", city)
        return None

    today = datetime.now(TZINFO).date()
    forecast_days = (end_date - today).days + 1
    forecast_days = max(1, min(16, forecast_days))

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": coords[0],
        "longitude": coords[1],
        "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max",
        "timezone": TIMEZONE,
        "forecast_days": forecast_days,
    }

    try:
        r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()

        daily = data.get("daily", {})
        dates = daily.get("time", [])
        weather_codes = daily.get("weather_code", [])
        tmax = daily.get("temperature_2m_max", [])
        tmin = daily.get("temperature_2m_min", [])
        popmax = daily.get("precipitation_probability_max", [])

        rows: List[Dict[str, Any]] = []
        for idx, date_str in enumerate(dates):
            day = datetime.fromisoformat(str(date_str)).date()
            if day < start_date or day > end_date:
                continue

            rows.append(
                {
                    "date": day,
                    "weather": weather_code_to_zh(weather_codes[idx] if idx < len(weather_codes) else None),
                    "min_temp": tmin[idx] if idx < len(tmin) else None,
                    "max_temp": tmax[idx] if idx < len(tmax) else None,
                    "pop": popmax[idx] if idx < len(popmax) else None,
                }
            )

        return rows
    except Exception as e:
        logger.exception("Weather range fetch failed: %s", e)
        return None


def build_weather_tip(pop: Any) -> str:
    try:
        pop_value = int(round(float(pop)))
    except Exception:
        return ""

    if pop_value >= 70:
        return "提醒：降雨機率高，建議一定要帶傘。"
    if pop_value >= 40:
        return "提醒：可能下雨，外出建議備傘。"
    if pop_value >= 20:
        return "提醒：有些降雨機會，行程可留意天氣變化。"
    return "提醒：降雨機率不高。"


def format_weather_range_message(city: str, forecasts: Optional[List[Dict[str, Any]]], label: str = "") -> str:
    if not forecasts:
        return "天氣資料取得失敗，請稍後再試。"

    if len(forecasts) == 1:
        item = forecasts[0]
        tip = build_weather_tip(item.get("pop"))
        lines = [
            f"🌤️ {city} {item['date'].strftime('%Y-%m-%d')}（週{format_weekday(item['date'])}）天氣預報",
            f"天氣：{item.get('weather', '未知')}",
            f"溫度：{item.get('min_temp', '--')} ~ {item.get('max_temp', '--')}°C",
            f"降雨機率：{item.get('pop', '--')}%",
        ]
        if tip:
            lines.append(tip)
        return "\n".join(lines)

    lines = [f"🌤️ {city} {label or '區間'}天氣預報", ""]
    for item in forecasts:
        lines.append(
            f"{item['date'].strftime('%m/%d')}（週{format_weekday(item['date'])}）"
            f"{item.get('weather', '未知')} "
            f"{item.get('min_temp', '--')}~{item.get('max_temp', '--')}°C "
            f"降雨{item.get('pop', '--')}%"
        )
    return "\n".join(lines).strip()


def handle_weather(chat_id: int, text: str = "") -> None:
    register_chat_id(chat_id)
    city = extract_weather_city(text)
    parsed_range = parse_weather_date_range(text)

    if not parsed_range:
        send_message(chat_id, format_weather_message(fetch_weather(city)))
        return

    start_date, end_date, label = parsed_range
    today = datetime.now(TZINFO).date()
    max_date = weather_max_supported_date(today)

    if start_date < today:
        send_message(chat_id, "目前僅支援今天之後的天氣預報。")
        return

    if end_date > max_date:
        send_message(chat_id, weather_query_limit_message(city, max_date))
        return

    forecasts = fetch_weather_range(city, start_date, end_date)
    send_message(chat_id, format_weather_range_message(city, forecasts, label))


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


WEEKDAY_MAP = {
    "一": 0, "二": 1, "三": 2, "四": 3, "五": 4, "六": 5, "日": 6, "天": 6,
}


def extract_time_and_message(text: str) -> Optional[Tuple[Optional[str], int, int, str]]:
    m = re.match(
        r"^\s*(早上|上午|中午|下午|晚上)?\s*(\d{1,2})(?:(?:\s*[:：]\s*(\d{1,2}))|(?:\s*點\s*(半|(\d{1,2}))?))?\s*(?:分)?\s*(提醒我)?\s*(.+?)\s*$",
        text,
    )
    if not m:
        return None

    period, hour_str, minute_str_colon, half_flag, minute_str_dot, _, msg = m.groups()
    hour = int(hour_str)

    if minute_str_colon is not None:
        minute = int(minute_str_colon)
    elif half_flag == "半":
        minute = 30
    elif minute_str_dot is not None:
        minute = int(minute_str_dot)
    else:
        minute = 0

    if minute < 0 or minute > 59:
        return None

    if period in ("下午", "晚上") and hour < 12:
        hour += 12
    elif period == "中午":
        if hour != 12 and hour < 11:
            hour += 12
    elif period in ("早上", "上午") and hour == 12:
        hour = 0

    if hour < 0 or hour > 23:
        return None

    return period, hour, minute, msg.strip()


def end_of_date_prefix(text: str) -> Optional[int]:
    for sep in (" ", "　"):
        idx = text.find(sep)
        if idx > 0:
            return idx
    return None


def split_date_and_rest(raw: str) -> Tuple[Optional[str], str]:
    raw = raw.strip()
    prefixes = [
        r"\d{4}[/-]\d{1,2}[/-]\d{1,2}",
        r"\d{1,2}[/-]\d{1,2}",
        r"今天",
        r"明天",
        r"後天",
        r"大後天",
        r"(\d+)天後",
        r"(\d+)週後",
        r"(\d+)周後",
        r"(\d+)個星期後",
        r"(\d+)個禮拜後",
        r"下週[一二三四五六日天]",
        r"下周[一二三四五六日天]",
        r"下禮拜[一二三四五六日天]",
        r"這週[一二三四五六日天]",
        r"這周[一二三四五六日天]",
        r"這禮拜[一二三四五六日天]",
    ]

    for pattern in prefixes:
        m = re.match(rf"^\s*({pattern})", raw)
        if m:
            prefix = m.group(1)
            rest = raw[m.end():].strip()
            return prefix, rest

    return None, raw


def parse_date_token(token: str, now: datetime) -> Optional[datetime.date]:
    token = token.strip()
    if not token:
        return now.date()

    if token == "今天":
        return now.date()
    if token == "明天":
        return (now + timedelta(days=1)).date()
    if token == "後天":
        return (now + timedelta(days=2)).date()
    if token == "大後天":
        return (now + timedelta(days=3)).date()

    m = re.fullmatch(r"(\d+)天後", token)
    if m:
        return (now + timedelta(days=int(m.group(1)))).date()

    m = re.fullmatch(r"(\d+)\s*(週|周|個星期|個禮拜)後", token)
    if m:
        return (now + timedelta(days=7 * int(m.group(1)))).date()

    m = re.fullmatch(r"(下週|下周|下禮拜|這週|這周|這禮拜)([一二三四五六日天])", token)
    if m:
        prefix, weekday_ch = m.groups()
        target_weekday = WEEKDAY_MAP[weekday_ch]
        current_weekday = now.weekday()
        delta = (target_weekday - current_weekday) % 7
        if prefix in ("下週", "下周", "下禮拜"):
            delta += 7 if delta == 0 else 7
        return (now + timedelta(days=delta)).date()

    m = re.fullmatch(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", token)
    if m:
        year, month, day = map(int, m.groups())
        try:
            return datetime(year, month, day, tzinfo=TZINFO).date()
        except ValueError:
            return None

    m = re.fullmatch(r"(\d{1,2})[/-](\d{1,2})", token)
    if m:
        month, day = map(int, m.groups())
        year = now.year
        try:
            candidate = datetime(year, month, day, tzinfo=TZINFO)
        except ValueError:
            return None
        if candidate.date() < now.date():
            try:
                candidate = datetime(year + 1, month, day, tzinfo=TZINFO)
            except ValueError:
                return None
        return candidate.date()

    return None


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

    date_token, rest = split_date_and_rest(raw)

    if date_token:
        base_date = parse_date_token(date_token, now)
        if not base_date:
            return None
        parsed = extract_time_and_message(rest)
        if not parsed:
            return None
        _, hour, minute, msg = parsed
        try:
            dt = datetime(base_date.year, base_date.month, base_date.day, hour, minute, tzinfo=TZINFO)
        except ValueError:
            return None
        if dt <= now:
            return None
        return {"event_time": dt, "message": msg.strip()}

    m = re.match(r"^\s*(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})\s+(.+?)\s*$", raw)
    if m:
        date_str, hour_str, minute_str, msg = m.groups()
        dt = datetime.strptime(f"{date_str} {hour_str}:{minute_str}", "%Y-%m-%d %H:%M").replace(tzinfo=TZINFO)
        if dt <= now:
            return None
        return {"event_time": dt, "message": msg.strip()}

    parsed = extract_time_and_message(raw)
    if not parsed:
        return None

    _, hour, minute, msg = parsed
    base_date = now.date()

    try:
        dt = datetime(base_date.year, base_date.month, base_date.day, hour, minute, tzinfo=TZINFO)
    except ValueError:
        return None

    if dt <= now:
        return None

    return {"event_time": dt, "message": msg.strip()}


def parse_chinese_reminder(text: str) -> Optional[Dict[str, Any]]:
    return parse_relative_reminder(text) or parse_absolute_reminder(text)


def notification_job_id(notification_id: int) -> str:
    return f"notify_{notification_id}"


def build_notification_text(label: str, event_time: datetime, message: str, event_id: int) -> str:
    if label == "- 1小時前":
        return f"⏰ 提醒通知\n還有1小時：{event_time.strftime('%Y-%m-%d %H:%M')}｜{message}"
    return f"⏰ 提醒通知\n現在時間到：{event_time.strftime('%Y-%m-%d %H:%M')}｜{message}"


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
                SELECT rn.sent
                FROM reminder_notifications rn
                WHERE rn.id = %s
                """,
                (notification_id,)
            )
            row = cur.fetchone()
            if not row:
                return False
            return int(row["sent"]) == 0
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
                WHERE rn.sent = 0
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
                  AND rn.sent = 0
                ORDER BY re.event_time ASC, re.id ASC
                """,
                (chat_id,)
            )
            return cur.fetchall()
    finally:
        conn.close()


def delete_event_by_id(event_id: int, chat_id: int) -> bool:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM reminder_events
                WHERE id = %s AND chat_id = %s
                """,
                (event_id, chat_id)
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return False

            cur.execute("DELETE FROM reminder_notifications WHERE event_id = %s", (event_id,))
            cur.execute("DELETE FROM reminder_events WHERE id = %s AND chat_id = %s", (event_id, chat_id))

        conn.commit()
        return True
    finally:
        conn.close()


def delete_all_events(chat_id: int) -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM reminder_events
                WHERE chat_id = %s
                """,
                (chat_id,)
            )
            rows = cur.fetchall()
            event_ids = [int(row["id"]) for row in rows]

            if not event_ids:
                conn.commit()
                return 0

            cur.execute(
                """
                DELETE FROM reminder_notifications
                WHERE event_id IN (
                    SELECT id FROM reminder_events WHERE chat_id = %s
                )
                """,
                (chat_id,)
            )
            cur.execute("DELETE FROM reminder_events WHERE chat_id = %s", (chat_id,))

        conn.commit()
        return len(event_ids)
    finally:
        conn.close()


def cleanup_all_completed_events() -> None:
    conn = get_conn()
    try:
        now_dt = datetime.now(TZINFO)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT re.id
                FROM reminder_events re
                LEFT JOIN reminder_notifications rn
                  ON rn.event_id = re.id
                 AND rn.sent = 0
                WHERE rn.id IS NULL
                   OR re.event_time <= %s
                """,
                (now_dt,)
            )
            rows = cur.fetchall()

            event_ids = [int(row["id"]) for row in rows]
            if event_ids:
                cur.execute("DELETE FROM reminder_notifications WHERE event_id = ANY(%s)", (event_ids,))
                cur.execute("DELETE FROM reminder_events WHERE id = ANY(%s)", (event_ids,))

        conn.commit()
        logger.info("Daily cleanup finished. cleaned=%s", len(rows))
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


def find_latest_event_by_keyword(chat_id: int, keyword: str) -> Optional[Dict[str, Any]]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, event_time, message, keyword
                FROM reminder_events
                WHERE chat_id = %s
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


def build_display_code(event_time: datetime, daily_index: int) -> str:
    return f"{event_time.strftime('%m%d')}-{daily_index}"


def build_display_mapping(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        event_time = parse_db_datetime(row["event_time"])
        date_key = event_time.strftime("%Y-%m-%d")
        row_copy = dict(row)
        row_copy["_event_time_obj"] = event_time
        grouped[date_key].append(row_copy)

    display_rows: List[Dict[str, Any]] = []
    mapping: Dict[str, int] = {}

    for date_key in sorted(grouped.keys()):
        day_rows = sorted(grouped[date_key], key=lambda x: (x["_event_time_obj"], int(x["id"])))
        for idx, row in enumerate(day_rows, start=1):
            display_code = build_display_code(row["_event_time_obj"], idx)
            row["display_code"] = display_code
            mapping[display_code] = int(row["id"])
            display_rows.append(row)

    return display_rows, mapping


def resolve_event_id_from_cancel_token(chat_id: int, token: str) -> Optional[int]:
    rows = get_user_pending_events(chat_id)
    display_rows, mapping = build_display_mapping(rows)

    if token in mapping:
        return mapping[token]

    if token.isdigit():
        db_event_id = int(token)
        for row in display_rows:
            if int(row["id"]) == db_event_id:
                return db_event_id

    return None


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


def remove_all_scheduled_jobs_for_chat(chat_id: int) -> None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT rn.id
                FROM reminder_notifications rn
                JOIN reminder_events re ON rn.event_id = re.id
                WHERE re.chat_id = %s
                  AND rn.sent = 0
                """,
                (chat_id,)
            )
            rows = cur.fetchall()

        for row in rows:
            notification_id = int(row["id"])
            try:
                scheduler.remove_job(notification_job_id(notification_id))
            except JobLookupError:
                pass
            except Exception as e:
                logger.exception("Failed removing notification job id=%s: %s", notification_id, e)
    finally:
        conn.close()


def catch_up_missed_notifications() -> None:
    try:
        rows = get_due_unsent_notifications()
        if not rows:
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

            try:
                scheduler.remove_job(notification_job_id(notification_id))
            except JobLookupError:
                pass
            except Exception as e:
                logger.exception("Failed removing catch-up job id=%s: %s", notification_id, e)

            logger.info("Catch-up notification sent: id=%s event_id=%s", notification_id, event_id)

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
        "/news\n"
        "/news tech\n"
        "/news business\n"
        "/weather\n"
        "附近美食（會要求定位）\n"
        "/list\n"
        "/cancel 事件代碼\n"
        "/cancel_all\n"
        "/help\n\n"
    )
    send_message(chat_id, msg)


def handle_help(chat_id: int) -> None:
    msg = (
        "指令說明\n\n"
        "/start\n"
        "/help\n"
        "/news\n"
        "/news tech\n"
        "/news business\n"
        "/weather\n"
        "/weather 臺北市\n"
        "桃園天氣\n"
        "下週五天氣\n"
        "桃園下週六日天氣\n"
        "3/30~4/5 天氣\n"
        "附近美食（會要求定位）\n"
        "早餐 200以下（會要求定位）\n"
        "台南晚餐 500~1000 3公里\n"
        "/list\n"
        "/cancel 事件代碼\n"
        "/cancel_all\n\n"
        "提醒輸入範例：\n"
        "晚上7點半打球\n"
        "今天早上七點吃早餐\n"
        "明天晚上七點半打球\n"
        "30分鐘後提醒我喝水\n"
        "兩小時後提醒我洗衣服\n\n"
        "多筆提醒範例：\n"
        "明天早上9點開會\n"
        "明天下午2點買東西\n"
        "30分鐘後提醒我喝水\n\n"
        "事件代碼格式會依日期每日重新編號，例如：0329-1、0329-2、0330-1\n"
        "取消範例：\n"
        "/cancel 0329-1\n"
        "取消所有提醒\n"
        "/cancel_all"
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

    display_rows, _ = build_display_mapping(rows)

    lines = ["📌 目前所有未取消提醒", ""]
    current_date_label = None

    for row in display_rows[:50]:
        event_time = row["_event_time_obj"]
        date_label = event_time.strftime("%Y-%m-%d")
        if date_label != current_date_label:
            if current_date_label is not None:
                lines.append("")
            lines.append(f"📅 {date_label}")
            current_date_label = date_label

        lines.append(f"事件代碼：{row['display_code']}")
        lines.append(f"{event_time.strftime('%Y-%m-%d %H:%M')}｜{row['message']}")
        lines.append("")

    send_message(chat_id, "\n".join(lines).strip())


def handle_cancel(chat_id: int, text: str) -> None:
    m = re.match(r"^/cancel\s+([0-9]{4}-\d+|\d+)\s*$", text.strip())
    if not m:
        send_message(chat_id, "用法：/cancel 事件代碼\n例如：/cancel 0329-1")
        return

    token = m.group(1)
    event_id = resolve_event_id_from_cancel_token(chat_id, token)
    if event_id is None:
        send_message(chat_id, f"找不到可取消的事件代碼 {token}")
        return

    remove_scheduled_jobs_for_event(event_id)
    ok = delete_event_by_id(event_id, chat_id)
    if not ok:
        send_message(chat_id, f"找不到可取消的事件代碼 {token}")
        return
    send_message(chat_id, "✅ 已取消提醒")


def handle_cancel_all(chat_id: int) -> None:
    remove_all_scheduled_jobs_for_chat(chat_id)
    canceled_count = delete_all_events(chat_id)
    if canceled_count <= 0:
        send_message(chat_id, "目前沒有可取消的提醒。")
        return
    send_message(chat_id, f"✅ 已取消所有提醒，共 {canceled_count} 筆事件。")


def handle_cancel_by_keyword(chat_id: int, text: str) -> bool:
    normalized = text.strip().replace("　", "")
    if normalized in ("取消所有提醒", "清空所有提醒", "刪除所有提醒"):
        handle_cancel_all(chat_id)
        return True

    m = re.match(r"^\s*取消\s*(.+?)\s*$", text)
    if not m:
        return False

    keyword = m.group(1).strip()
    if not keyword or keyword == "所有提醒":
        handle_cancel_all(chat_id)
        return True

    row = find_latest_event_by_keyword(chat_id, keyword)
    if not row:
        send_message(chat_id, f"找不到符合「{keyword}」的未取消事件。")
        return True

    event_id = int(row["id"])
    event_time = parse_db_datetime(row["event_time"])

    remove_scheduled_jobs_for_event(event_id)
    ok = delete_event_by_id(event_id, chat_id)
    if not ok:
        send_message(chat_id, "取消失敗，請稍後再試。")
        return True

    send_message(chat_id, f"✅ 已取消提醒\n{event_time.strftime('%Y-%m-%d %H:%M')}｜{row['message']}")
    return True


def try_handle_multiple_event_reminders(chat_id: int, text: str) -> bool:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) <= 1:
        return False

    parsed_items = []
    failed_lines = []

    for line in lines:
        parsed = parse_chinese_reminder(line)
        if parsed:
            parsed_items.append((line, parsed))
        else:
            failed_lines.append(line)

    if not parsed_items:
        return False

    success_lines = []

    for _, parsed in parsed_items:
        event_time: datetime = parsed["event_time"]
        message: str = parsed["message"]

        replaced = False
        duplicate = find_duplicate_event(chat_id, event_time, message)
        if duplicate:
            old_event_id = int(duplicate["id"])
            remove_scheduled_jobs_for_event(old_event_id)
            if delete_event_by_id(old_event_id, chat_id):
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

        prefix = "✅ 已更新" if replaced else "✅ 已建立"
        success_lines.append(f"{prefix}｜{event_time.strftime('%Y-%m-%d %H:%M')}｜{message}")

    reply = ["📌 多筆提醒處理結果", ""]
    reply.extend(success_lines)

    if failed_lines:
        reply.append("")
        reply.append("⚠️ 下列內容未成功辨識：")
        reply.extend(failed_lines)

    send_message(chat_id, "\n".join(reply))
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
        if delete_event_by_id(old_event_id, chat_id):
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
        "/start\n"
        "/help\n"
        "/news\n"
        "/news tech\n"
        "/news business\n"
        "/weather\n"
        "附近美食（會要求定位）\n"
        "/list\n"
        "/cancel 事件代碼\n"
        "/cancel_all\n\n"
        "也可以直接輸入：\n"
        "news\n"
        "new\n"
        "weather\n"
        "天氣\n"
        "桃園天氣\n"
        "下週五天氣\n"
        "桃園下週六日天氣\n"
        "3/30~4/5 天氣\n"
        "美食 / 小吃 / 早餐 / 午餐 / 晚餐 / 宵夜（沒寫地點會要求定位）\n"
        "台南晚餐 500~1000 3公里\n\n"
        "也可以直接輸入提醒，例如：\n"
        "晚上7點半打球\n"
        "今天早上七點吃早餐\n"
        "明天晚上七點半打球\n"
        "兩小時後提醒我喝水\n\n"
        "多筆提醒可一行一筆輸入。\n"
        "全部取消可輸入：取消所有提醒"
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

    try:
        scheduler.remove_job("daily_cleanup_job")
    except JobLookupError:
        pass
    except Exception as e:
        logger.exception("Failed removing old daily_cleanup_job: %s", e)

    try:
        c_hour, c_minute = DAILY_CLEANUP_TIME.split(":")
        scheduler.add_job(
            cleanup_all_completed_events,
            trigger="cron",
            hour=int(c_hour),
            minute=int(c_minute),
            id="daily_cleanup_job",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        logger.info("Daily cleanup scheduled at %s (%s)", DAILY_CLEANUP_TIME, TIMEZONE)
    except Exception as e:
        logger.exception("Failed scheduling daily cleanup: %s", e)

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
            "daily_cleanup_time": DAILY_CLEANUP_TIME,
            "owner_id_set": bool(OWNER_ID),
            "openai_model": OPENAI_MODEL,
            "chinese_summary_enabled": ENABLE_CHINESE_SUMMARY,
            "weather_city": DEFAULT_WEATHER_CITY,
            "weather_enabled": bool(CWA_API_KEY),
            "food_enabled": bool(GOOGLE_MAPS_API_KEY),
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

        if OWNER_ID and chat_id != OWNER_ID:
            logger.info("Blocked non-owner: %s", chat_id)
            return jsonify({"ok": True})

        if message.get("location"):
            handle_food_location_message(chat_id, message["location"])
            return jsonify({"ok": True})

        text = (message.get("text") or "").strip()
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
        elif is_food_query(text):
            handle_food(chat_id, text)
        elif is_weather_query(text):
            handle_weather(chat_id, text)
        elif text.startswith("/list"):
            handle_list(chat_id)
        elif text.startswith("/cancel_all"):
            handle_cancel_all(chat_id)
        elif text.startswith("/cancel"):
            handle_cancel(chat_id, text)
        else:
            if handle_cancel_by_keyword(chat_id, text):
                return jsonify({"ok": True})

            if try_handle_multiple_event_reminders(chat_id, text):
                return jsonify({"ok": True})

            if try_handle_event_reminder(chat_id, text):
                return jsonify({"ok": True})

            if handle_ai_router(chat_id, text):
                return jsonify({"ok": True})

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
