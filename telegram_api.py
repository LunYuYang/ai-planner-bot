import requests
from config import BOT_TOKEN


HTTP_TIMEOUT = 20


def send_message(chat_id, text, disable_web_page_preview=False):
    if not BOT_TOKEN:
        raise RuntimeError("Missing BOT_TOKEN in environment variables.")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview,
    }

    resp = requests.post(url, json=payload, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()

    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram sendMessage failed: {data}")

    return data