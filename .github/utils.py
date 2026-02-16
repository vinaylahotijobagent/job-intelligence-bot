import os
import requests
from datetime import datetime, timezone
from config import SECONDS_BACK

def is_recent(posted_ts):
    now_ts = int(datetime.now(timezone.utc).timestamp())
    return (now_ts - posted_ts) <= SECONDS_BACK

def send_message(text):
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    CHAT_ID = os.getenv("CHAT_ID")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text
    }

    requests.post(url, data=payload)
