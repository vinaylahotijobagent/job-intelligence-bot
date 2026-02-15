import requests
import os
import sqlite3
import hashlib
from datetime import datetime, timezone

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

DB_NAME = "jobs.db"

BASE_URL = "https://apply.careers.microsoft.com/api/pcsx/search"

KEYWORDS = ["data", "azure", "analytics", "bi", "databricks", "power"]

def send_message(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }
    requests.post(url, data=payload)

def create_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            title TEXT,
            link TEXT,
            date_seen TEXT
        )
    """)
    conn.commit()
    conn.close()

def fetch_microsoft_jobs(start):
    params = {
        "domain": "microsoft.com",
        "query": "",
        "location": "India, Telangana, Hyderabad",
        "start": start,
        "sort_by": "distance",
        "filter_distance": 160,
        "filter_include_remote": 1
    }

    response = requests.get(BASE_URL, params=params)
    return response.json()

def is_recent(posted_ts):
    posted_time = datetime.fromtimestamp(posted_ts, tz=timezone.utc)
    now = datetime.now(timezone.utc)
    return (now - posted_time).total_seconds() <= 259200

def matches_keywords(title):
    title_lower = title.lower()
    return any(keyword in title_lower for keyword in KEYWORDS)

def process_jobs():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    new_count = 0

    for start in [0, 25, 50, 75]:
        data = fetch_microsoft_jobs(start)

        for job in data["data"]["positions"]:
            job_id = str(job["id"])
            title = job["name"]
            posted_ts = job["postedTs"]
            link = "https://apply.careers.microsoft.com" + job["positionUrl"]

            if not is_recent(posted_ts):
                continue

            if not matches_keywords(title):
                continue

            cursor.execute("SELECT 1 FROM jobs WHERE job_id=?", (job_id,))
            exists = cursor.fetchone()

            if not exists:
                cursor.execute(
                    "INSERT INTO jobs VALUES (?, ?, ?, ?)",
                    (job_id, title, link, datetime.now().isoformat())
                )

                message = f"🔥 <b>{title}</b>\n🏢 Microsoft\n🔗 {link}"
                send_message(message)

                new_count += 1

    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_db()
    process_jobs()
