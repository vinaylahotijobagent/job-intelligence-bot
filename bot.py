import requests
import os
import sqlite3
from datetime import datetime, timezone

# ---------------- CONFIG ----------------

DB_NAME = "jobs.db"

MICROSOFT_BASE_URL = "https://apply.careers.microsoft.com/api/pcsx/search"

MICROSOFT_SEARCH_TERMS = [
    "Data",
    "Analytics",
    "Data Engineer",
    "Azure",
    "Power BI",
    "Databricks"
]

AMAZON_API = "https://www.amazon.jobs/en/search.json"

MAX_PAGES = 5          # Pagination depth
DAYS_BACK = 1          # Last 24 hours
SECONDS_BACK = DAYS_BACK * 86400

# ----------------------------------------


def send_message(text):
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    CHAT_ID = os.getenv("CHAT_ID")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text
    }
    requests.post(url, data=payload)


def create_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            company TEXT,
            title TEXT,
            link TEXT,
            search_term TEXT,
            posted_ts INTEGER,
            processed INTEGER DEFAULT 0,
            date_seen TEXT
        )
    """)

    conn.commit()
    conn.close()


def is_recent(posted_ts):
    posted_time = datetime.fromtimestamp(posted_ts, tz=timezone.utc)
    now = datetime.now(timezone.utc)
    return (now - posted_time).total_seconds() <= SECONDS_BACK


# ================= MICROSOFT =================

def fetch_microsoft_jobs(term, start):
    params = {
        "domain": "microsoft.com",
        "query": term,
        "location": "India, Telangana, Hyderabad",
        "start": start,
        "sort_by": "distance",
        "filter_distance": 160,
        "filter_include_remote": 1
    }

    response = requests.get(MICROSOFT_BASE_URL, params=params)
    return response.json()


def ingest_microsoft():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    total_checked = 0
    inserted = 0

    for term in MICROSOFT_SEARCH_TERMS:
        for page in range(MAX_PAGES):
            start = page * 25
            data = fetch_microsoft_jobs(term, start)

            positions = data.get("data", {}).get("positions", [])
            if not positions:
                break

            for job in positions:
                total_checked += 1

                job_id = f"ms_{job['id']}"
                title = job["name"]
                posted_ts = job["postedTs"]
                link = "https://apply.careers.microsoft.com" + job["positionUrl"]

                if not is_recent(posted_ts):
                    continue

                cursor.execute("SELECT 1 FROM jobs WHERE job_id=?", (job_id,))
                exists = cursor.fetchone()

                if not exists:
                    cursor.execute("""
                        INSERT INTO jobs 
                        (job_id, company, title, link, search_term, posted_ts, date_seen)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        job_id,
                        "Microsoft",
                        title,
                        link,
                        term,
                        posted_ts,
                        datetime.now().isoformat()
                    ))

                    inserted += 1

    conn.commit()
    conn.close()

    return total_checked, inserted


# ================= AMAZON =================

def fetch_amazon_jobs(offset=0):
    params = {
        "radius": "24km",
        "offset": offset,
        "result_limit": 50,
        "loc_query": "Hyderabad",
        "base_query": ""
    }
    response = requests.get(AMAZON_API, params=params)
    return response.json()


def parse_amazon_posting_date(dt_str):
    # Example format: "2026-02-13T10:15:00Z"
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return int(dt.timestamp())


def ingest_amazon():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    total_checked = 0
    inserted = 0
    offset = 0

    while True:
        data = fetch_amazon_jobs(offset)
        jobs_list = data.get("jobs", [])

        if not jobs_list:
            break

        for job in jobs_list:
            total_checked += 1

            posted_ts = parse_amazon_posting_date(job["posting_date"])
            now_ts = int(datetime.now(timezone.utc).timestamp())

            if now_ts - posted_ts > SECONDS_BACK:
                continue

            job_id = f"amz_{job['id']}"
            title = job["title"]
            link = "https://www.amazon.jobs" + job["url"]

            cursor.execute("SELECT 1 FROM jobs WHERE job_id=?", (job_id,))
            exists = cursor.fetchone()

            if not exists:
                cursor.execute("""
                    INSERT INTO jobs 
                    (job_id, company, title, link, search_term, posted_ts, date_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    job_id,
                    "Amazon",
                    title,
                    link,
                    "Amazon Search",
                    posted_ts,
                    datetime.now().isoformat()
                ))

                inserted += 1

        offset += len(jobs_list)

    conn.commit()
    conn.close()

    return total_checked, inserted


# ================= MAIN =================

if __name__ == "__main__":
    create_db()

    ms_checked, ms_inserted = ingest_microsoft()
    amz_checked, amz_inserted = ingest_amazon()

    summary = f"""
Daily Ingestion Summary (Last 24h)

Microsoft:
Checked: {ms_checked}
Inserted: {ms_inserted}

Amazon:
Checked: {amz_checked}
Inserted: {amz_inserted}
"""

    send_message(summary)
