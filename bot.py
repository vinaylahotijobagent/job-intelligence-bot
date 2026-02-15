import requests
import os
import sqlite3
from datetime import datetime, timezone

# ================= CONFIG =================

DB_NAME = "jobs.db"

MICROSOFT_BASE_URL = "https://apply.careers.microsoft.com/api/pcsx/search"
AMAZON_API = "https://www.amazon.jobs/en/search.json"

MICROSOFT_SEARCH_TERMS = [
    "Data",
    "Analytics",
    "Data Engineer",
    "Azure",
    "Power BI",
    "Databricks"
]

MAX_PAGES = 5
DAYS_BACK = 1
SECONDS_BACK = DAYS_BACK * 86400

# ==========================================


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
    now_ts = int(datetime.now(timezone.utc).timestamp())
    return (now_ts - posted_ts) <= SECONDS_BACK


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
                if cursor.fetchone():
                    continue

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


def ingest_amazon():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    total_checked = 0
    inserted = 0
    offset = 0

    while True:
        data = fetch_amazon_jobs(offset)

        # Amazon JSON structure uses "jobs" list
        jobs_list = data.get("jobs", [])
        if not jobs_list:
            break

        for job in jobs_list:
            total_checked += 1

            # Amazon uses "updated_at" field
            # Example: "2026-02-13T10:15:00Z"
            updated_at = job.get("updated_at")
            if not updated_at:
                continue

            dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            posted_ts = int(dt.timestamp())

            if not is_recent(posted_ts):
                continue

            job_id = f"amz_{job['id']}"
            title = job.get("title", "")
            link = "https://www.amazon.jobs" + job.get("url", "")

            cursor.execute("SELECT 1 FROM jobs WHERE job_id=?", (job_id,))
            if cursor.fetchone():
                continue

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
