import requests
import sqlite3
from datetime import datetime, timezone

AMAZON_API = "https://www.amazon.jobs/en/search.json"
SECONDS_BACK = 3 * 86400   # safer window


def is_recent(posted_ts):
    now_ts = int(datetime.now(timezone.utc).timestamp())
    return (now_ts - posted_ts) <= SECONDS_BACK


def fetch_amazon(offset=0):
    params = {
        "radius": "24km",
        "offset": offset,
        "result_limit": 50,
        "loc_query": "Hyderabad",
        "base_query": ""
    }
    r = requests.get(AMAZON_API, params=params)
    return r.json()


def ingest_amazon(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    total_checked = 0
    inserted = 0
    offset = 0

    while True:
        data = fetch_amazon(offset)
        jobs = data.get("jobs", [])

        if not jobs:
            break

        for job in jobs:
            total_checked += 1

            updated = job.get("updated_at")
            if not updated:
                continue

            dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            posted_ts = int(dt.timestamp())

            if not is_recent(posted_ts):
                continue

            job_id = f"amz_{job['id']}"
            title = job.get("title")
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
                "Amazon Hyderabad",
                posted_ts,
                datetime.now().isoformat()
            ))

            inserted += 1

        offset += len(jobs)

    conn.commit()
    conn.close()

    return total_checked, inserted
