import requests
import sqlite3
from datetime import datetime, timezone

JPMC_API = "https://jpmc.fa.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions"

SECONDS_BACK = 3 * 86400


def is_recent(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    posted_ts = int(dt.replace(tzinfo=timezone.utc).timestamp())
    now_ts = int(datetime.now(timezone.utc).timestamp())
    return (now_ts - posted_ts) <= SECONDS_BACK, posted_ts


def fetch_jpmc(offset=0):
    params = {
        "onlyData": "true",
        "limit": 25,
        "offset": offset,
        "finder": "findReqs;siteNumber=CX_1001,locationId=300000081155702,radius=25,radiusUnit=MI"
    }

    r = requests.get(JPMC_API, params=params)
    return r.json()


def ingest_jpmc(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    total_checked = 0
    inserted = 0
    offset = 0

    while True:
        data = fetch_jpmc(offset)

        items = data.get("items", [])
        if not items:
            break

        requisitions = items[0].get("requisitionList", [])
        if not requisitions:
            break

        for job in requisitions:
            total_checked += 1

            recent, posted_ts = is_recent(job["PostedDate"])
            if not recent:
                continue

            job_id = f"jpmc_{job['Id']}"
            title = job["Title"]
            link = f"https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/job/{job['Id']}"

            cursor.execute("SELECT 1 FROM jobs WHERE job_id=?", (job_id,))
            if cursor.fetchone():
                continue

            cursor.execute("""
                INSERT INTO jobs
                (job_id, company, title, link, search_term, posted_ts, date_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                job_id,
                "JP Morgan",
                title,
                link,
                "Hyderabad",
                posted_ts,
                datetime.now().isoformat()
            ))

            inserted += 1

        offset += 25

    conn.commit()
    conn.close()

    return total_checked, inserted
