import requests
import os
import hashlib
import sqlite3
from datetime import datetime

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

DB_NAME = "jobs.db"

KEYWORDS = [
    "Data Analyst",
    "Azure Data Engineer",
    "Databricks",
    "Power BI",
    "BI Developer"
]

SEARCH_URLS = []

for keyword in KEYWORDS:
    encoded = keyword.replace(" ", "%20")
    for start in [0, 25, 50]:
        url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={encoded}&location=Hyderabad&f_TPR=r86400&start={start}"
        SEARCH_URLS.append(url)


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
            fingerprint TEXT PRIMARY KEY,
            title TEXT,
            company TEXT,
            link TEXT,
            date_seen TEXT
        )
    """)
    conn.commit()
    conn.close()

def create_fingerprint(company, title):
    key = f"{company.lower().strip()}_{title.lower().strip()}"
    return hashlib.md5(key.encode()).hexdigest()

def fetch_jobs_from_url(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    return response.text

def parse_jobs(html):
    jobs = []
    sections = html.split('base-card__full-link')

    for section in sections[1:]:
        try:
            link = section.split('href="')[1].split('"')[0]
            title = section.split('>')[1].split('<')[0]
            company = section.split('base-search-card__subtitle">')[1].split('<')[0]

            jobs.append((title.strip(), company.strip(), link.strip()))
        except:
            continue

    return jobs

def store_and_notify(all_jobs):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    new_count = 0

    for title, company, link in all_jobs:
        fingerprint = create_fingerprint(company, title)

        cursor.execute("SELECT 1 FROM jobs WHERE fingerprint=?", (fingerprint,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(
                "INSERT INTO jobs VALUES (?, ?, ?, ?, ?)",
                (fingerprint, title, company, link, datetime.now().isoformat())
            )

            message = f"🔥 <b>{title}</b>\n🏢 {company}\n🔗 {link}"
            send_message(message)

            new_count += 1

            if new_count >= 10:
                break

    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_db()

    all_jobs = []

    for url in SEARCH_URLS:
        html = fetch_jobs_from_url(url)
        jobs = parse_jobs(html)
        all_jobs.extend(jobs)

    store_and_notify(all_jobs)
