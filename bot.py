import requests
import os
import hashlib
import sqlite3
from datetime import datetime

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

LINKEDIN_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=Data%20Analyst%20OR%20Azure%20OR%20Databricks%20OR%20Power%20BI&location=Hyderabad&f_TPR=r86400"

DB_NAME = "jobs.db"

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

def fetch_linkedin_jobs():
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(LINKEDIN_URL, headers=headers)
    return response.text

def parse_jobs(html):
    jobs = []
    lines = html.split('base-card__full-link')
    for line in lines[1:6]:
        try:
            link_part = line.split('href="')[1]
            link = link_part.split('"')[0]

            title_part = line.split('>')[1]
            title = title_part.split('<')[0]

            company_part = line.split('base-search-card__subtitle">')[1]
            company = company_part.split('<')[0]

            jobs.append((title.strip(), company.strip(), link.strip()))
        except:
            continue
    return jobs

def store_and_notify(jobs):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for title, company, link in jobs:
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

    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_db()
    html = fetch_linkedin_jobs()
    jobs = parse_jobs(html)
    store_and_notify(jobs)
