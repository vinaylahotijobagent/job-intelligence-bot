# scrapers/amazon.py

import sqlite3
from datetime import datetime


def is_recent(posted_date: str) -> bool:
    return True  # Adjust logic if needed


def fetch_amazon():
    jobs = [
        {
            "title": "Software Development Engineer",
            "location": "Bangalore",
            "posted": "2026-02-15",
            "company": "Amazon"
        }
    ]
    return jobs


def ingest_amazon(db_name: str):
    jobs = fetch_amazon()

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            location TEXT,
            company TEXT,
            posted TEXT
        )
    """)

    for job in jobs:
        if is_recent(job["posted"]):
            cursor.execute("""
                INSERT INTO jobs (title, location, company, posted)
                VALUES (?, ?, ?, ?)
            """, (
                job["title"],
                job["location"],
                job["company"],
                job["posted"]
            ))

    conn.commit()
    conn.close()


# 🔥 STANDARD ENTRYPOINT
def run(db_name: str):
    ingest_amazon(db_name)
