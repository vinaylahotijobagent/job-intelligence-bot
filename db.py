import sqlite3
from datetime import datetime
from config import DB_NAME

def create_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            company TEXT,
            title TEXT,
            link TEXT,
            posted_ts INTEGER,
            date_seen TEXT
        )
    """)

    conn.commit()
    conn.close()

def job_exists(job_id):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("SELECT 1 FROM jobs WHERE job_id=?", (job_id,))
    exists = cursor.fetchone()

    conn.close()
    return exists is not None

def insert_job(job_id, company, title, link, posted_ts):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO jobs (job_id, company, title, link, posted_ts, date_seen)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        job_id,
        company,
        title,
        link,
        posted_ts,
        datetime.now().isoformat()
    ))

    conn.commit()
    conn.close()
