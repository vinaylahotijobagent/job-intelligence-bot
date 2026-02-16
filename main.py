from db import create_db
from utils import send_message

from scrapers.microsoft import run as run_microsoft
from scrapers.amazon import run as run_amazon
from scrapers.jpmc import run as run_jpmc

def main():
    create_db()

    total_checked = 0
    total_inserted = 0

    for scraper in [run_microsoft, run_amazon, run_jpmc]:
        checked, inserted = scraper()
        total_checked += checked
        total_inserted += inserted

    summary = f"""
Daily Job Scan (Last 24h)

Total Checked: {total_checked}
New Inserted: {total_inserted}
"""

    send_message(summary)

if __name__ == "__main__":
    main()
