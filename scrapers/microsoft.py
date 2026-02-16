import requests
from utils import is_recent
from db import job_exists, insert_job

BASE_URL = "https://apply.careers.microsoft.com/api/pcsx/search"

SEARCH_TERMS = [
    "Data",
    "Analytics",
    "Data Engineer",
    "Azure",
    "Power BI",
    "Databricks"
]

def run():
    total_checked = 0
    inserted = 0

    for term in SEARCH_TERMS:
        params = {
            "domain": "microsoft.com",
            "query": term,
            "location": "India, Telangana, Hyderabad",
            "start": 0,
            "sort_by": "distance",
            "filter_distance": 160,
            "filter_include_remote": 1
        }

        response = requests.get(BASE_URL, params=params)
        data = response.json()
        positions = data.get("data", {}).get("positions", [])

        for job in positions:
            total_checked += 1

            job_id = f"ms_{job['id']}"
            title = job["name"]
            posted_ts = job["postedTs"]
            link = "https://apply.careers.microsoft.com" + job["positionUrl"]

            if not is_recent(posted_ts):
                continue

            if job_exists(job_id):
                continue

            insert_job(job_id, "Microsoft", title, link, posted_ts)
            inserted += 1

    return total_checked, inserted
