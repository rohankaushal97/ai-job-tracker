import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

KEYWORDS = [
    "ai", "artificial intelligence", "machine learning",
    "ml", "data", "analytics",
    "product manager", "product",
    "consultant", "strategy",
    "analyst", "business analyst",
    "genai", "llm"
]
DAYS_LIMIT = 5

def is_recent(date_str):
    try:
        posted = datetime.fromisoformat(date_str.replace("Z", ""))
        return posted > datetime.now() - timedelta(days=DAYS_LIMIT)
    except:
        return False

def match_keywords(text):
    return any(k in text.lower() for k in KEYWORDS)

def fetch_greenhouse(company):
    url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"
    res = requests.get(url).json()
    jobs = []
    for job in res.get("jobs", []):
        if match_keywords(job["title"]):
            jobs.append({
                "company": company,
                "title": job["title"],
                "location": job["location"]["name"],
                "url": job["absolute_url"],
                "date": job["updated_at"]
            })
    return jobs

def fetch_lever(company):
    url = f"https://api.lever.co/v0/postings/{company}?mode=json"
    
    try:
        res = requests.get(url)
        data = res.json()
    except:
        return []

    jobs = []

    # ✅ safety check
    if not isinstance(data, list):
        return []

    for job in data:
        # ✅ skip bad entries
        if not isinstance(job, dict):
            continue

        title = job.get("text", "")

        if True:
            jobs.append({
                "company": company,
                "title": title,
                "location": job.get("categories", {}).get("location", ""),
                "url": job.get("hostedUrl", ""),
                "date": str(job.get("createdAt", ""))
            })

    return jobs

def upload_to_sheets(df):
    creds_json = os.getenv("GOOGLE_CREDS")
    with open("creds.json", "w") as f:
        f.write(creds_json)

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
    client = gspread.authorize(creds)

    sheet = client.open("AI Job Tracker").sheet1
    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())

def run():
    companies_greenhouse = [
    "openai",
    "stripe",
    "airbnb",
    "robinhood",
    "discord",
    "notion",
    "pinterest",
    "coinbase",
    "dropbox",
    "shopify",
    "canva",
    "instacart",
    "databricks",
    "snowflake",
    "plaid",
    "brex",
    "zapier",
    "razorpay",
    "cred",
    "meesho",
    "groww",
    "swiggy",
    "urbancompany"
]
    companies_lever = [
    "figma",
    "netflix",
    "atlassian",
    "udemy",
    "robinhood",
    "asana",
    "circle",
    "scaleai",
    "rippling",
    "segment",
    "hashicorp",
    "postman",
    "sharechat",
    "browserstack",
    "instahyre"
]

    jobs = []

    for c in companies_greenhouse:
        jobs.extend(fetch_greenhouse(c))

    for c in companies_lever:
        jobs.extend(fetch_lever(c))

    df = pd.DataFrame(jobs)

    if df.empty:
        return

    df = df[df["date"].apply(is_recent)]
    df = df.drop_duplicates()

    upload_to_sheets(df)
    print(f"Total jobs collected: {len(df)}")

if __name__ == "__main__":
    run()
