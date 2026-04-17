import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

KEYWORDS = ["ai", "artificial intelligence", "machine learning", "ml", "deep learning", "nlp", "computer vision", "predictive modeling", "feature engineering", "model deployment", "mlops", "ai strategy", "ai transformation", "responsible ai", "ai governance", "genai", "llm", "prompt engineering", "rag", "vector databases", "fine tuning", "automation", "intelligent automation", "data", "data science", "data analysis", "data modeling", "data visualization", "business intelligence", "analytics", "sql", "python", "statistics", "hypothesis testing", "a b testing", "forecasting", "data pipelines", "etl", "big data", "data driven decision making", "kpi tracking", "product manager", "product management", "product strategy", "product lifecycle", "product roadmap", "go to market", "gtm strategy", "user research", "user experience", "ux", "customer journey", "product analytics", "feature prioritization", "stakeholder management", "agile", "scrum", "mvp", "product discovery", "consultant", "management consulting", "business strategy", "growth strategy", "digital transformation", "operating model", "process optimization", "market entry", "competitive analysis", "benchmarking", "problem solving", "structured thinking", "client engagement", "c suite", "change management", "analyst", "business analyst", "business analysis", "requirements gathering", "process mapping", "gap analysis", "root cause analysis", "financial modeling", "excel modeling", "reporting", "insights generation", "decision support", "power bi", "tableau", "excel", "advanced excel", "google analytics", "jira", "confluence", "figma", "notion", "aws", "azure", "gcp"]
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
    print("UPLOADING TO SHEETS...")

def run():
    print("RUN STARTED")
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
    print(f"Total jobs before filtering: {len(jobs)}")
    df = pd.DataFrame(jobs)

    if df.empty:
        return
    
    print(f"Total jobs after filtering: {len(df)}")
    print(df.head())
    
    #df = df[df["date"].apply(is_recent)]
    #df = df.drop_duplicates()

    upload_to_sheets(df)
    
    print(f"Total jobs collected: {len(df)}")

if __name__ == "__main__":
    run()
