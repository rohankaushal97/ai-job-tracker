import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from serpapi import GoogleSearch
import os
import re


KEYWORDS = ["ai", "artificial intelligence", "machine learning", "ml", "deep learning", "nlp", "computer vision", "predictive modeling", "feature engineering", "model deployment", "mlops", "ai strategy", "ai transformation", "responsible ai", "ai governance", "genai", "llm", "prompt engineering", "rag", "vector databases", "fine tuning", "automation", "intelligent automation", "data", "data science", "data analysis", "data modeling", "data visualization", "business intelligence", "analytics", "sql", "python", "statistics", "hypothesis testing", "a b testing", "forecasting", "data pipelines", "etl", "big data", "data driven decision making", "kpi tracking", "product manager", "product management", "product strategy", "product lifecycle", "product roadmap", "go to market", "gtm strategy", "user research", "user experience", "ux", "customer journey", "product analytics", "feature prioritization", "stakeholder management", "agile", "scrum", "mvp", "product discovery", "consultant", "management consulting", "business strategy", "growth strategy", "digital transformation", "operating model", "process optimization", "market entry", "competitive analysis", "benchmarking", "problem solving", "structured thinking", "client engagement", "c suite", "change management", "analyst", "business analyst", "business analysis", "requirements gathering", "process mapping", "gap analysis", "root cause analysis", "financial modeling", "excel modeling", "reporting", "insights generation", "decision support", "power bi", "tableau", "excel", "advanced excel", "google analytics", "jira", "confluence", "figma", "notion", "aws", "azure", "gcp"]
DAYS_LIMIT = 5

def extract_company(title):
    title = title.replace(" | LinkedIn", "").strip()

    if " - " in title:
        return title.split(" - ")[0].strip().lower()

    if " at " in title.lower():
        return title.lower().split(" at ")[-1].strip()

    return "unknown"

def fetch_linkedin_jobs():
    queries = [
"site:linkedin.com/jobs product manager", "site:linkedin.com/jobs associate product manager", "site:linkedin.com/jobs technical program manager",
"site:linkedin.com/jobs business analyst", "site:linkedin.com/jobs data analyst", "site:linkedin.com/jobs product analyst", "site:linkedin.com/jobs machine learning engineer",
"site:linkedin.com/jobs GenAI engineer", "site:linkedin.com/jobs LLM engineer", "site:linkedin.com/jobs AI product manager", "site:linkedin.com/jobs data product manager",
"site:linkedin.com/jobs MLOps engineer", "site:linkedin.com/jobs business intelligence analyst", "site:linkedin.com/jobs decision scientist", "site:linkedin.com/jobs strategy consultant",
"site:linkedin.com/jobs management consultant", "site:linkedin.com/jobs business consultant", "site:linkedin.com/jobs analytics consultant", "site:linkedin.com/jobs AI ML consultant",
"site:linkedin.com/jobs operations manager", "site:linkedin.com/jobs supply chain analyst",  "site:linkedin.com/jobs marketing manager", "site:linkedin.com/jobs growth manager",
"site:linkedin.com/jobs digital marketing manager", "site:linkedin.com/jobs performance marketing manager", "site:linkedin.com/jobs investment banking analyst",
"site:linkedin.com/jobs equity research analyst", "site:linkedin.com/jobs financial analyst", "site:linkedin.com/jobs venture capital analyst", "site:linkedin.com/jobs private equity analyst"
]

    all_jobs = []

    for q in queries:
        print("SEARCHING:", q)

        params = {
            "engine": "google",   # ✅ IMPORTANT CHANGE
            "q": q,
            "num": 20,
            "api_key": os.getenv("SERPAPI_KEY"),
            "tbs": "qdr:w"   # ✅ last 24 hours
        }

        search = GoogleSearch(params)
        results = search.get_dict()

        for res in results.get("organic_results", []):
            link = res.get("link", "")
            title = res.get("title", "")

            # Only keep LinkedIn job links
            if "linkedin.com/jobs" in link:
                title_clean = title.replace(" | LinkedIn", "")
                company_name = extract_company(title_clean)
                
                all_jobs.append({
                    "company": company_name,
                    "title": title_clean,
                    "location": res.get("snippet", "").lower(),
                    "url": link,
                    "date": res.get("snippet", "")
                })

    print("TOTAL LINKEDIN JOBS:", len(all_jobs))
    return all_jobs
    
"""def is_recent(date_str):
    try:
        if not date_str:
            return True  # keep if unknown

        date_str = str(date_str).lower()

        if "day" in date_str:
            return True
        if "hour" in date_str:
            return True
        if "today" in date_str or "just posted" in date_str:
            return True

        return False
    except:
        return True"""

def mark_recent(date_str):
    try:
        if not date_str:
            return "unknown"

        text = str(date_str).lower()

        # ✅ Handle "X days/weeks ago"
        match = re.search(r"(\d+)\s*(hour|day|week|month)", text)
        if match:
            value = int(match.group(1))
            unit = match.group(2)

            if unit == "hour":
                return "very recent"
            elif unit == "day":
                if value <= 1:
                    return "very recent"
                elif value <= 3:
                    return "recent"
                elif value <= 7:
                    return "okay"
                else:
                    return "old"
            elif unit == "week":
                if value == 1:
                    return "okay"
                else:
                    return "old"
            elif unit == "month":
                return "old"

        # ✅ Handle "today"
        if "today" in text:
            return "very recent"

        # ✅ Handle ISO timestamps
        try:
            dt = datetime.fromisoformat(text.replace("z", "+00:00"))
            now = datetime.now(timezone.utc)
            diff = now - dt

            if diff.days <= 1:
                return "very recent"
            elif diff.days <= 3:
                return "recent"
            elif diff.days <= 7:
                return "okay"
            else:
                return "old"
        except:
            pass

        return "unknown"

    except:
        return "unknown"
        
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
                "title": title.replace(" | LinkedIn", ""),
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
    companies_greenhouse = [ "openai",    "stripe",    "airbnb",    "robinhood",    "discord",    "notion",    "pinterest",    "coinbase",    "dropbox",
    "shopify",    "canva",    "instacart",    "databricks",    "snowflake",    "plaid",    "brex",    "zapier",    "razorpay",    "cred",    "meesho",
    "groww",    "swiggy",    "urbancompany"]

    companies_lever = [
    "figma",    "netflix",    "atlassian",    "udemy",    "robinhood",    "asana",    "circle",    "scaleai",    "rippling",    "segment",
    "hashicorp",    "postman",    "sharechat",    "browserstack",    "instahyre"]

    jobs = []
    
    for c in companies_greenhouse:
        jobs.extend(fetch_greenhouse(c))

    for c in companies_lever:
        jobs.extend(fetch_lever(c))
        
    print(f"Total jobs before filtering: {len(jobs)}")
    
    # ✅ NEW LinkedIn/Google Jobs
    linkedin_jobs = fetch_linkedin_jobs()
    print("LinkedIn jobs:", len(linkedin_jobs))
    jobs.extend(linkedin_jobs)

    df = pd.DataFrame(jobs)

    # Normalize location
    df["location"] = df["location"].astype(str).str.lower()
    
    india_keywords = [
        "india", "bangalore", "bengaluru", "hyderabad",
        "pune", "mumbai", "gurgaon", "delhi", "noida", "chennai", "remote"
    ]
    
    df = df[df["location"].apply(lambda x: any(k in x for k in india_keywords))]

    """df["company"] = df["company"].astype(str).str.lower()

    target_companies = [
        "tata", "wipro", "hcl", "tech mahindra",
        "accenture", "capgemini", "cognizant",
        "ibm", "oracle", "microsoft", "google",
        "amazon", "adobe", "salesforce", "sap",
        "intel", "qualcomm", "nvidia", "cisco",
        "flipkart", "meesho", "swiggy", "zomato",
        "paytm", "razorpay", "phonepe",
        "zoho", "freshworks",
        "cred", "groww", "zerodha",
        "mckinsey", "bcg", "bain", "deloitte",
        "pwc", "ey", "kpmg"
    ]

    df = df[df["company"].apply(lambda x: any(c in x for c in target_companies) if x else False)]"""
        
    if df.empty:
        return
    
    print(f"Total jobs after filtering: {len(df)}")
    print(df.head())

    df = df.drop_duplicates()
    df["recency"] = df["date"].apply(mark_recent)
    #df = df[df["date"].apply(is_recent)]

    # Create recency score
    df["recency_score"] = df["recency"].map({
        "very recent": 3,
        "recent": 2,
        "okay": 1,
        "old": 0,
        "unknown": 1
    })

    # Sort by best jobs first
    df = df.sort_values(by="recency_score", ascending=False)
    
    # Keep top 3 jobs per company
    df = df.groupby("company").head(3)

    
    import numpy as np
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.replace([np.inf, -np.inf], "")
    df = df.fillna("")
    df = df.astype(str)
    
    upload_to_sheets(df)
    
    print(f"Total jobs collected: {len(df)}")

if __name__ == "__main__":
    run()
