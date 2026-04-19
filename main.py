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

TARGET_COMPANIES = [
    # Indian IT Services / Consulting
    "tata", "tcs", "infosys", "wipro", "hcl", "hcltech", "tech mahindra", "lti mindtree", "persistent systems", "mphasis", "accenture", "capgemini",
    "cognizant", "deloitte", "pwc", "ey", "kpmg", "mckinsey", "bcg", "bain",

    # Big Tech / Global
    "microsoft", "google", "amazon", "amazon aws", "meta", "apple", "netflix", "oracle", "ibm", "adobe", "salesforce", "sap", "intel", "amd", "nvidia",
    "qualcomm", "cisco", "vmware", "servicenow", "atlassian", "snowflake", "databricks", "mongodb", "elastic",

    # Indian Product / SaaS
    "zoho", "freshworks", "postman", "browserstack", "chargebee", "cleartax", "coin dcx", "zerodha", "groww", "razorpay", "phonepe", "paytm", "cred",
    "meesho", "flipkart", "swiggy", "zomato", "ola", "rapido", "make my trip", "goibibo", "oyo", "redbus",
    
    # FMCG / Consumer Goods
    "hul", "hindustan unilever", "itc", "nestle", "britannia", "godrej", "marico", "dabur", "colgate palmolive", "reliance consumer products",
    "nykaa", "fsn ecommerce", "myntra", "ajio", "reliance retail", "dmart",

    # Banking / Finance / NBFC
    "hdfc bank", "icici bank", "axis bank", "kotak mahindra bank", "indusind bank", "sbi", "bajaj finance", "bajaj finserv", "lic",

    # Fintech / Global Finance
    "stripe", "visa", "mastercard", "coinbase", "revolut", "wise",

    # AI / New-age startups
    "openai", "anthropic", "hugging face", "scale ai", "perplexity", "midjourney", "turing", "gupshup", "yellow.ai", "uniphore", "sarvam ai",

    # Semiconductor / Infra / Cloud
    "arm", "red hat", "workday", "palantir",

    # Mobility / Travel
    "uber", "lyft", "airbnb", "indigo", "air india"
]

TARGET_ROLES = [
    # Product & Management
    "product manager", "associate product manager", "senior product manager", "product owner", "technical program manager", "program manager",

    # Software Engineering
    "software engineer", "software developer", "frontend engineer", "full stack engineer", 
    "systems engineer", "platform engineer", "api engineer",

    # Infrastructure / DevOps
    "devops engineer", "site reliability engineer", "sre", "cloud engineer", "infrastructure engineer",

    # Data Roles
    "data analyst", "business analyst", "analytics engineer", "data engineer", "data scientist", "senior data scientist",
    "machine learning engineer", "ai engineer", "ml engineer",

    # AI / Research
    "research scientist", "applied scientist", "ai researcher",

    # Design
    "ui ux designer", "product designer", "ux researcher",

    # Business / Strategy
    "growth manager", "product analyst", "strategy analyst", "business intelligence analyst", "consultant",

    # Specialized Tech
    "solutions architect", "enterprise architect", "quant analyst"
]

def fetch_linkedin_jobs():
    queries = []
    
    for company in TARGET_COMPANIES:
        for role in TARGET_ROLES:
            queries.append(f'site:linkedin.com/jobs "{role}" "{company}"')
        
    all_jobs = []

    for q in queries:
        print("SEARCHING:", q)

        params = {
            "engine": "google",
            "q": q + "India OR Bangalore OR Bengaluru OR Hyderabad OR Pune OR Mumbai OR Gurgaon OR Delhi OR Noida OR Chennai",
            "num": 10,
            "api_key": os.getenv("SERPAPI_KEY"),
            "tbs": "qdr:w"   # last 24 hours
        }

        search = GoogleSearch(params)
        results = search.get_dict()

        for res in results.get("organic_results", []):
            link = res.get("link", "")
            title = res.get("title", "")

            # Only keep LinkedIn job links
            if "linkedin.com/jobs" in link:
                title_clean = title.replace(" | LinkedIn", "")

                all_jobs.append({
                    "company": company.lower(),   # 👈 use QUERY company (NOT extraction)
                    "title": title_clean,
                    "location": res.get("snippet", "").lower(),
                    "url": link,
                    "date": res.get("snippet", "")
                })

    print("TOTAL LINKEDIN JOBS:", len(all_jobs))
    return all_jobs

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

    df["company"] = df["company"].astype(str).str.lower()

    df = df[df["company"].isin([c.lower() for c in TARGET_COMPANIES])]
    
    # Normalize location
    df["location"] = df["location"].astype(str).str.lower()
    
    india_keywords = [
        "india", "bangalore", "bengaluru", "hyderabad", "pune", "mumbai", "gurgaon", "delhi", "noida", "chennai"
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
