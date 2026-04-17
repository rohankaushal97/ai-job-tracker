import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from serpapi import GoogleSearch
import os

KEYWORDS = ["ai", "artificial intelligence", "machine learning", "ml", "deep learning", "nlp", "computer vision", "predictive modeling", "feature engineering", "model deployment", "mlops", "ai strategy", "ai transformation", "responsible ai", "ai governance", "genai", "llm", "prompt engineering", "rag", "vector databases", "fine tuning", "automation", "intelligent automation", "data", "data science", "data analysis", "data modeling", "data visualization", "business intelligence", "analytics", "sql", "python", "statistics", "hypothesis testing", "a b testing", "forecasting", "data pipelines", "etl", "big data", "data driven decision making", "kpi tracking", "product manager", "product management", "product strategy", "product lifecycle", "product roadmap", "go to market", "gtm strategy", "user research", "user experience", "ux", "customer journey", "product analytics", "feature prioritization", "stakeholder management", "agile", "scrum", "mvp", "product discovery", "consultant", "management consulting", "business strategy", "growth strategy", "digital transformation", "operating model", "process optimization", "market entry", "competitive analysis", "benchmarking", "problem solving", "structured thinking", "client engagement", "c suite", "change management", "analyst", "business analyst", "business analysis", "requirements gathering", "process mapping", "gap analysis", "root cause analysis", "financial modeling", "excel modeling", "reporting", "insights generation", "decision support", "power bi", "tableau", "excel", "advanced excel", "google analytics", "jira", "confluence", "figma", "notion", "aws", "azure", "gcp"]
DAYS_LIMIT = 5

def fetch_linkedin_jobs():
    queries = ["Product Manager", "Associate Product Manager (APM)", "Program Manager", "Technical Program Manager (TPM)", "Business Analyst",
"Senior Business Analyst", "Data Analyst", "Machine Learning Engineer", "GenAI Engineer", "LLM Engineer",
"Prompt Engineer", "AI Product Manager", "Growth Product Manager", "Product Operations Manager", "Strategy Associate", "Strategy Consultant",
"Management Consultant", "Business Consultant", "Operations Manager", "Operations Strategy Manager", "Supply Chain Analyst", "Category Manager",
"Revenue Manager", "Pricing Analyst", "Marketing Manager", "Performance Marketing Manager", "Growth Manager", "Digital Marketing Manager",
"Brand Manager", "Sales Strategy Manager", "Account Manager (Enterprise)", "Customer Success Manager", "GTM (Go-To-Market) Manager",
"Investment Banking Analyst", "Equity Research Analyst", "Financial Analyst", "Corporate Finance Associate", "Risk Analyst", "Product Analyst",
"Data Product Manager", "Analytics Consultant", "AI/ML Consultant",  "Cloud Solutions Architect", "Solutions Consultant", "Pre-Sales Consultant",
"Techno-Functional Consultant", "ERP Consultant (SAP/Oracle)", "Cybersecurity Analyst", "MLOps Engineer", "AI Governance Specialist", 
"AI Operations (AIOps) Engineer", "AI Orchestration Specialist", "Business Intelligence (BI) Analyst", "Decision Scientist", "Experimentation Analyst (A/B Testing)",
"Startup Founder’s Office Role", "Chief of Staff (early career)", "Venture Capital Analyst", "Private Equity Analyst"    ]

    all_jobs = []

    for q in queries:
        params = {
            "engine": "google_jobs",
            "q": q,
            "location": "India",
            "api_key": os.getenv("SERPAPI_KEY")
        }

        search = GoogleSearch(params)
        results = search.get_dict()

        for job in results.get("jobs_results", []):
            all_jobs.append({
                "company": job.get("company_name", ""),
                "title": job.get("title", ""),
                "location": job.get("location", ""),
                "url": job.get("related_links", [{}])[0].get("link", ""),
                "date": job.get("detected_extensions", {}).get("posted_at", "")
            })

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

        date_str = str(date_str).lower()

        if "hour" in date_str or "today" in date_str:
            return "very recent"
        elif "day" in date_str:
            return "recent"
        elif "week" in date_str:
            return "okay"
        else:
            return "old"
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
    
    # ✅ NEW LinkedIn/Google Jobs
    jobs.extend(fetch_linkedin_jobs())

    df = pd.DataFrame(jobs)

    df = df.sort_values(by="company")

# keep max 3 jobs per company
    df = df.groupby("company").head(3)

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
    
    upload_to_sheets(df)
    
    print(f"Total jobs collected: {len(df)}")

if __name__ == "__main__":
    run()
