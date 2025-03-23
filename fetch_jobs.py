import asyncio
import ssl
import aiohttp
import sqlite3
import logging
import os
import re
import json
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from supabase import create_client, Client
from aiohttp import ClientSession, ClientTimeout
from typing import Tuple, List, Dict, Optional, Any, Union

# Load environment variables
load_dotenv()
INFOPARK_URL = os.getenv("INFOPARK_URL", "https://infopark.in/companies/job-search")
TECHNOPARK_URL = os.getenv("TECHNOPARK_URL", "https://technopark.org/api/paginated-jobs")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DB_FILE = "jobs.db"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Database setup
def init_db() -> None:
    """Initialize SQLite database with jobs table if it doesn't exist."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company TEXT,
            role TEXT,
            deadline TEXT,
            link TEXT,
            tech_park TEXT,
            description TEXT,
            company_profile TEXT,
            email TEXT
        )
    """)
    conn.commit()
    conn.close()

async def fetch(session: ClientSession, url: str, timeout: int = 30) -> Optional[str]:
    """Fetch URL content with SSL context and timeout handling."""
    try:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        async with session.get(url, ssl=ssl_context, timeout=timeout) as response:
            response.raise_for_status()
            return await response.text()
    except asyncio.TimeoutError:
        logging.error(f"Timeout error fetching {url}")
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
    return None

def extract_emails(text: str) -> List[str]:
    """Extract email addresses from text using regex pattern."""
    email_pattern = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
    return re.findall(email_pattern, text)

async def get_infopark_job_details(session: ClientSession, job_link: str) -> Tuple[str, str]:
    """Scrape job details and company profile from Infopark job page."""
    html = await fetch(session, job_link)
    if not html:
        return "", ""
    
    soup = BeautifulSoup(html, "html.parser")
    description_div = soup.find("div", class_="deatil-box")
    description = description_div.get_text(strip=True) if description_div else ""

    company_id = job_link.split('/')[-1]
    company_profile_url = f"https://infopark.in/companies/profile/{company_id}"
    company_html = await fetch(session, company_profile_url)
    company_profile = ""

    if company_html:
        company_soup = BeautifulSoup(company_html, "html.parser")
        carer_box = company_soup.find("div", class_="carer-box")

        if carer_box:
            con_div = carer_box.find("div", class_="con")
            if con_div:
                name = con_div.find("h4").get_text(strip=True) if con_div.find("h4") else ""
                spans = con_div.find_all("span", recursive=False)

                address = phone = email = website = ""
                if len(spans) > 0:
                    address = spans[0].get_text(separator="\n", strip=True)
                if len(spans) > 1:
                    phone = spans[1].get_text(strip=True)
                if len(spans) > 2:
                    email = spans[2].get_text(strip=True)
                if len(spans) > 3:
                    website_anchor = spans[3].find("a")
                    website = website_anchor.get_text(strip=True) if website_anchor else spans[3].get_text(strip=True)

                company_profile = (
                    f"Company Name: {name}\n"
                    f"Address: {address}\n"
                    f"Phone: {phone}\n"
                    f"Email: {email}\n"
                    f"Website: {website}"
                )

    return description, company_profile

async def get_technopark_job_details(session: ClientSession, job_link: str) -> Tuple[str, str, str]:
    """Scrape job details, company profile, and email from Technopark job page."""
    html = await fetch(session, job_link)
    if not html:
        return "", "", ""
    
    soup = BeautifulSoup(html, "html.parser")
    description_div = soup.find("div", class_="mb-4 flex w-full flex-col gap-8 pb-12 pt-10 lg:w-2/3")
    description = ""

    if description_div:
        description = description_div.get_text(separator="\n", strip=True)
        description = "\n".join([line.strip() for line in description.splitlines() if line.strip()])

    company_section = soup.find("div", class_="w-full border-b px-8 pt-8 lg:w-1/3 lg:border-r lg:border-b-0")
    company_profile = ""

    if company_section:
        company_name_tag = company_section.find("a", class_="bodybold text-theme_color_1")
        company_name = company_name_tag.get_text(strip=True) if company_name_tag else "N/A"

        address_tag = company_section.find("p", class_="bodysmall")
        address = address_tag.get_text(separator="\n", strip=True) if address_tag else "N/A"

        website_tag = company_section.find("div", class_="pt-4 pb-4").find("a") if company_section.find("div", class_="pt-4 pb-4") else None
        website = website_tag.get("href", "N/A") if website_tag else "N/A"

        company_profile = (
            f"Company Name: {company_name}\n"
            f"Address: {address}\n"
            f"Website: {website}"
        )

    # Extract email directly
    email = ""
    a_tag = soup.find("a", href=lambda href: href and href.startswith("mailto:"))
    if a_tag:
        email = a_tag.get_text(strip=True)
        if not email:
            email = a_tag["href"].replace("mailto:", "").strip()

    return description, company_profile, email

async def scrape_jobs(base_url: str, tech_park: str, max_concurrent_requests: int = 10) -> List[Tuple]:
    """Scrape jobs from the specified tech park."""
    async with aiohttp.ClientSession(timeout=ClientTimeout(total=30)) as session:
        page = 1
        all_jobs = []
        semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        while True:
            url = f"{base_url}?page={page}"
            html_or_json = await fetch(session, url)
            if not html_or_json:
                break

            jobs_in_page = []
            
            if tech_park == "Infopark":
                soup = BeautifulSoup(html_or_json, "html.parser")
                for row in soup.select("#job-list tbody tr"):
                    job_role = row.select_one("td.head").get_text(strip=True)
                    company = row.select_one("td.date").get_text(strip=True)
                    deadline = row.select_one("td:nth-child(3)").get_text(strip=True)
                    job_link = row.select_one("td.btn-sec a")["href"] if row.select_one("td.btn-sec a") else ""
                    jobs_in_page.append((company, job_role, deadline, job_link))
                
                # Check if there's a next page
                has_next_page = bool(soup.select_one("li.page-item a[rel='next']"))
                
            elif tech_park == "Technopark":
                data = json.loads(html_or_json)
                if not data.get("data"):
                    break
                
                for job in data["data"]:
                    company = job["company"]["company"]
                    job_role = job["job_title"]
                    deadline = job["closing_date"]
                    job_link = f"https://technopark.org/job-details/{job['id']}"
                    jobs_in_page.append((company, job_role, deadline, job_link))
                
                # Check if there's a next page based on pagination data
                has_next_page = data.get("current_page", 0) < data.get("last_page", 0)
            
            if not jobs_in_page:
                break

            tasks = []
            for company, role, deadline, link in jobs_in_page:
                if tech_park == "Infopark":
                    task = asyncio.create_task(get_infopark_job_details(session, link))
                else:  # Technopark
                    task = asyncio.create_task(get_technopark_job_details(session, link))
                tasks.append((company, role, deadline, link, task))

            for company, role, deadline, link, task in tasks:
                if tech_park == "Infopark":
                    desc, comp_profile = await task
                    email = extract_emails(desc)[0] if extract_emails(desc) else ""
                else:  # Technopark
                    desc, comp_profile, email = await task
                
                all_jobs.append((company, role, deadline, link, tech_park, desc, comp_profile, email))
            
            page += 1
            if not has_next_page:
                break
                
        return all_jobs

def save_jobs_to_db(jobs: List[Tuple]) -> None:
    """Save jobs to SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO jobs
        (company, role, deadline, link, tech_park, description, company_profile, email)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, jobs)
    conn.commit()
    conn.close()
    logging.info(f"Saved {len(jobs)} jobs to SQLite database.")

def save_jobs_to_supabase(jobs: List[Tuple]) -> None:
    """Save jobs to Supabase database."""
    jobs_dict_list = [
        {
            "company": job[0],
            "role": job[1],
            "deadline": job[2],
            "link": job[3],
            "tech_park": job[4],
            "description": job[5],
            "company_profile": job[6],
            "email": job[7]
        }
        for job in jobs
    ]

    response = supabase.table('jobs').insert(jobs_dict_list).execute()

    if hasattr(response, 'error') and response.error:
        logging.error(f"Failed to insert jobs into Supabase: {response.error}")
    else:
        logging.info(f"Saved {len(jobs)} jobs to Supabase.")

def get_jobs_missing_email() -> List[Tuple]:
    """Retrieve all jobs where email is missing or empty."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT company, role, deadline, link, tech_park, description, company_profile, email
        FROM jobs
        WHERE (email IS NULL OR TRIM(email) = '')
    """)
    jobs = cursor.fetchall()
    conn.close()
    return jobs

async def update_missing_emails() -> None:
    """Update jobs with missing emails."""
    jobs_missing_email = get_jobs_missing_email()
    if not jobs_missing_email:
        logging.info("No jobs with missing emails found.")
        return

    async with aiohttp.ClientSession(timeout=ClientTimeout(total=30)) as session:
        semaphore = asyncio.Semaphore(10)
        tasks = []
        
        for job in jobs_missing_email:
            job_link = job[3]
            tech_park = job[4]
            
            async def process_job(job_link, tech_park):
                async with semaphore:
                    if tech_park == "Infopark":
                        html = await fetch(session, job_link)
                        if html:
                            desc, _ = await get_infopark_job_details(session, job_link)
                            email = extract_emails(desc)[0] if extract_emails(desc) else ""
                    else:  # Technopark
                        _, _, email = await get_technopark_job_details(session, job_link)
                    
                    if email:
                        # Update in SQLite
                        conn = sqlite3.connect(DB_FILE)
                        cursor = conn.cursor()
                        cursor.execute("UPDATE jobs SET email = ? WHERE link = ?", (email, job_link))
                        conn.commit()
                        conn.close()
                        
                        # Update in Supabase
                        supabase.table('jobs').update({"email": email}).match({"link": job_link}).execute()
                        logging.info(f"Updated job: {job_link} with email: {email}")
            
            tasks.append(asyncio.create_task(process_job(job_link, tech_park)))
        
        await asyncio.gather(*tasks)

async def main() -> None:
    """Main function to scrape and update jobs."""
    init_db()
    
    # Scrape new jobs
    infopark_jobs = await scrape_jobs(INFOPARK_URL, "Infopark")
    technopark_jobs = await scrape_jobs(TECHNOPARK_URL, "Technopark")
    all_jobs = infopark_jobs + technopark_jobs

    if all_jobs:
        save_jobs_to_db(all_jobs)
        save_jobs_to_supabase(all_jobs)
        logging.info(f"Scraped and saved {len(all_jobs)} jobs in total.")
    else:
        logging.info("No new jobs found.")
    
    # Update missing emails
    await update_missing_emails()

if __name__ == "__main__":
    asyncio.run(main())