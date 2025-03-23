import asyncio
import ssl
import aiohttp
import sqlite3
import logging
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
from supabase import create_client, Client

# Load environment variables
load_dotenv()
INFOPARK_URL = os.getenv("INFOPARK_URL", "https://infopark.in/companies/job-search")
TECHNOPARK_URL = os.getenv("TECHNOPARK_URL", "https://technopark.org/api/paginated-jobs")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Database setup remains the same
def init_db():
    conn = sqlite3.connect("jobs.db")
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
            company_profile TEXT
        )
    """)
    conn.commit()
    conn.close()

async def fetch(session, url):
    try:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        async with session.get(url, ssl=ssl_context) as response:
            return await response.text()
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

async def get_infopark_job_details(session, job_link):
    # Fetch job details page
    html = await fetch(session, job_link)
    if not html:
        return "", ""
    soup = BeautifulSoup(html, "html.parser")

    # Extract job description
    description_div = soup.find("div", class_="deatil-box")
    description = description_div.get_text(strip=True) if description_div else ""

    # Construct company profile URL and fetch details
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
                # Extract company details
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

async def get_technopark_job_details(session, job_link):
    html = await fetch(session, job_link)
    if not html:
        return "", ""
    soup = BeautifulSoup(html, "html.parser")

    # Extract job description
    description_div = soup.find("div", class_="mb-4 flex w-full flex-col gap-8 pb-12 pt-10 lg:w-2/3")
    description = ""

    if description_div:
        description = description_div.get_text(separator="\n", strip=True)
        description = "\n".join([line.strip() for line in description.splitlines() if line.strip()])

    # Extract company profile
    company_profile = ""
    # Target the company info container based on provided HTML structure
    company_section = soup.find("div", class_="w-full border-b px-8 pt-8 lg:w-1/3 lg:border-r lg:border-b-0")

    if company_section:
        # Extract company name
        company_name_tag = company_section.find("a", class_="bodybold text-theme_color_1")
        company_name = company_name_tag.get_text(strip=True) if company_name_tag else "N/A"

        # Extract address
        address_tag = company_section.find("p", class_="bodysmall")
        address = address_tag.get_text(separator="\n", strip=True) if address_tag else "N/A"

        # Extract website
        website_tag = company_section.find("div", class_="pt-4 pb-4").find("a") if company_section.find("div", class_="pt-4 pb-4") else None
        website = website_tag.get("href", "N/A") if website_tag else "N/A"

        # Build company profile
        company_profile = (
            f"Company Name: {company_name}\n"
            f"Address: {address}\n"
            f"Website: {website}"
        )

    return description, company_profile

async def scrape_infopark_jobs():
    async with aiohttp.ClientSession() as session:
        page = 1
        all_jobs = []
        while True:
            url = f"{INFOPARK_URL}?page={page}"
            html = await fetch(session, url)
            if not html:
                break
            soup = BeautifulSoup(html, "html.parser")
            jobs_in_page = []
            for row in soup.select("#job-list tbody tr"):
                job_role = row.select_one("td.head").get_text(strip=True)
                company = row.select_one("td.date").get_text(strip=True)
                deadline = row.select_one("td:nth-child(3)").get_text(strip=True)
                job_link = row.select_one("td.btn-sec a")["href"] if row.select_one("td.btn-sec a") else ""
                jobs_in_page.append((company, job_role, deadline, job_link, "Infopark"))
            if not jobs_in_page:
                break
            # Fetch details for all jobs in current page
            details = await asyncio.gather(*[get_infopark_job_details(session, link) for (_, _, _, link, _) in jobs_in_page])
            for i, (company, role, deadline, link, tech_park) in enumerate(jobs_in_page):
                desc, comp_profile = details[i]
                all_jobs.append((company, role, deadline, link, tech_park, desc, comp_profile))
            page += 1
            if not soup.select_one("li.page-item a[rel='next']"):
                break
        return all_jobs

async def scrape_technopark_jobs():
    async with aiohttp.ClientSession() as session:
        page = 1
        all_jobs = []
        while True:
            url = f"{TECHNOPARK_URL}?page={page}"
            json_data = await fetch(session, url)
            if not json_data:
                break
            data = json.loads(json_data)
            if not data.get("data"):
                break
            jobs_in_page = []
            for job in data["data"]:
                company = job["company"]["company"]
                job_role = job["job_title"]
                deadline = job["closing_date"]
                job_link = f"https://technopark.org/job-details/{job['id']}"
                jobs_in_page.append((company, job_role, deadline, job_link, "Technopark"))
            # Fetch details for all jobs in current page
            details = await asyncio.gather(*[get_technopark_job_details(session, link) for (_, _, _, link, _) in jobs_in_page])
            for i, (company, role, deadline, link, tech_park) in enumerate(jobs_in_page):
                desc, comp_profile = details[i]
                all_jobs.append((company, role, deadline, link, tech_park, desc, comp_profile))
            page += 1
        return all_jobs

def save_jobs_to_db(jobs):
    conn = sqlite3.connect("jobs.db")
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO jobs
        (company, role, deadline, link, tech_park, description, company_profile)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, jobs)
    conn.commit()
    conn.close()
    logging.info(f"Saved {len(jobs)} jobs to SQLite database.")

def save_jobs_to_supabase(jobs):
    # Convert the list of jobs into a list of dictionaries
    jobs_dict_list = [
        {
            "company": job[0],
            "role": job[1],
            "deadline": job[2],
            "link": job[3],
            "tech_park": job[4],
            "description": job[5],
            "company_profile": job[6]
        }
        for job in jobs
    ]

    # Perform batch insert
    response = supabase.table('jobs').insert(jobs_dict_list).execute()

    # Inspect the response object
    if hasattr(response, 'error'):
        logging.error(f"Failed to insert jobs into Supabase: {response.error}")
    else:
        logging.info(f"Saved {len(jobs)} jobs to Supabase.")

async def main():
    init_db()
    infopark_jobs, technopark_jobs = await asyncio.gather(
        scrape_infopark_jobs(), scrape_technopark_jobs()
    )
    all_jobs = infopark_jobs + technopark_jobs
    if all_jobs:
        save_jobs_to_db(all_jobs)
        save_jobs_to_supabase(all_jobs)
    else:
        logging.info("No jobs found.")

if __name__ == "__main__":
    asyncio.run(main())
