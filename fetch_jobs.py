import asyncio
import ssl
import aiohttp
import mysql.connector
import logging
import os
import re
import time
import json
import feedparser
from urllib.parse import urljoin
from datetime import datetime
from dateutil.parser import parse as parse_date
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from aiohttp import ClientSession, ClientTimeout
from typing import Tuple, List, Optional, Set
from google import genai
from google.genai import types

# Load environment variables
load_dotenv()

# Configuration
INFOPARK_URL = os.getenv("INFOPARK_URL", "https://infopark.in/companies/job-search")
TECHNOPARK_URL = os.getenv("TECHNOPARK_URL", "https://technopark.org/api/paginated-jobs")
UL_URL = "https://www.ulcyberpark.com/jobs/index"
CYBERPARK_RSS_URL = "https://www.cyberparkkerala.org/?feed=job_feed"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Database Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "kljobs_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "PX#lGJi5D68lH@")
DB_NAME = os.getenv("DB_NAME", "kljobs_db")

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_db_connection():
    """Establish a connection to the MySQL database."""
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def init_db() -> None:
    """Initialize MySQL database and table. Retries connection if DB is not ready."""
    logging.info(f"Initializing database {DB_NAME} on {DB_HOST}...")
    
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            # Connect to MySQL server first to create DB if needed
            conn = mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASSWORD
            )
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
            conn.close()

            # Now connect to the specific database
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Create table with UNIQUE constraint on link
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    company TEXT,
                    role TEXT,
                    deadline TEXT,
                    link VARCHAR(255) UNIQUE,
                    tech_park TEXT,
                    description MEDIUMTEXT,
                    company_profile TEXT,
                    email TEXT,
                    cleaned_data JSON,
                    is_cleaned BOOLEAN DEFAULT FALSE
                )
            """)
            
            # Check if columns exist (for migration)
            cursor.execute("SHOW COLUMNS FROM jobs LIKE 'cleaned_data'")
            if not cursor.fetchone():
                cursor.execute("ALTER TABLE jobs ADD COLUMN cleaned_data JSON")
                cursor.execute("ALTER TABLE jobs ADD COLUMN is_cleaned BOOLEAN DEFAULT FALSE")
                logging.info("Added 'cleaned_data' and 'is_cleaned' columns.")

            logging.info("Database and table 'jobs' checked/initialized.")
            conn.commit()
            conn.close()
            return # Success
        except mysql.connector.Error as err:
            logging.warning(f"Database connection attempt {attempt + 1}/{max_retries} failed: {err}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.error("Max retries reached. Could not connect to database.")
                raise

def get_existing_links() -> Set[str]:
    """Fetch all existing job links from the database to avoid re-scraping."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT link FROM jobs")
        links = {row[0] for row in cursor.fetchall()}
        conn.close()
        logging.info(f"Loaded {len(links)} existing jobs from database.")
        return links
    except mysql.connector.Error:
        logging.warning("Could not fetch existing links (DB might be empty or unreachable). Proceeding with full scrape.")
        return set()

def format_description(description: str) -> str:
    """Normalize whitespace in the job description."""
    return re.sub(r'\s+', ' ', description).strip()

def is_deadline_in_future(deadline: str) -> bool:
    """
    Try to parse the deadline and return True if it's in the future.
    If parsing fails, assume the deadline is valid.
    """
    try:
        # Attempt to parse the deadline string.
        deadline_date = parse_date(deadline)
        return deadline_date >= datetime.now()
    except Exception as e:
        # logging.warning(f"Could not parse deadline '{deadline}'. Assuming it's valid.")
        return True

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
    """Extract email addresses using a regex pattern."""
    email_pattern = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
    return re.findall(email_pattern, text)

async def get_infopark_job_details(session: ClientSession, job_link: str) -> Tuple[str, str]:
    """Scrape job details and company profile from Infopark job page."""
    html = await fetch(session, job_link)
    if not html:
        return "", ""
    
    soup = BeautifulSoup(html, "html.parser")
    description_div = soup.find("div", class_="deatil-box")
    # Use separator to keep paragraphs distinct
    description = description_div.get_text(separator="\n", strip=True) if description_div else ""

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
                
                # improved extraction
                spans = con_div.find_all("span", recursive=False)
                address = ""
                phone = ""
                email = ""
                website = ""

                # Usually: [0] Address, [1] Phone, [2] Email, [3] Website
                # But we should be careful.
                if len(spans) > 0:
                    address = spans[0].get_text(separator=" ", strip=True)
                if len(spans) > 1:
                    phone = spans[1].get_text(strip=True)
                if len(spans) > 2:
                    email = spans[2].get_text(strip=True)
                if len(spans) > 3:
                    website_anchor = spans[3].find("a")
                    website = website_anchor.get_text(strip=True) if website_anchor else spans[3].get_text(strip=True)
                
                # Basic cleanup: If address contains "Contacts", it might be dirty.
                # But the main fix is using separators above.
                
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
    
    email = ""
    a_tag = soup.find("a", href=lambda href: href and href.startswith("mailto:"))
    if a_tag:
        email = a_tag.get_text(strip=True)
        if not email:
            email = a_tag["href"].replace("mailto:", "").strip()

    return description, company_profile, email

async def scrape_ul_jobs(existing_links: Set[str]) -> List[Tuple]:
    """Scrape jobs from UL Cyberpark."""
    logging.info("Started scraping jobs from UL Cyberpark...")
    all_jobs = []
    
    async with aiohttp.ClientSession(timeout=ClientTimeout(total=30)) as session:
        current_url = UL_URL
        while current_url:
            logging.info(f"Fetching {current_url}")
            html = await fetch(session, current_url)
            if not html:
                break
            
            soup = BeautifulSoup(html, 'html.parser')
            table_div = soup.find('div', class_='table-responsive-sm table-job')
            if not table_div:
                break
            table = table_div.find('table', class_='table')
            if not table:
                break

            for row in table.find_all('tr'):
                tds = row.find_all('td')
                if len(tds) < 3:
                    continue
                
                # Extract details
                # For link, we prefer the 'Details' page as it is unique to the job
                details_elem = tds[2].find('a')
                link = details_elem.get('href', 'N/A') if details_elem else 'N/A'
                if link != 'N/A' and not link.startswith('http'):
                    link = urljoin(UL_URL, link)
                
                if link in existing_links:
                    continue

                job_title_elem = tds[0].find('a', class_='btn-1')
                role = job_title_elem.text.strip() if job_title_elem else 'N/A'
                
                closing_date_elem = tds[0].find('span')
                deadline = 'N/A'
                if closing_date_elem:
                    deadline = closing_date_elem.text.split('closing date: ')[-1].strip()
                
                company_elem = tds[1].find('a', class_='btn-1')
                company = company_elem.text.strip() if company_elem else 'N/A'
                
                # Placeholder for now, could fetch deep details if needed
                description = f"Job at {company}. See link for details."
                company_profile = f"Company: {company}"
                email = ""

                if is_deadline_in_future(deadline):
                    all_jobs.append((company, role, deadline, link, "UL Cyberpark", description, company_profile, email))
            
            # Pagination
            next_link = None
            pagination = soup.find(lambda tag: tag.name in ['ul', 'section'] and 
                                     tag.get('class') and any('pagination' in cls for cls in tag.get('class')))
            if pagination:
                next_a = pagination.find('a', rel='next')
                if not next_a:
                    active_li = pagination.find('li', class_='active')
                    if active_li:
                        next_li = active_li.find_next_sibling('li')
                        if next_li:
                            next_a = next_li.find('a')
                
                if next_a and next_a.get('href'):
                    next_link = next_a['href']
                    if not next_link.startswith('http'):
                        next_link = urljoin(UL_URL, next_link)
            
            current_url = next_link
            
    logging.info(f"Finished scraping jobs from UL Cyberpark. Found {len(all_jobs)} new jobs.")
    return all_jobs

def scrape_cyberpark_rss(existing_links: Set[str]) -> List[Tuple]:
    """Scrape jobs from Cyberpark Kerala RSS Feed."""
    logging.info("Started scraping jobs from Cyberpark RSS...")
    all_jobs = []
    try:
        feed = feedparser.parse(CYBERPARK_RSS_URL)
        for entry in feed.entries:
            link = entry.link
            if link in existing_links:
                continue

            role = entry.title
            company = "Cyberpark Company" # Placeholder, sometimes in title or summary
            deadline = entry.published if hasattr(entry, 'published') else "N/A"
            description = entry.summary if hasattr(entry, 'summary') else ""
            
            # Try to extract cleaner company name from title if " - " exists
            if " - " in role:
                parts = role.split(" - ")
                if len(parts) >= 2:
                    role = parts[0].strip()
                    company = parts[1].strip()
            
            # Basic cleanup of HTML in summary for description
            clean_desc = BeautifulSoup(description, "html.parser").get_text(separator="\n", strip=True)
            
            company_profile = f"Company: {company}"
            email = ""
            
            # RSS items are usually recent, assume valid deadline or unknown
            all_jobs.append((company, role, deadline, link, "Cyberpark Kozhikode", clean_desc, company_profile, email))
            
    except Exception as e:
        logging.error(f"Error parsing RSS feed: {e}")
        
    logging.info(f"Finished scraping jobs from Cyberpark RSS. Found {len(all_jobs)} new jobs.")
    return all_jobs

async def scrape_jobs(base_url: str, tech_park: str, existing_links: Set[str], max_concurrent_requests: int = 10) -> List[Tuple]:
    """Scrape jobs from the specified tech park."""
    logging.info(f"Started scraping jobs from {tech_park}...")
    async with aiohttp.ClientSession(timeout=ClientTimeout(total=30)) as session:
        page = 1
        all_jobs = []
        semaphore = asyncio.Semaphore(max_concurrent_requests)
        while True:
            url = f"{base_url}?page={page}"
            logging.info(f"Fetching {url}")
            html_or_json = await fetch(session, url)
            if not html_or_json:
                break
            jobs_in_page = []
            if tech_park == "Infopark":
                soup = BeautifulSoup(html_or_json, "html.parser")
                rows = soup.select("#job-list tbody tr")
                if not rows:
                    break
                for row in rows:
                    job_link = row.select_one("td.btn-sec a")["href"] if row.select_one("td.btn-sec a") else ""
                    if job_link in existing_links:
                        continue
                        
                    job_role = row.select_one("td.head").get_text(strip=True)
                    company = row.select_one("td.date").get_text(strip=True)
                    deadline = row.select_one("td:nth-child(3)").get_text(strip=True)
                    jobs_in_page.append((company, job_role, deadline, job_link))
                has_next_page = bool(soup.select_one("li.page-item a[rel='next']"))
            elif tech_park == "Technopark":
                data = json.loads(html_or_json)
                if not data.get("data"):
                    break
                for job in data["data"]:
                    job_link = f"https://technopark.org/job-details/{job['id']}"
                    if job_link in existing_links:
                        continue
                    
                    company = job["company"]["company"]
                    job_role = job["job_title"]
                    deadline = job["closing_date"]
                    jobs_in_page.append((company, job_role, deadline, job_link))
                has_next_page = data.get("current_page", 0) < data.get("last_page", 0)
            
            # Optimization: Stop if all jobs on current page are duplicates (and not page 1)
            if not jobs_in_page:
                if page > 1:
                    logging.info(f"All jobs on page {page} are duplicates. Stopping scrape for {tech_park}.")
                    break
                if page == 1 and html_or_json:
                    logging.info(f"All jobs on page 1 are duplicates. No new jobs at {tech_park}.")
                    break

            tasks = []
            for company, role, deadline, link in jobs_in_page:
                if tech_park == "Infopark":
                    task = asyncio.create_task(get_infopark_job_details(session, link))
                else:
                    task = asyncio.create_task(get_technopark_job_details(session, link))
                tasks.append((company, role, deadline, link, task))
            for company, role, deadline, link, task in tasks:
                if tech_park == "Infopark":
                    desc, comp_profile = await task
                    email = extract_emails(desc)[0] if extract_emails(desc) else ""
                else:
                    desc, comp_profile, email = await task
                formatted_desc = format_description(desc)
                # Only add jobs with deadlines in the future
                if is_deadline_in_future(deadline):
                    all_jobs.append((company, role, deadline, link, tech_park, formatted_desc, comp_profile, email))
                else:
                    logging.info(f"Skipping job {link} as the deadline has passed: {deadline}")
            page += 1
            if not has_next_page:
                break
        logging.info(f"Finished scraping jobs from {tech_park}.")
        return all_jobs

def save_jobs_to_db(jobs: List[Tuple]) -> None:
    """Save jobs to MySQL database using INSERT IGNORE to avoid duplicates."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # MySQL uses %s for placeholders
        query = """
            INSERT IGNORE INTO jobs
            (company, role, deadline, link, tech_park, description, company_profile, email)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(query, jobs)
        conn.commit()
        logging.info(f"Saved {cursor.rowcount} new jobs to MySQL database.")
        conn.close()
    except mysql.connector.Error as err:
        logging.error(f"Error saving jobs to database: {err}")

async def update_missing_emails() -> None:
    """Update jobs with missing emails."""
    logging.info("Starting update of missing emails...")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT company, role, deadline, link, tech_park, description, company_profile, email
            FROM jobs
            WHERE (email IS NULL OR TRIM(email) = '')
        """)
        jobs_missing_email = cursor.fetchall()
        conn.close()
    except mysql.connector.Error as err:
        logging.error(f"Error fetching jobs for email update: {err}")
        return

    if not jobs_missing_email:
        logging.info("No jobs with missing emails found.")
        return
    async with aiohttp.ClientSession(timeout=ClientTimeout(total=30)) as session:
        semaphore = asyncio.Semaphore(10)
        tasks = []
        for job in jobs_missing_email:
            # MySQL result is a tuple, indexes should be same as select
            job_link = job[3]
            tech_park = job[4]
            # Skip email update for UL/RSS for now as they don't have detail scrapers set up in this specific function yet
            if tech_park not in ["Infopark", "Technopark"]:
                continue
                
            async def process_job(job_link, tech_park):
                async with semaphore:
                    if tech_park == "Infopark":
                        html = await fetch(session, job_link)
                        if html:
                            desc, _ = await get_infopark_job_details(session, job_link)
                            email = extract_emails(desc)[0] if extract_emails(desc) else ""
                    else:
                        _, _, email = await get_technopark_job_details(session, job_link)
                    if email:
                        try:
                            conn = get_db_connection()
                            cursor = conn.cursor()
                            cursor.execute("UPDATE jobs SET email = %s WHERE link = %s", (email, job_link))
                            conn.commit()
                            conn.close()
                            logging.info(f"Updated job: {job_link} with email: {email}")
                        except Exception as e:
                            logging.error(f"Error updating email for {job_link}: {e}")
            tasks.append(asyncio.create_task(process_job(job_link, tech_park)))
        await asyncio.gather(*tasks)
    logging.info("Finished updating missing emails.")

def clean_jobs_with_gemini():
    """Clean job descriptions using Google Gemini API in batches."""
    if not GEMINI_API_KEY:
        logging.warning("GEMINI_API_KEY is not set. Skipping data cleaning.")
        return

    logging.info("Starting Gemini data cleaning (Batch Mode)...")
    
    BATCH_SIZE = 5
    
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Fetch a larger chunk of uncleaned jobs
        cursor.execute("""
            SELECT id, role, company, description, company_profile 
            FROM jobs 
            WHERE is_cleaned = FALSE 
            LIMIT 50
        """)
        jobs_to_clean = cursor.fetchall()
        
        if not jobs_to_clean:
            logging.info("No jobs pending cleaning.")
            conn.close()
            return

        logging.info(f"Found {len(jobs_to_clean)} jobs to clean. Processing in batches of {BATCH_SIZE}...")

        # Process in batches
        for i in range(0, len(jobs_to_clean), BATCH_SIZE):
            batch = jobs_to_clean[i : i + BATCH_SIZE]
            batch_input = []
            
            for job in batch:
                raw_text = f"Role: {job['role']}\nCompany: {job['company']}\nDescription: {job['description']}\nCompany Profile: {job['company_profile']}"
                batch_input.append({
                    "id": job['id'],
                    "text": raw_text
                })

            prompt = """
            You are a data cleaning assistant. I will provide a list of job descriptions.
            For EACH job, extract and clean the data into a JSON object.
            
            Return a SINGLE JSON object where the keys are the provided "id"s and the values are the cleaned details.
            
            Format for each value:
            {
              "job_summary": "A short 2-sentence summary of the role",
              "skills": ["skill1", "skill2"],
              "experience_required": "e.g., '2-4 years' or 'Fresher'",
              "clean_description": "The full description formatted in Markdown",
              "clean_address": "Verified company address if available, else null",
              "clean_email": "Contact email if available, else null"
            }
            
            Input Data:
            """ + json.dumps(batch_input)

            try:
                # Call Gemini API
                response = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json"
                    )
                )
                
                # Parse the batch response
                cleaned_batch = json.loads(response.text)
                
                update_cursor = conn.cursor()
                
                # Update each job in the batch
                for job_id_str, cleaned_data in cleaned_batch.items():
                    # Gemini might return ID as string, convert to ensure match
                    cleaned_json_str = json.dumps(cleaned_data)
                    
                    update_cursor.execute(
                        "UPDATE jobs SET cleaned_data = %s, is_cleaned = TRUE WHERE id = %s",
                        (cleaned_json_str, job_id_str)
                    )
                
                conn.commit()
                logging.info(f"Processed batch of {len(cleaned_batch)} jobs.")
                
                # Sleep briefly to respect rate limits
                time.sleep(2) 

            except Exception as e:
                logging.error(f"Error processing batch starting at index {i}: {e}")
        
        conn.close()
        logging.info("Batch cleaning session completed.")

    except Exception as e:
        logging.error(f"Fatal error in cleaning process: {e}")

async def main() -> None:
    """Main function to scrape and update jobs."""
    logging.info("Script is starting...")
    init_db()
    
    existing_links = get_existing_links()
    
    # Run independent scrape tasks with existing_links passed
    infopark_task = scrape_jobs(INFOPARK_URL, "Infopark", existing_links)
    technopark_task = scrape_jobs(TECHNOPARK_URL, "Technopark", existing_links)
    ul_task = scrape_ul_jobs(existing_links)
    
    # RSS is sync, run it directly
    rss_jobs = scrape_cyberpark_rss(existing_links)
    
    # Gather async results
    results = await asyncio.gather(infopark_task, technopark_task, ul_task)
    infopark_jobs, technopark_jobs, ul_jobs = results
    
    all_jobs = infopark_jobs + technopark_jobs + ul_jobs + rss_jobs

    if all_jobs:
        save_jobs_to_db(all_jobs)
        logging.info(f"Scraped and saved {len(all_jobs)} jobs in total.")
    else:
        logging.info("No new jobs found.")
    
    await update_missing_emails()
    
    # Run cleaning synchronously
    clean_jobs_with_gemini()
    
    logging.info("Script has completed.")

if __name__ == "__main__":
    asyncio.run(main())