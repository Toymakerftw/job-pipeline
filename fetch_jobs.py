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
UL_URL = os.getenv("UL_URL", "https://www.ulcyberpark.com/jobs/index")
CYBERPARK_RSS_URL = os.getenv("CYBERPARK_RSS_URL", "https://www.cyberparkkerala.org/?feed=job_feed")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Scraper Configuration
ENABLE_INFOPARK = os.getenv("ENABLE_INFOPARK", "true").lower() == "true"
ENABLE_TECHNOPARK = os.getenv("ENABLE_TECHNOPARK", "true").lower() == "true"
ENABLE_UL = os.getenv("ENABLE_UL", "true").lower() == "true"
ENABLE_CYBERPARK_RSS = os.getenv("ENABLE_CYBERPARK_RSS", "true").lower() == "true"

# Scraping Configuration
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "10"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
SCRAPE_DELAY = float(os.getenv("SCRAPE_DELAY", "0.5"))  # Delay between requests to be respectful

# Job filtering configuration
FILTER_EXPIRED_JOBS = os.getenv("FILTER_EXPIRED_JOBS", "true").lower() == "true"
MAX_DAYS_TO_DEADLINE = int(os.getenv("MAX_DAYS_TO_DEADLINE", "0"))  # 0 means no limit

# Database Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "kljobs_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "PX#lGJi5D68lH@")
DB_NAME = os.getenv("DB_NAME", "kljobs_db")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("job_pipeline.log"),
        logging.StreamHandler()
    ]
)

def get_db_connection():
    """Establish a connection to the MySQL database."""
    try:
        return mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            autocommit=True,
            connection_timeout=30
        )
    except mysql.connector.Error as e:
        logging.error(f"Failed to connect to database: {e}")
        raise

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

def extract_job_details_from_description(description: str) -> Dict[str, any]:
    """Extract structured information from job description using pattern matching."""
    details = {
        'experience_required': None,
        'salary_range': None,
        'job_type': None,  # Full-time, Part-time, Internship, etc.
        'skills': [],
        'location': None
    }

    # Extract experience requirements
    exp_patterns = [
        r'(\d+)\+?\s*(?:years?|yrs?)\s+experience',
        r'(\d+)\s*-\s*(\d+)\s*(?:years?|yrs?)\s+experience',
        r'experience:\s*(\d+)\+?\s*(?:years?|yrs?)',
        r'(\d+)\+?\s*(?:years?|yrs?)\s+(?:of\s+)?(?:experience|exp)',
        r'fresher|entry\s*level|junior|senior|lead|principal',
    ]

    for pattern in exp_patterns:
        matches = re.findall(pattern, description, re.IGNORECASE)
        if matches:
            for match in matches:
                if isinstance(match, tuple):
                    details['experience_required'] = f"{match[0]}-{match[1]} years"
                elif 'fresher' in match.lower() or 'entry' in match.lower():
                    details['experience_required'] = 'Fresher/Entry Level'
                elif 'junior' in match.lower():
                    details['experience_required'] = 'Junior Level'
                elif 'senior' in match.lower():
                    details['experience_required'] = 'Senior Level'
                elif 'lead' in match.lower():
                    details['experience_required'] = 'Lead Level'
                elif 'principal' in match.lower():
                    details['experience_required'] = 'Principal Level'
                else:
                    details['experience_required'] = f"{match}+ years"
            break

    # Extract salary information
    salary_patterns = [
        r'(?:Rs\.?|INR|₹)?\s*(\d+(?:\.\d+)?\s*(?:lacs?|lakhs?|L)?)\s*(?:-|to|–)\s*(\d+(?:\.\d+)?\s*(?:lacs?|lakhs?|L)?)',
        r'(?:Rs\.?|INR|₹)?\s*(\d+(?:\.\d+)?\s*(?:lacs?|lakhs?|L)?)\s*(?:pa|per\s+annum|annually)?',
    ]

    for pattern in salary_patterns:
        matches = re.findall(pattern, description, re.IGNORECASE)
        if matches:
            if len(matches[0]) == 2:
                details['salary_range'] = f"{matches[0][0]} - {matches[0][1]}"
            else:
                details['salary_range'] = f"{matches[0][0]} per annum"
            break

    # Extract job type
    job_type_patterns = [
        (r'full[ -]?time', 'Full-time'),
        (r'part[ -]?time', 'Part-time'),
        (r'contract', 'Contract'),
        (r'intern', 'Internship'),
        (r'freelance', 'Freelance'),
        (r'remote', 'Remote'),
        (r'hybrid', 'Hybrid'),
        (r'work[ -]?from[ -]?home', 'Work from Home')
    ]

    for pattern, job_type in job_type_patterns:
        if re.search(pattern, description, re.IGNORECASE):
            details['job_type'] = job_type
            break

    # Extract common skills
    skill_keywords = [
        'Python', 'Java', 'JavaScript', 'TypeScript', 'C++', 'C#', 'Go', 'Rust', 'PHP',
        'React', 'Angular', 'Vue', 'Node.js', 'Django', 'Flask', 'Spring', 'AWS', 'Azure',
        'Docker', 'Kubernetes', 'SQL', 'MongoDB', 'PostgreSQL', 'MySQL', 'Git', 'CI/CD',
        'Machine Learning', 'AI', 'Data Science', 'DevOps', 'Full Stack', 'Frontend', 'Backend',
        'API', 'REST', 'GraphQL', 'Microservices', 'Agile', 'Scrum', 'Testing', 'Jenkins',
        'React Native', 'Android', 'iOS', 'Swift', 'Kotlin', 'TensorFlow', 'PyTorch'
    ]

    for skill in skill_keywords:
        if skill.lower() in description.lower():
            details['skills'].append(skill)

    # Extract location
    location_patterns = [
        r'(?:at|in|location:)\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)(?:\s+|,).*?(?:office|company|work|location)',
    ]

    for pattern in location_patterns:
        matches = re.findall(pattern, description, re.IGNORECASE)
        if matches:
            details['location'] = matches[0].strip()
            break

    return details


def format_description(description: str) -> str:
    """Normalize whitespace in the job description."""
    return re.sub(r'\s+', ' ', description).strip()

def is_deadline_in_future(deadline: str) -> bool:
    """
    Try to parse the deadline and return True if it's in the future.
    If parsing fails, assume the deadline is valid.
    """
    if not FILTER_EXPIRED_JOBS:
        return True

    try:
        # Attempt to parse the deadline string.
        deadline_date = parse_date(deadline)

        # Check if deadline is within the allowed range
        if MAX_DAYS_TO_DEADLINE > 0:
            from datetime import datetime, timedelta
            max_date = datetime.now() + timedelta(days=MAX_DAYS_TO_DEADLINE)
            return datetime.now() <= deadline_date <= max_date
        else:
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
        async with session.get(url, ssl=ssl_context, timeout=ClientTimeout(total=timeout)) as response:
            response.raise_for_status()
            return await response.text()
    except asyncio.TimeoutError:
        logging.error(f"Timeout error fetching {url}")
    except aiohttp.ClientResponseError as e:
        logging.error(f"HTTP error {e.status} fetching {url}: {e.message}")
    except aiohttp.ClientError as e:
        logging.error(f"Client error fetching {url}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error fetching {url}: {type(e).__name__}: {e}")
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
    consecutive_empty_pages = 0  # Track consecutive empty pages to avoid infinite loops

    async with aiohttp.ClientSession(timeout=ClientTimeout(total=30)) as session:
        current_url = UL_URL
        while current_url and consecutive_empty_pages < 3:
            logging.info(f"Fetching {current_url}")
            html = await fetch(session, current_url)
            if not html:
                logging.warning(f"No content received for {current_url}, skipping")
                break

            try:
                soup = BeautifulSoup(html, 'html.parser')
                table_div = soup.find('div', class_='table-responsive-sm table-job')
                if not table_div:
                    consecutive_empty_pages += 1
                    logging.info(f"No job table found on {current_url}, consecutive empty pages: {consecutive_empty_pages}")
                    break
                table = table_div.find('table', class_='table')
                if not table:
                    consecutive_empty_pages += 1
                    logging.info(f"No job table element found on {current_url}, consecutive empty pages: {consecutive_empty_pages}")
                    break

                jobs_on_page = 0
                for row in table.find_all('tr'):
                    try:
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
                        role = job_title_elem.get_text(strip=True) if job_title_elem else 'N/A'

                        closing_date_elem = tds[0].find('span')
                        deadline = 'N/A'
                        if closing_date_elem:
                            deadline = closing_date_elem.get_text(strip=True)
                            # Extract date from text like "closing date: 2023-12-31"
                            if 'closing date:' in deadline.lower():
                                deadline = deadline.split('closing date:')[-1].strip()

                        company_elem = tds[1].find('a', class_='btn-1')
                        company = company_elem.get_text(strip=True) if company_elem else 'N/A'

                        # Placeholder for now, could fetch deep details if needed
                        description = f"Job at {company}. See link for details."
                        company_profile = f"Company: {company}"
                        email = ""

                        if is_deadline_in_future(deadline):
                            all_jobs.append((company, role, deadline, link, "UL Cyberpark", description, company_profile, email))
                            jobs_on_page += 1
                        else:
                            logging.info(f"Skipping job {link} as the deadline has passed: {deadline}")
                    except Exception as e:
                        logging.error(f"Error parsing job row: {e}")
                        continue

                if jobs_on_page == 0:
                    consecutive_empty_pages += 1
                    logging.info(f"No new jobs found on page, consecutive empty pages: {consecutive_empty_pages}")
                else:
                    consecutive_empty_pages = 0  # Reset if we found jobs

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

            except Exception as e:
                logging.error(f"Error processing page {current_url}: {e}")
                break

    logging.info(f"Finished scraping jobs from UL Cyberpark. Found {len(all_jobs)} new jobs.")
    return all_jobs

def scrape_cyberpark_rss(existing_links: Set[str]) -> List[Tuple]:
    """Scrape jobs from Cyberpark Kerala RSS Feed."""
    logging.info("Started scraping jobs from Cyberpark RSS...")
    all_jobs = []
    try:
        feed = feedparser.parse(CYBERPARK_RSS_URL)

        if not feed.entries:
            logging.warning("No entries found in RSS feed")
            return all_jobs

        for entry in feed.entries:
            try:
                link = entry.link
                if link in existing_links:
                    continue

                role = entry.title if hasattr(entry, 'title') else "N/A"
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
                logging.error(f"Error processing RSS entry: {e}")
                continue

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
        consecutive_empty_pages = 0  # Track consecutive empty pages to avoid infinite loops

        while consecutive_empty_pages < 3:  # Stop after 3 consecutive empty pages
            url = f"{base_url}?page={page}"
            logging.info(f"Fetching {url}")
            html_or_json = await fetch(session, url)
            if not html_or_json:
                logging.warning(f"No content received for {url}, skipping page {page}")
                page += 1
                continue

            jobs_in_page = []
            try:
                if tech_park == "Infopark":
                    soup = BeautifulSoup(html_or_json, "html.parser")
                    rows = soup.select("#job-list tbody tr")
                    if not rows:
                        consecutive_empty_pages += 1
                        logging.info(f"No job rows found on page {page}, consecutive empty pages: {consecutive_empty_pages}")
                        break
                    consecutive_empty_pages = 0  # Reset counter when we find jobs

                    for row in rows:
                        try:
                            job_link_elem = row.select_one("td.btn-sec a")
                            if not job_link_elem:
                                continue
                            job_link = job_link_elem.get("href", "")
                            if not job_link or job_link in existing_links:
                                continue

                            job_role = row.select_one("td.head")
                            job_role = job_role.get_text(strip=True) if job_role else "N/A"

                            company_elem = row.select_one("td.date")
                            company = company_elem.get_text(strip=True) if company_elem else "N/A"

                            deadline_elem = row.select_one("td:nth-child(3)")
                            deadline = deadline_elem.get_text(strip=True) if deadline_elem else "N/A"

                            jobs_in_page.append((company, job_role, deadline, job_link))
                        except Exception as e:
                            logging.error(f"Error parsing job row on page {page}: {e}")
                            continue

                    has_next_page = bool(soup.select_one("li.page-item a[rel='next']"))

                elif tech_park == "Technopark":
                    try:
                        data = json.loads(html_or_json)
                        if not data.get("data"):
                            consecutive_empty_pages += 1
                            logging.info(f"No job data found on page {page}, consecutive empty pages: {consecutive_empty_pages}")
                            break
                        consecutive_empty_pages = 0  # Reset counter when we find jobs

                        for job in data["data"]:
                            try:
                                job_link = f"https://technopark.org/job-details/{job['id']}"
                                if job_link in existing_links:
                                    continue

                                company = job.get("company", {}).get("company", "N/A")
                                job_role = job.get("job_title", "N/A")
                                deadline = job.get("closing_date", "N/A")

                                jobs_in_page.append((company, job_role, deadline, job_link))
                            except Exception as e:
                                logging.error(f"Error parsing job data: {e}")
                                continue

                        has_next_page = data.get("current_page", 0) < data.get("last_page", 0)
                    except json.JSONDecodeError as e:
                        logging.error(f"Error parsing JSON response from {url}: {e}")
                        break
            except Exception as e:
                logging.error(f"Error processing page {page} from {tech_park}: {e}")
                consecutive_empty_pages += 1
                continue

            # Optimization: Stop if all jobs on current page are duplicates (and not page 1)
            if not jobs_in_page:
                if page > 1:
                    consecutive_empty_pages += 1
                    logging.info(f"All jobs on page {page} are duplicates. Consecutive empty pages: {consecutive_empty_pages}")
                if page == 1 and html_or_json:
                    logging.info(f"All jobs on page 1 are duplicates. No new jobs at {tech_park}.")
                    break
            else:
                consecutive_empty_pages = 0  # Reset if we found jobs

            # Process jobs on this page
            tasks = []
            for company, role, deadline, link in jobs_in_page:
                try:
                    if tech_park == "Infopark":
                        task = asyncio.create_task(get_infopark_job_details(session, link))
                    else:
                        task = asyncio.create_task(get_technopark_job_details(session, link))
                    tasks.append((company, role, deadline, link, task))
                except Exception as e:
                    logging.error(f"Error creating task for job {link}: {e}")
                    continue

            for company, role, deadline, link, task in tasks:
                try:
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
                except Exception as e:
                    logging.error(f"Error processing job details for {link}: {e}")
                    continue

            page += 1
            if not has_next_page:
                logging.info(f"No more pages to fetch for {tech_park}")
                break

        logging.info(f"Finished scraping jobs from {tech_park}. Total jobs found: {len(all_jobs)}")
        return all_jobs

def remove_similar_jobs(jobs: List[Tuple], similarity_threshold: float = 0.9) -> List[Tuple]:
    """Remove jobs that are very similar based on role and company to avoid duplicates across sources."""
    if not jobs:
        return jobs

    # Simple similarity function based on role and company
    def similarity(job1: Tuple, job2: Tuple) -> float:
        # Compare role (index 1) and company (index 0)
        role1, company1 = job1[1].lower(), job1[0].lower()
        role2, company2 = job2[1].lower(), job2[0].lower()

        # Calculate similarity - simple approach using common words
        role_words1 = set(role1.split())
        role_words2 = set(role2.split())
        common_role_words = role_words1.intersection(role_words2)
        role_similarity = len(common_role_words) / max(len(role_words1), len(role_words2), 1)

        company_words1 = set(company1.split())
        company_words2 = set(company2.split())
        common_company_words = company_words1.intersection(company_words2)
        company_similarity = len(common_company_words) / max(len(company_words1), len(company_words2), 1)

        # Weight role similarity more heavily than company similarity
        return 0.7 * role_similarity + 0.3 * company_similarity

    unique_jobs = []
    for job in jobs:
        is_similar = False
        for unique_job in unique_jobs:
            if similarity(job, unique_job) >= similarity_threshold:
                is_similar = True
                # Keep the job with more complete information (longer description)
                if len(job[5]) > len(unique_job[5]):  # Compare description lengths
                    unique_jobs.remove(unique_job)
                    unique_jobs.append(job)
                break
        if not is_similar:
            unique_jobs.append(job)

    return unique_jobs


def save_jobs_to_db(jobs: List[Tuple]) -> None:
    """Save jobs to MySQL database using INSERT IGNORE to avoid duplicates."""
    try:
        # Remove similar jobs to avoid duplicates across sources
        unique_jobs = remove_similar_jobs(jobs)

        conn = get_db_connection()
        cursor = conn.cursor()
        # MySQL uses %s for placeholders
        query = """
            INSERT IGNORE INTO jobs
            (company, role, deadline, link, tech_park, description, company_profile, email)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(query, unique_jobs)
        conn.commit()
        logging.info(f"Saved {cursor.rowcount} new jobs to MySQL database (after removing {len(jobs) - len(unique_jobs)} similar jobs).")
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

class ScrapingStats:
    """Track statistics about the scraping process"""
    def __init__(self):
        self.infopark_jobs = 0
        self.technopark_jobs = 0
        self.ul_jobs = 0
        self.rss_jobs = 0
        self.total_jobs = 0
        self.start_time = datetime.now()

    def log_stats(self):
        """Log scraping statistics"""
        end_time = datetime.now()
        duration = end_time - self.start_time

        logging.info("="*50)
        logging.info("SCRAPING STATISTICS")
        logging.info("="*50)
        logging.info(f"Infopark jobs: {self.infopark_jobs}")
        logging.info(f"Technopark jobs: {self.technopark_jobs}")
        logging.info(f"UL Cyberpark jobs: {self.ul_jobs}")
        logging.info(f"Cyberpark RSS jobs: {self.rss_jobs}")
        logging.info(f"Total jobs found: {self.total_jobs}")
        logging.info(f"Execution time: {duration}")
        logging.info("="*50)


async def main() -> None:
    """Main function to scrape and update jobs."""
    logging.info("Script is starting...")
    stats = ScrapingStats()
    init_db()

    existing_links = get_existing_links()

    # Run independent scrape tasks based on configuration
    tasks = []

    if ENABLE_INFOPARK:
        logging.info("Infopark scraping is enabled")
        tasks.append(scrape_jobs(INFOPARK_URL, "Infopark", existing_links, MAX_CONCURRENT_REQUESTS))
    else:
        logging.info("Infopark scraping is disabled")

    if ENABLE_TECHNOPARK:
        logging.info("Technopark scraping is enabled")
        tasks.append(scrape_jobs(TECHNOPARK_URL, "Technopark", existing_links, MAX_CONCURRENT_REQUESTS))
    else:
        logging.info("Technopark scraping is disabled")

    if ENABLE_UL:
        logging.info("UL Cyberpark scraping is enabled")
        tasks.append(scrape_ul_jobs(existing_links))
    else:
        logging.info("UL Cyberpark scraping is disabled")

    # Gather async results
    results = []
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Handle RSS separately since it's synchronous
    rss_jobs = []
    if ENABLE_CYBERPARK_RSS:
        logging.info("Cyberpark RSS scraping is enabled")
        rss_jobs = scrape_cyberpark_rss(existing_links)
        stats.rss_jobs = len(rss_jobs)
    else:
        logging.info("Cyberpark RSS scraping is disabled")

    # Filter out any exceptions from the results
    all_jobs = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logging.error(f"Error in scraping task {i}: {result}")
        else:
            all_jobs.extend(result)
            # Update stats based on which source this was
            if ENABLE_INFOPARK and i == 0:
                stats.infopark_jobs = len(result)
            elif ENABLE_TECHNOPARK and (i == 1 or (not ENABLE_INFOPARK and i == 0)):
                stats.technopark_jobs = len(result)
            elif ENABLE_UL and (i == 2 or (not ENABLE_INFOPARK and not ENABLE_TECHNOPARK and i == 0) or
                                (ENABLE_INFOPARK and not ENABLE_TECHNOPARK and i == 1) or
                                (not ENABLE_INFOPARK and ENABLE_TECHNOPARK and i == 1)):
                stats.ul_jobs = len(result)

    all_jobs.extend(rss_jobs)
    stats.total_jobs = len(all_jobs)

    if all_jobs:
        save_jobs_to_db(all_jobs)
        logging.info(f"Scraped and saved {len(all_jobs)} jobs in total.")
    else:
        logging.info("No new jobs found.")

    await update_missing_emails()

    # Run cleaning synchronously if Gemini API key is available
    if GEMINI_API_KEY:
        clean_jobs_with_gemini()
    else:
        logging.info("GEMINI_API_KEY not set, skipping data cleaning.")

    # Log statistics
    stats.log_stats()
    logging.info("Script has completed.")

if __name__ == "__main__":
    asyncio.run(main())