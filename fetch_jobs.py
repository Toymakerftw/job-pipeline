import asyncio
import json
import logging
import os
import re
import ssl
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Set, Dict, Any, Tuple

import aiohttp
import feedparser
import mysql.connector
from mysql.connector import pooling
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google import genai
from google.genai import types
import openai
import redis

# Load environment variables
load_dotenv()

# --- Configuration ---
@dataclass
class Config:
    # Database
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_USER: str = os.getenv("DB_USER", "kljobs_user")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "PX#lGJi5D68lH@")
    DB_NAME: str = os.getenv("DB_NAME", "kljobs_db")
    
    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD")

    # Scraping Sources
    INFOPARK_URL: str = "https://infopark.in/companies/job-search"
    TECHNOPARK_URL: str = "https://technopark.org/api/paginated-jobs"
    UL_URL: str = "https://www.ulcyberpark.com/jobs/index"
    CYBERPARK_RSS_URL: str = "https://www.cyberparkkerala.org/?feed=job_feed"
    
    # Timing
    SCRAPE_INTERVAL: int = int(os.getenv("SCRAPE_INTERVAL", "43200"))  # 12 hours
    PROCESS_INTERVAL: int = int(os.getenv("PROCESS_INTERVAL", "3600"))  # 1 hour
    REQUEST_TIMEOUT: int = 30

    # AI Keys & Limits
    GEMINI_API_KEY: Optional[str] = os.getenv("GEMINI_API_KEY")
    OPENROUTER_API_KEYS: List[str] = [k.strip() for k in os.getenv("OPENROUTER_API_KEYS", "").split(",") if k.strip()]
    AI_DAILY_REQUEST_LIMIT: int = int(os.getenv("AI_DAILY_REQUEST_LIMIT", "50")) # Intelligent Limit

# --- Logging Setup ---
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/aggregator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Database Manager (Connection Pooling) ---
class DBManager:
    def __init__(self, config: Config):
        self.config = config
        self.db_config = {
            "host": config.DB_HOST,
            "user": config.DB_USER,
            "password": config.DB_PASSWORD,
            "database": config.DB_NAME,
            "autocommit": True,
            "connection_timeout": 30
        }
        self._pool = None

    def get_connection(self):
        try:
            if not self._pool:
                # Initialize pool
                self._pool = pooling.MySQLConnectionPool(pool_name="job_pool", pool_size=5, **self.db_config)
            return self._pool.get_connection()
        except mysql.connector.Error as e:
            logger.error(f"DB Connection Pool Error: {e}")
            # Fallback for initial setup
            return mysql.connector.connect(**self.db_config)

    def init_db(self):
        logger.info(f"Initializing Database: {self.config.DB_NAME}")
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.config.DB_NAME}")
        cursor.execute("USE {}".format(self.config.DB_NAME))
        
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
        
        # Migration for older DBs
        cursor.execute("SHOW COLUMNS FROM jobs LIKE 'cleaned_data'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE jobs ADD COLUMN cleaned_data JSON")
            cursor.execute("ALTER TABLE jobs ADD COLUMN is_cleaned BOOLEAN DEFAULT FALSE")
        
        conn.close()
        logger.info("Database initialized.")

    def get_existing_links(self) -> Set[str]:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT link FROM jobs")
            links = {row[0] for row in cursor.fetchall()}
            conn.close()
            return links
        except Exception as e:
            logger.warning(f"Could not fetch existing links: {e}")
            return set()

# --- Quota Manager (Intelligent Usage Control) ---
class QuotaManager:
    def __init__(self, config: Config):
        self.config = config
        self.redis_key = "kljobs:ai_daily_usage"
        self.r = redis.Redis(
            host=config.REDIS_HOST, 
            port=config.REDIS_PORT, 
            password=config.REDIS_PASSWORD, 
            decode_responses=True
        )
        self.daily_limit = config.AI_DAILY_REQUEST_LIMIT

    def can_process(self) -> int:
        try:
            current_usage = int(self.r.get(self.redis_key) or 0)
            remaining = self.daily_limit - current_usage
            
            if remaining <= 0:
                logger.warning(f"⛔ Daily AI Quota Exceeded ({current_usage}/{self.daily_limit}). Pausing AI.")
                return 0
            
            logger.info(f"✅ Quota Check: {current_usage}/{self.daily_limit} used. {remaining} remaining.")
            return remaining
        except Exception as e:
            logger.error(f"Failed to check quota: {e}. Proceeding with safe limit (5).")
            return 5

    def record_usage(self, count: int):
        try:
            pipe = self.r.pipeline()
            pipe.incrby(self.redis_key, count)
            pipe.expire(self.redis_key, 86400) # 24 hours
            pipe.execute()
        except Exception as e:
            logger.error(f"Failed to record usage: {e}")

    def reset_quota(self):
        self.r.delete(self.redis_key)

# --- AI Clients ---
class LLMProvider:
    def generate_cleaned_data(self, prompt: str) -> str: raise NotImplementedError

class NativeGeminiClient(LLMProvider):
    def __init__(self, key):
        self.client = genai.Client(api_key=key)
    def generate_cleaned_data(self, prompt: str) -> str:
        res = self.client.models.generate_content(
            model="gemini-2.5-flash", contents=prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json")
        )
        return res.text

class OpenRouterClient(LLMProvider):
    def __init__(self, key):
        self.client = openai.OpenAI(base_url="https://openrouter.ai/api/v1", api_key=key)
    def generate_cleaned_data(self, prompt: str) -> str:
        res = self.client.chat.completions.create(
            model="mistralai/mistral-small-3.1-24b-instruct:free",
            messages=[{"role": "system", "content": "Output valid JSON only."},
                      {"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        return res.choices[0].message.content

# --- AI Cleaner (Intelligent) ---
class AICleaner:
    def __init__(self, config: Config, db: DBManager):
        self.config = config
        self.db = db
        self.clients = []
        self._index = 0
        self.quota = QuotaManager(config)
        self._init_clients()

    def _init_clients(self):
        if self.config.GEMINI_API_KEY:
            self.clients.append(NativeGeminiClient(self.config.GEMINI_API_KEY))
        for key in self.config.OPENROUTER_API_KEYS:
            self.clients.append(OpenRouterClient(key))

    def get_next_client(self):
        if not self.clients: return None
        client = self.clients[self._index]
        self._index = (self._index + 1) % len(self.clients)
        return client

    def process_jobs(self):
        remaining = self.quota.can_process()
        if remaining <= 0 or not self.clients:
            return

        logger.info(f"Starting AI Cleaning (Budget: {remaining})...")
        conn = self.db.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Intelligent Fetch: Newest First (DESC), limited by budget
        limit = min(remaining, 50)
        cursor.execute(f"""
            SELECT id, role, company, description, company_profile, link, deadline, tech_park
            FROM jobs 
            WHERE is_cleaned = FALSE AND cleaned_data IS NULL 
            ORDER BY id DESC 
            LIMIT {limit}
        """)
        jobs = cursor.fetchall()
        cursor.close()
        
        if not jobs:
            conn.close()
            return

        BATCH_SIZE = 5
        total_cleaned = 0
        
        try:
            for i in range(0, len(jobs), BATCH_SIZE):
                batch = jobs[i:i + BATCH_SIZE]
                job_map = {str(j['id']): j for j in batch}
                
                batch_input = [{"id": j['id'], "text": f"Role: {j['role']}\nCompany: {j['company']}\nDesc: {j['description']}"} for j in batch]
                prompt = f"""
                Extract data into JSON. Keys are input IDs. 
                Value format: {{"job_title", "job_summary", "skills": [], "experience_required", "clean_description", "clean_address", "clean_email"}}
                Input: {json.dumps(batch_input)}
                """
                
                client = self.get_next_client()
                try:
                    response_text = client.generate_cleaned_data(prompt)
                    
                    # Check for soft quota errors in response body
                    if "quota" in response_text.lower() or "limit" in response_text.lower() and "error" in response_text.lower():
                        raise Exception("Soft quota limit detected in response body")

                    cleaned_batch = json.loads(response_text)
                    update_cursor = conn.cursor()
                    for jid, data in cleaned_batch.items():
                        original = job_map.get(str(jid))
                        if original:
                            data['id'] = original['id']
                            data['company'] = original['company']
                            data['link'] = original['link']
                            data['deadline'] = original['deadline']
                            data['tech_park'] = original['tech_park']
                            data['role'] = data.get('job_title', original['role'])
                        
                        update_cursor.execute(
                            "UPDATE jobs SET cleaned_data = %s, is_cleaned = TRUE WHERE id = %s",
                            (json.dumps(data), jid)
                        )
                        total_cleaned += 1
                    conn.commit()
                    time.sleep(1) 
                    
                except Exception as e:
                    error_str = str(e).lower()
                    is_quota_error = any(x in error_str for x in ["429", "quota", "limit", "credit", "402"])
                    if is_quota_error:
                        logger.error(f"🛑 QUOTA HIT. Stopping AI. Error: {e}")
                        break
                    else:
                        logger.error(f"Batch Error: {e}")

            if total_cleaned > 0:
                self.quota.record_usage(total_cleaned)
                logger.info(f"Cleaned {total_cleaned} jobs.")
            
            self.cache_to_redis()
        finally:
            conn.close()

    def cache_to_redis(self):
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT id, cleaned_data, company, role, deadline, link, tech_park FROM jobs ORDER BY id DESC")
            jobs = cursor.fetchall()
            conn.close()

            formatted = []
            for job in jobs:
                if job.get('cleaned_data'):
                    try:
                        data = json.loads(job['cleaned_data'])
                        data['is_cleaned'] = True
                        if 'id' not in data: data['id'] = job['id']
                        formatted.append(data)
                        continue
                    except: pass
                
                formatted.append({
                    "id": job['id'], "company": job['company'], "role": job['role'],
                    "deadline": job['deadline'], "link": job['link'], "tech_park": job['tech_park'],
                    "is_cleaned": False
                })

            r = redis.Redis(host=self.config.REDIS_HOST, port=self.config.REDIS_PORT, password=self.config.REDIS_PASSWORD, decode_responses=True)
            r.set("jobs_data", json.dumps(formatted))
            r.set("last_updated", datetime.now().isoformat())
        except Exception as e:
            logger.error(f"Redis update failed: {e}")

# --- Scraping Logic ---
class JobScraper:
    def __init__(self, config: Config, db: DBManager):
        self.config = config
        self.db = db
        self.existing_links = db.get_existing_links()

    async def run_cycle(self):
        logger.info("Starting Scrape Cycle...")
        tasks = [self.scrape_infopark(), self.scrape_technopark(), self.scrape_ul()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        rss_jobs = await asyncio.get_event_loop().run_in_executor(None, self.scrape_cyberpark_rss)
        results.append(rss_jobs)

        all_jobs = []
        for res in results:
            if isinstance(res, Exception):
                logger.error(f"Scrape task failed: {res}")
            elif isinstance(res, list):
                all_jobs.extend(res)

        if all_jobs:
            self.save_jobs(self.remove_similar_jobs(all_jobs))
        logger.info("Scrape Cycle Finished.")

    def save_jobs(self, jobs: List[Tuple]):
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            q = """INSERT IGNORE INTO jobs 
                   (company, role, deadline, link, tech_park, description, company_profile, email)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.executemany(q, jobs)
            logger.info(f"Saved {cursor.rowcount} new jobs.")
            conn.close()
        except Exception as e:
            logger.error(f"DB Save Error: {e}")

    def remove_similar_jobs(self, jobs):
        # Basic deduplication logic based on title/company
        unique = []
        seen = set()
        for job in jobs:
            # signature = (job[0].lower(), job[1].lower()) # Company, Role
            # Using Link is better but we already did that. 
            # This is for cross-source duplicates where link differs.
            signature = f"{job[1]}-{job[0]}".lower() # Role - Company
            if signature not in seen:
                seen.add(signature)
                unique.append(job)
            else:
                # Keep the one with longer description (likely better data)
                for i, u_job in enumerate(unique):
                    if f"{u_job[1]}-{u_job[0]}".lower() == signature:
                        if len(job[5]) > len(u_job[5]):
                            unique[i] = job
                        break
        return unique

    async def fetch(self, session, url):
        try:
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
            async with session.get(url, ssl=ssl_ctx, timeout=aiohttp.ClientTimeout(total=self.config.REQUEST_TIMEOUT)) as resp:
                return await resp.text()
        except Exception as e:
            return None

    async def scrape_infopark(self):
        logger.info("Scraping Infopark...")
        async with aiohttp.ClientSession() as session:
            page = 1
            jobs = []
            while page < 5: # Limit pages for performance
                url = f"{self.config.INFOPARK_URL}?page={page}"
                html = await self.fetch(session, url)
                if not html: break
                soup = BeautifulSoup(html, "html.parser")
                rows = soup.select("#job-list tbody tr")
                if not rows: break
                
                tasks = []
                for row in rows:
                    link_tag = row.select_one("td.btn-sec a")
                    if not link_tag: continue
                    link = link_tag.get("href")
                    if link in self.existing_links: continue
                    role = row.select_one("td:nth-child(3)").get_text(strip=True) if row.select_one("td:nth-child(3)") else "N/A"
                    company = row.select_one("td.date").get_text(strip=True) if row.select_one("td.date") else "N/A"
                    deadline = row.select_one("td.head").get_text(strip=True) if row.select_one("td.head") else "N/A"
                    tasks.append((company, role, deadline, link, "Infopark", self.get_infopark_details(session, link)))
                
                for company, role, deadline, link, source, task in tasks:
                    try:
                        desc, profile, email = await task
                        jobs.append((company, role, deadline, link, source, desc, profile, email))
                    except: pass
                page += 1
            return jobs

    async def get_infopark_details(self, session, link):
        html = await self.fetch(session, link)
        if not html: return "", "", ""
        soup = BeautifulSoup(html, "html.parser")
        desc_div = soup.find("div", class_="deatil-box")
        desc = desc_div.get_text(strip=True) if desc_div else ""
        
        # Simplified profile extraction
        profile = ""
        try:
            company_id = link.split('/')[-1]
            c_html = await self.fetch(session, f"https://infopark.in/companies/profile/{company_id}")
            if c_html:
                c_soup = BeautifulSoup(c_html, "html.parser")
                name = c_soup.find("h4").get_text(strip=True) if c_soup.find("h4") else ""
                profile = f"Company: {name}"
        except: pass
        
        emails = re.findall(r'[\w\.-]+@[\w\.-]+', desc)
        return desc, profile, emails[0] if emails else ""

    async def scrape_technopark(self):
        logger.info("Scraping Technopark...")
        async with aiohttp.ClientSession() as session:
            page = 1
            jobs = []
            while page < 5:
                url = f"{self.config.TECHNOPARK_URL}?page={page}"
                data_text = await self.fetch(session, url)
                if not data_text: break
                try:
                    data = json.loads(data_text)
                    if not data.get("data"): break
                    
                    tasks = []
                    for job in data["data"]:
                        link = f"https://technopark.org/job-details/{job['id']}"
                        if link in self.existing_links: continue
                        role = job.get("job_title", "N/A")
                        company = job.get("company", {}).get("company", "N/A")
                        deadline = job.get("closing_date", "N/A")
                        tasks.append((company, role, deadline, link, "Technopark", self.get_techno_details(session, link)))
                    
                    for company, role, deadline, link, source, task in tasks:
                        try:
                            desc, profile, email = await task
                            jobs.append((company, role, deadline, link, source, desc, profile, email))
                        except: pass
                    page += 1
                except json.JSONDecodeError: break
            return jobs

    async def get_techno_details(self, session, link):
        html = await self.fetch(session, link)
        if not html: return "", "", ""
        soup = BeautifulSoup(html, "html.parser")
        desc_div = soup.find("div", class_="mb-4")
        desc = desc_div.get_text(strip=True) if desc_div else ""
        
        # Profile/Email
        comp_div = soup.find("div", class_="w-full")
        comp_name = comp_div.find("a").get_text(strip=True) if comp_div and comp_div.find("a") else "N/A"
        profile = f"Company: {comp_name}"
        
        email = ""
        a_tag = soup.find("a", href=lambda x: x and "mailto:" in x)
        if a_tag: email = a_tag.get_text(strip=True).replace("mailto:", "")
        
        return desc, profile, email

    async def scrape_ul(self):
        logger.info("Scraping UL Cyberpark...")
        async with aiohttp.ClientSession() as session:
            jobs = []
            url = self.config.UL_URL
            html = await self.fetch(session, url)
            if not html: return []
            soup = BeautifulSoup(html, "html.parser")
            table = soup.find("table", class_="table")
            if not table: return []
            
            for row in table.find_all("tr")[1:]: # Skip header
                tds = row.find_all("td")
                if len(tds) < 3: continue
                link = tds[2].find("a").get("href") if tds[2].find("a") else None
                if not link: continue
                if not link.startswith("http"): link = f"{self.config.UL_URL}/{link}"
                if link in self.existing_links: continue
                
                role = tds[0].find("a").get_text(strip=True) if tds[0].find("a") else "N/A"
                company = tds[1].find("a").get_text(strip=True) if tds[1].find("a") else "N/A"
                
                # Extract deadline from text
                text = tds[0].get_text(strip=True)
                match = re.search(r'(\d{2}-\d{2}-\d{4})', text)
                deadline = match.group(1) if match else "N/A"
                
                desc = f"Job at {company}. Please check link for details."
                jobs.append((company, role, deadline, link, "UL Cyberpark", desc, f"Company: {company}", ""))
            return jobs

    def scrape_cyberpark_rss(self):
        logger.info("Scraping Cyberpark RSS...")
        jobs = []
        try:
            feed = feedparser.parse(self.config.CYBERPARK_RSS_URL)
            for entry in feed.entries:
                link = entry.link
                if link in self.existing_links: continue
                role = entry.title if hasattr(entry, 'title') else "N/A"
                company = "Cyberpark Company"
                if " - " in role:
                    parts = role.split(" - ")
                    if len(parts) >= 2:
                        role = parts[0].strip()
                        company = parts[1].strip()
                
                deadline = entry.published if hasattr(entry, 'published') else "N/A"
                desc = entry.summary if hasattr(entry, 'summary') else ""
                # Clean HTML
                clean_desc = BeautifulSoup(desc, "html.parser").get_text(strip=True)
                jobs.append((company, role, deadline, link, "Cyberpark Kozhikode", clean_desc, f"Company: {company}", ""))
        except Exception as e:
            logger.error(f"RSS Error: {e}")
        return jobs

# --- Main Orchestrator ---
async def main():
    cfg = Config()
    db = DBManager(cfg)
    db.init_db()
    
    scraper = JobScraper(cfg, db)
    cleaner = AICleaner(cfg, db)

    logger.info("System Ready.")

    while True:
        try:
            # 1. Scrape (Always runs)
            await scraper.run_cycle()
            
            # 2. Clean (Intelligent: Checks quota first)
            cleaner.process_jobs()
            
        except Exception as e:
            logger.error(f"Main Loop Error: {e}")

        # Sleep for the shorter interval (Processing interval).
        # If quota is full, process_jobs returns immediately, so we wait for the next scrape cycle effectively.
        # But we keep checking every hour to see if quota reset or new jobs appeared.
        logger.info(f"Cycle complete. Sleeping for {cfg.PROCESS_INTERVAL}s...")
        await asyncio.sleep(cfg.PROCESS_INTERVAL)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")