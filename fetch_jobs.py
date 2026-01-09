import hashlib
from collections import deque
from dataclasses import dataclass, field
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
from openai import RateLimitError, APIConnectionError
import redis

# Load environment variables
load_dotenv()

# --- Configuration ---
@dataclass
class Config:
    # Database
    DB_HOST: str = os.getenv("DB_HOST", "db")
    DB_USER: str = os.getenv("DB_USER", "kljobs_user")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "PX#lGJi5D68lH@")
    DB_NAME: str = os.getenv("DB_NAME", "kljobs_db")
    
    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD")

    # Scraping Sources
    INFOPARK_URL: str = "https://infopark.in/companies/job-search"
    TECHNOPARK_URL: str = "https://technopark.org/api/paginated-jobs"
    UL_URL: str = "https://www.ulcyberpark.com/jobs/index"
    CYBERPARK_RSS_URL: str = "https://www.cyberparkkerala.org/?feed=job_feed"
    
    # Timing
    SCRAPE_INTERVAL: int = int(os.getenv("SCRAPE_INTERVAL", "43200"))  # 12 hours
    PROCESS_INTERVAL: int = int(os.getenv("PROCESS_INTERVAL", "300"))  # 5 minutes
    REQUEST_TIMEOUT: int = 30

    # AI Keys & Limits
    GEMINI_API_KEY: Optional[str] = os.getenv("GEMINI_API_KEY")
    
    # Fixed mutable default argument using field(default_factory=...)
    OPENROUTER_API_KEYS: List[str] = field(default_factory=lambda: [k.strip() for k in os.getenv("OPENROUTER_API_KEYS", "").split(",") if k.strip()])
    
    # Per-Key Limits (Default: OpenRouter Free Tier)
    OR_KEY_RPM_LIMIT: int = int(os.getenv("OR_KEY_RPM_LIMIT", "20"))
    OR_KEY_DAILY_LIMIT: int = int(os.getenv("OR_KEY_DAILY_LIMIT", "50"))

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

    def _wait_for_db(self):
        """Retry logic to wait for DB to come online"""
        logger.info("Waiting for database connection...")
        retries = 20
        while retries > 0:
            try:
                conn = mysql.connector.connect(
                    host=self.db_config['host'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    connection_timeout=5
                )
                conn.close()
                logger.info("Database connection successful.")
                return True
            except mysql.connector.Error as e:
                retries -= 1
                logger.warning(f"Database not ready yet. Retries left: {retries}. Retrying in 5s...")
                time.sleep(5)
        
        logger.error("Could not connect to database after multiple retries.")
        return False

    def get_connection(self):
        try:
            if not self._pool:
                self._pool = pooling.MySQLConnectionPool(pool_name="job_pool", pool_size=5, **self.db_config)
            return self._pool.get_connection()
        except mysql.connector.Error as e:
            logger.error(f"DB Connection Pool Error: {e}")
            # Fallback
            return mysql.connector.connect(**self.db_config)

    def init_db(self):
        # Wait for DB
        if not self._wait_for_db():
            raise Exception("Failed to connect to Database")

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
        
        # Migration
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
        self.client = openai.OpenAI(base_url="https://openrouter.ai/api/v1", api_key=key, max_retries=0)
    def generate_cleaned_data(self, prompt: str) -> str:
        res = self.client.chat.completions.create(
            model="mistralai/mistral-small-3.1-24b-instruct:free",
            messages=[{"role": "system", "content": "Output valid JSON only."},
                      {"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        return res.choices[0].message.content

# --- Key Management ---
class KeyTracker:
    def __init__(self, key: str, redis_client: redis.Redis, config: Config):
        self.key = key
        self.r = redis_client
        self.config = config
        self.key_hash = hashlib.md5(key.encode()).hexdigest()
        self.usage_key = f"usage:{self.key_hash}"
        self.timestamps = deque()
        self.cooldown_until = 0

    @property
    def daily_usage(self) -> int:
        try:
            val = self.r.get(self.usage_key)
            return int(val) if val else 0
        except Exception as e:
            logger.error(f"Redis get error: {e}")
            return 9999 # Fail safe to avoid usage if redis down

    @property
    def has_daily_capacity(self) -> bool:
        return self.daily_usage < self.config.OR_KEY_DAILY_LIMIT

    def is_available(self) -> bool:
        # Check Cooldown
        if time.time() < self.cooldown_until:
            return False
            
        # Check Daily Limit
        if not self.has_daily_capacity:
            return False

        # Check RPM (Sliding window)
        now = time.time()
        while self.timestamps and now - self.timestamps[0] > 60:
            self.timestamps.popleft()
            
        if len(self.timestamps) >= self.config.OR_KEY_RPM_LIMIT:
            return False
            
        return True

    def increment_usage(self):
        # RPM
        self.timestamps.append(time.time())
        # Daily
        try:
            pipe = self.r.pipeline()
            pipe.incr(self.usage_key)
            pipe.expire(self.usage_key, 86400) # 24h
            pipe.execute()
        except Exception as e:
            logger.error(f"Redis error incrementing usage: {e}")

    def trigger_cooldown(self, seconds=60):
        self.cooldown_until = time.time() + seconds
        logger.warning(f"Key {self.key_hash[:6]}... on cooldown for {seconds}s")

class KeyManager:
    def __init__(self, config: Config, redis_client: redis.Redis):
        self.trackers = [KeyTracker(k, redis_client, config) for k in config.OPENROUTER_API_KEYS]
        self.clients = {t.key: OpenRouterClient(t.key) for t in self.trackers}

    def get_best_client(self) -> Optional[Tuple[KeyTracker, OpenRouterClient]]:
        for tracker in self.trackers:
            if tracker.is_available():
                return tracker, self.clients[tracker.key]
        return None

    def has_any_daily_capacity(self) -> bool:
        return any(t.has_daily_capacity for t in self.trackers)

# --- AI Cleaner ---
class AICleaner:
    def __init__(self, config: Config, db: DBManager):
        self.config = config
        self.db = db
        # Redis connection for KeyManager
        self.redis = redis.Redis(
            host=config.REDIS_HOST, 
            port=config.REDIS_PORT, 
            password=config.REDIS_PASSWORD, 
            decode_responses=True
        )
        self.key_manager = KeyManager(config, self.redis)
        self.gemini_client = NativeGeminiClient(config.GEMINI_API_KEY) if config.GEMINI_API_KEY else None

    def process_jobs(self):
        logger.info("Starting AI Cleaning Cycle...")
        
        while True:
            # 1. Check if we have ANY capacity (Daily) or Gemini
            # If all OpenRouter keys are daily-exhausted AND no Gemini key, we stop.
            if not self.key_manager.has_any_daily_capacity() and not self.gemini_client:
                logger.warning("All AI keys exhausted (Daily Limit). Stopping cycle.")
                break

            conn = self.db.get_connection()
            cursor = conn.cursor(dictionary=True)
            
            # Fetch small batch
            cursor.execute("""
                SELECT id, role, company, description, company_profile, link, deadline, tech_park
                FROM jobs 
                WHERE is_cleaned = FALSE AND cleaned_data IS NULL 
                ORDER BY id DESC 
                LIMIT 5
            """)
            batch = cursor.fetchall()
            cursor.close()
            conn.close() # Close immediately to free connection while processing AI

            if not batch:
                logger.info("No more uncleaned jobs found.")
                break

            job_map = {str(j['id']): j for j in batch}
            batch_input = [{"id": j['id'], "text": f"Role: {j['role']}\nCompany: {j['company']}\nDesc: {j['description']}"} for j in batch]
            prompt = f"""
            Extract data into JSON. Keys are input IDs. 
            Value format: {{"job_title", "job_summary", "skills": [], "experience_required", "clean_description", "clean_address", "clean_email"}}
            Input: {json.dumps(batch_input)}
            """

            # 2. Get Client
            tracker = None
            client = None
            
            or_result = self.key_manager.get_best_client()
            if or_result:
                tracker, client = or_result
            elif self.gemini_client:
                client = self.gemini_client
            
            if not client:
                # If we are here, it means has_any_daily_capacity() was true (or Gemini exists),
                # BUT get_best_client() failed. This implies RPM Limit / Cooldown.
                logger.info("Keys momentarily unavailable (RPM/Cooldown). Sleeping 10s...")
                time.sleep(10)
                continue

            # 3. Execute
            try:
                conn = self.db.get_connection() # Reconnect for update
                response_text = client.generate_cleaned_data(prompt)
                
                if "quota" in response_text.lower() or ("limit" in response_text.lower() and "error" in response_text.lower()):
                    raise RateLimitError("Soft quota limit detected in response body", response=None, body=None)

                if tracker: tracker.increment_usage()

                cleaned_batch = json.loads(response_text)
                update_cursor = conn.cursor()
                count = 0
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
                    count += 1
                conn.commit()
                logger.info(f"Cleaned batch of {count} jobs.")
                conn.close()
                
                # Small courtesy sleep to not hammer DB/CPU, but very short
                time.sleep(1)

            except RateLimitError as e:
                if conn: conn.close()
                logger.warning(f"Rate Limit Hit: {e}")
                if tracker: tracker.trigger_cooldown()
                continue # Loop again to try next key immediately
            except Exception as e:
                if conn: conn.close()
                logger.error(f"Batch Error: {e}")
                # If it's not a rate limit, maybe skip this batch or sleep? 
                # Let's sleep briefly to avoid error loops
                time.sleep(5)
        
        self.cache_to_redis()

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

            # Use self.redis since we have it
            self.redis.set("jobs_data", json.dumps(formatted))
            self.redis.set("last_updated", datetime.now().isoformat())
            logger.info("Redis Cache updated.")
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
        unique = []
        seen = set()
        for job in jobs:
            signature = f"{job[1]}-{job[0]}".lower() # Role - Company
            if signature not in seen:
                seen.add(signature)
                unique.append(job)
            else:
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
            while page < 5: 
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
            
            for row in table.find_all("tr")[1:]: 
                tds = row.find_all("td")
                if len(tds) < 3: continue
                link = tds[2].find("a").get("href") if tds[2].find("a") else None
                if not link: continue
                if not link.startswith("http"): link = f"{self.config.UL_URL}/{link}"
                if link in self.existing_links: continue
                
                role = tds[0].find("a").get_text(strip=True) if tds[0].find("a") else "N/A"
                company = tds[1].find("a").get_text(strip=True) if tds[1].find("a") else "N/A"
                
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

        logger.info(f"Cycle complete. Sleeping for {cfg.PROCESS_INTERVAL}s...")
        await asyncio.sleep(cfg.PROCESS_INTERVAL)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")