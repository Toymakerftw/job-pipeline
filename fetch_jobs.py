import asyncio
import hashlib
import json
import logging
import os
import re
import ssl
import time
from dataclasses import dataclass, field
from datetime import datetime
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
from openai import RateLimitError
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
    
    # Config File
    SELECTOR_CONFIG_PATH: str = "scraping_config.json"

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
    def generate_content(self, prompt: str) -> str: raise NotImplementedError

class NativeGeminiClient(LLMProvider):
    def __init__(self, key):
        self.client = genai.Client(api_key=key)
    def generate_content(self, prompt: str) -> str:
        try:
            res = self.client.models.generate_content(
                model="gemini-2.0-flash", contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json")
            )
            return res.text
        except Exception as e:
            logger.error(f"Gemini Error: {e}")
            raise

class OpenRouterClient(LLMProvider):
    def __init__(self, key):
        self.client = openai.OpenAI(base_url="https://openrouter.ai/api/v1", api_key=key, max_retries=0)
    def generate_content(self, prompt: str) -> str:
        try:
            res = self.client.chat.completions.create(
                model="mistralai/mistral-small-3.1-24b-instruct:free",
                messages=[{"role": "system", "content": "Output valid JSON only."}, 
                          {"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            return res.choices[0].message.content
        except Exception as e:
            logger.error(f"OpenRouter Error: {e}")
            raise

# --- Key Management ---
class KeyTracker:
    def __init__(self, key: str, redis_client: redis.Redis, config: Config):
        self.key = key
        self.r = redis_client
        self.config = config
        self.key_hash = hashlib.md5(key.encode()).hexdigest()
        self.usage_key = f"usage:{self.key_hash}"
        self.rpm_key = f"rpm:{self.key_hash}"
        self.jail_key = f"jail:{self.key_hash}"
        self.cooldown_until = 0

    @property
    def daily_usage(self) -> int:
        try:
            val = self.r.get(self.usage_key)
            return int(val) if val else 0
        except Exception as e:
            logger.error(f"Redis get error: {e}")
            return 9999 

    @property
    def has_daily_capacity(self) -> bool:
        return self.daily_usage < self.config.OR_KEY_DAILY_LIMIT

    def is_jailed(self) -> bool:
        try:
            return bool(self.r.exists(self.jail_key))
        except Exception as e:
            logger.error(f"Redis jail check error: {e}")
            return False

    def is_available(self) -> bool:
        if self.is_jailed(): return False
        if time.time() < self.cooldown_until: return False
        if not self.has_daily_capacity: return False
        try:
            now = time.time()
            pipe = self.r.pipeline()
            pipe.zremrangebyscore(self.rpm_key, 0, now - 60)
            pipe.zcard(self.rpm_key)
            results = pipe.execute()
            if results[1] >= self.config.OR_KEY_RPM_LIMIT: return False
        except Exception as e:
            logger.error(f"Redis RPM check error: {e}")
            return False
        return True

    def increment_usage(self):
        now = time.time()
        try:
            pipe = self.r.pipeline()
            pipe.zadd(self.rpm_key, {str(now): now})
            pipe.expire(self.rpm_key, 120)
            pipe.incr(self.usage_key)
            pipe.expire(self.usage_key, 86400) 
            pipe.execute()
        except Exception as e:
            logger.error(f"Redis error incrementing usage: {e}")

    def trigger_cooldown(self, seconds=60):
        self.cooldown_until = time.time() + seconds
        logger.warning(f"Key {self.key_hash[:6]}... on local cooldown for {seconds}s")

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
        return any(t.has_daily_capacity and not t.is_jailed() for t in self.trackers)

# --- Selectors & Self-Healing ---
class SelectorManager:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.selectors = self._load_selectors()

    def _load_selectors(self) -> Dict:
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r') as f:
                return json.load(f)
        return {}

    def get(self, section: str, key: str, default: str = None) -> str:
        return self.selectors.get(section, {}).get(key, default)

    def update(self, section: str, key: str, value: str):
        if section not in self.selectors:
            self.selectors[section] = {}
        self.selectors[section][key] = value
        with open(self.config_path, 'w') as f:
            json.dump(self.selectors, f, indent=2)
        logger.info(f"Updated selector [{section}][{key}] -> {value}")

class SelfHealer:
    def __init__(self, config: Config, key_manager: KeyManager, gemini_client: Optional[NativeGeminiClient], selector_manager: SelectorManager):
        self.config = config
        self.key_manager = key_manager
        self.gemini_client = gemini_client
        self.selector_manager = selector_manager

    def _get_client(self):
        tracker = None
        client = None
        or_result = self.key_manager.get_best_client()
        if or_result:
            tracker, client = or_result
        elif self.gemini_client:
            client = self.gemini_client
        return tracker, client

    def heal_extraction(self, html_snippet: str, section: str, field_key: str, context_desc: str) -> Optional[str]:
        logger.warning(f"Attempting self-healing for {section}.{field_key} ({context_desc})...")
        tracker, client = self._get_client()
        if not client:
            logger.error("No AI client available for self-healing.")
            return None

        prompt = f"""
        I have a snippet of HTML from a {context_desc}. 
        I need to extract the '{field_key}'. The previous CSS selector failed. 
        
        Task:
        1. Extract the value for '{field_key}' from the HTML.
        2. Create a NEW, robust CSS selector that would find this element.
        
        HTML Snippet:
        ```html
        {html_snippet[:4000]}
        ```
        
        Return JSON format: {{"extracted_value": "...", "new_selector": "..."}}
        """

        try:
            resp_text = client.generate_content(prompt)
            if tracker: tracker.increment_usage()
            
            data = json.loads(resp_text)
            val = data.get("extracted_value")
            new_sel = data.get("new_selector")
            
            if val and new_sel:
                logger.info(f"Self-healing successful! Found: {val}, New Selector: {new_sel}")
                self.selector_manager.update(section, field_key, new_sel)
                return val
        except Exception as e:
            logger.error(f"Self-healing failed: {e}")
        return None

# --- AI Cleaner (Processing) ---
class AICleaner:
    def __init__(self, config: Config, db: DBManager, key_manager: KeyManager, gemini_client):
        self.config = config
        self.db = db
        self.key_manager = key_manager
        self.gemini_client = gemini_client
        self.redis = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)

    def process_jobs(self):
        logger.info("Starting AI Cleaning Cycle...")
        
        while True:
            if not self.key_manager.has_any_daily_capacity() and not self.gemini_client:
                break

            conn = self.db.get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT id, role, company, description, company_profile, link, deadline, tech_park
                FROM jobs 
                WHERE is_cleaned = FALSE AND cleaned_data IS NULL 
                AND LENGTH(description) > 50
                ORDER BY id DESC LIMIT 5
            """)
            batch = cursor.fetchall()
            cursor.close()
            conn.close()

            if not batch: break

            job_map = {str(j['id']): j for j in batch}
            batch_input = []
            for j in batch:
                desc = (j['description'] or "")[:2500]
                prof = (j['company_profile'] or "")[:2500]
                batch_input.append({
                    "id": j['id'], 
                    "text": f"Role: {j['role']}\nCompany: {j['company']}\nDesc: {desc}\nCompanyProfile: {prof}"
                })

            prompt = f"""
            Extract job details into JSON. Keys are input IDs. 
            Value format: {{"job_title", "job_summary", "skills": [], "experience_required", "clean_description", "clean_address", "clean_email", "clean_company_profile"}}
            Input: {json.dumps(batch_input)}
            """

            tracker = None
            client = None
            or_result = self.key_manager.get_best_client()
            if or_result:
                tracker, client = or_result
            elif self.gemini_client:
                client = self.gemini_client
            
            if not client:
                time.sleep(10)
                continue

            try:
                conn = self.db.get_connection()
                response_text = client.generate_content(prompt)
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
                time.sleep(1)

            except Exception as e:
                if conn: conn.close()
                logger.error(f"Batch Error: {e}")
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

            self.redis.set("jobs_data", json.dumps(formatted))
            self.redis.set("last_updated", datetime.now().isoformat())
        except Exception as e:
            logger.error(f"Redis update failed: {e}")

# --- Scraping Logic with Self-Healing ---
class JobScraper:
    def __init__(self, config: Config, db: DBManager, healer: SelfHealer, selector_manager: SelectorManager):
        self.config = config
        self.db = db
        self.healer = healer
        self.selectors = selector_manager
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
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(q, jobs)
            logger.info(f"Saved {cursor.rowcount} new jobs.")
            conn.close()
        except Exception as e:
            logger.error(f"DB Save Error: {e}")

    def remove_similar_jobs(self, jobs):
        unique = []
        seen = set()
        for job in jobs:
            signature = f"{job[1]}-{job[0]}".lower()
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

    def _extract_text(self, element, selector, section, key, html_context):
        if not selector: return "N/A"
        found = element.select_one(selector)
        if found:
            return found.get_text(strip=True)
        
        # Healing Trigger
        repaired = self.healer.heal_extraction(html_context, section, key, f"{section} job row")
        return repaired if repaired else "N/A"

    async def scrape_infopark(self):
        logger.info("Scraping Infopark...")
        async with aiohttp.ClientSession() as session:
            page = 1
            jobs = []
            
            # Load Selectors
            section = "infopark_list"
            sel_rows = self.selectors.get(section, "rows")
            sel_link = self.selectors.get(section, "link")
            sel_role = self.selectors.get(section, "role")
            sel_comp = self.selectors.get(section, "company")
            sel_dead = self.selectors.get(section, "deadline")

            while page < 5: 
                url = f"{self.config.INFOPARK_URL}?page={page}"
                html = await self.fetch(session, url)
                if not html: break
                soup = BeautifulSoup(html, "html.parser")
                rows = soup.select(sel_rows)
                if not rows: break
                
                tasks = []
                for row in rows:
                    link_tag = row.select_one(sel_link)
                    if not link_tag: continue
                    link = link_tag.get("href")
                    if link in self.existing_links: continue
                    
                    row_html = str(row)
                    role = self._extract_text(row, sel_role, section, "role", row_html)
                    company = self._extract_text(row, sel_comp, section, "company", row_html)
                    deadline = self._extract_text(row, sel_dead, section, "deadline", row_html)

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
        
        section = "infopark_detail"
        sel_desc = self.selectors.get(section, "description")
        
        desc = self._extract_text(soup, sel_desc, section, "description", str(soup)[:5000])
        
        profile = ""
        try:
            company_id = link.split('/')[-1]
            c_html = await self.fetch(session, f"https://infopark.in/companies/profile/{company_id}")
            if c_html:
                c_soup = BeautifulSoup(c_html, "html.parser")
                sel_prof = self.selectors.get(section, "profile_section")
                prof_div = c_soup.select_one(sel_prof)
                
                if prof_div:
                    profile = prof_div.get_text(strip=True)
                else:
                    # Try fallback
                    sel_fallback = self.selectors.get(section, "profile_name_fallback")
                    name = c_soup.select_one(sel_fallback)
                    if name: profile = f"Company: {name.get_text(strip=True)}"
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
        
        section = "technopark_detail"
        
        description = "N/A"
        sel_prim = self.selectors.get(section, "description_primary")
        target_div = soup.select_one(sel_prim)
        if target_div:
            description = target_div.get_text(strip=True, separator="\n")
            
        if description == "N/A" or len(description) < 50:
             sel_sec = self.selectors.get(section, "description_secondary")
             target_div = soup.select_one(sel_sec)
             if target_div:
                description = target_div.get_text(strip=True, separator="\n")

        if description == "N/A":
             description = ""

        # Profile
        sel_comp = self.selectors.get(section, "company")
        comp_div = soup.select_one(sel_comp)
        comp_name = comp_div.get_text(strip=True) if comp_div else "N/A"
        profile = f"Company: {comp_name}"
        
        # Email
        email = ""
        sel_email = self.selectors.get(section, "email")
        a_tag = soup.select_one(sel_email)
        if a_tag: email = a_tag.get_text(strip=True).replace("mailto:", "")
        
        return description, profile, email

    async def scrape_ul(self):
        logger.info("Scraping UL Cyberpark...")
        async with aiohttp.ClientSession() as session:
            jobs = []
            url = self.config.UL_URL
            html = await self.fetch(session, url)
            if not html: return []
            soup = BeautifulSoup(html, "html.parser")
            
            section = "ul_list"
            sel_table = self.selectors.get(section, "table")
            table = soup.select_one(sel_table)
            if not table: return []
            
            rows = table.find_all("tr")[1:]
            sel_link = self.selectors.get(section, "apply_link")
            sel_role = self.selectors.get(section, "role_link")
            sel_comp = self.selectors.get(section, "company_link")
            
            for row in rows: 
                tds = row.find_all("td")
                if len(tds) < 3: continue
                
                link_tag = row.select_one(sel_link)
                link = link_tag.get("href") if link_tag else None
                if not link: continue
                
                if not link.startswith("http"): link = f"{self.config.UL_URL}/{link}"
                if link in self.existing_links: continue
                
                row_html = str(row)
                role = self._extract_text(row, sel_role, section, "role_link", row_html)
                company = self._extract_text(row, sel_comp, section, "company_link", row_html)
                
                # Date parsing is specific here
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
    
    redis_client = redis.Redis(
        host=cfg.REDIS_HOST, 
        port=cfg.REDIS_PORT, 
        password=cfg.REDIS_PASSWORD, 
        decode_responses=True
    )
    
    key_manager = KeyManager(cfg, redis_client)
    gemini_client = NativeGeminiClient(cfg.GEMINI_API_KEY) if cfg.GEMINI_API_KEY else None
    
    selector_manager = SelectorManager(cfg.SELECTOR_CONFIG_PATH)
    healer = SelfHealer(cfg, key_manager, gemini_client, selector_manager)
    
    scraper = JobScraper(cfg, db, healer, selector_manager)
    cleaner = AICleaner(cfg, db, key_manager, gemini_client)

    logger.info("System Ready.")

    while True:
        try:
            await scraper.run_cycle()
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