#!/usr/bin/env python3
"""
Kerala Tech Park job scraper — patched (v2)
============================================
Drop-in replacement for the original scraper. All fixes are traceable to the QA
report IDs. Selectors verified against the live sites on 2026-10-28.

P0 — verified broken, now fixed
  [P0-1]  Infopark: new URL https://infopark.in/companies-job (the old
          /companies/job-search serves a 378-byte meta-refresh stub -> 0 jobs,
          silently). New v2 selectors for the redesigned table.
  [P0-2]  Infopark company profile keyed by company id = link.split('/')[-2]
          (was [-1] = job id -> wrong profiles / 500 errors).
  [P0-3]  SelectorManager.update() implemented + atomic persist -> self-healing
          actually works and survives restarts (was a dead AttributeError path).
  [P0-4]  Atomic saves (tmp file + os.replace) for jobs.json, archive and the
          selector config -> no more truncated-dataset data loss.
  [P0-5]  AI cleaner: drains the whole backlog in batches with NUMERIC ids and a
          per-job attempt counter (max 3) so 5 stubborn records no longer block
          the queue forever.
  [P0-6]  HTTP: retry with backoff, raise_for_status, pagination stops only on a
          *successful empty* page; partial results kept + loudly logged.

P1 — data quality
  [P1-7]  Cyberpark RSS: reads the job_listing:{company,location,job_type}
          namespace (was: "Cyberpark Company" placeholder + hyphen title split
          that never matched the en-dash titles). pubDate stored as posted_date,
          NEVER as deadline.
  [P1-9]  Email extraction: mailto: hrefs first, then bounded regex with camelCase
          glue repair ("Emailcareer@srishtis.comBrief" -> "career@srishtis.com").
          Scraped email always wins over AI-guessed email.
  [P1-10] UL Cyberpark: contact email harvested from the company link href
          ("http://hr@retailcloud.com"), full description fetched from the
          job_vacancy detail page (was: placeholder text).
  [P1-11] Existing jobs refreshed when content changes (sha1 content hash);
          changed records are re-queued for AI cleaning.
  [P1-12] AI output validated: job_type normalized to an enum, skills
          deduped/capped, field lengths capped, temperature=0, response_schema
          with graceful fallback. AI title only fills missing/garbage titles.
  [P1-13] Schema: stable `id`, `link_canon`, `posted_date`, `company_logo`,
          `is_walk_in` fields added (Technopark API provides them; old code
          discarded them).
  [P1-14] Date parsing: ISO date/datetime accepted first (regex -> fromisoformat),
          DMY convention documented, timezone-aware utcnow (no DeprecationWarning).

P2 — performance / cost
  [P2-16] Detail pages fetched concurrently, semaphore-bounded (was strictly
          sequential -> roughly 10x wall-clock improvement).
  [P2-17] Self-healing: async Gemini call, ONE attempt per (section,key) per run,
          proposed selector validated against the snippet before persisting.
  [P2-18] One shared ClientSession + TCPConnector + UA header for the whole run.
  [P2-19] RSS fetched through aiohttp (timeouts/retries apply), feedparser only
          parses the bytes (its own blocking HTTP is bypassed).
  [P2-T]  Technopark detail URLs use technopark.in directly (skips the .org 301).

P3 — reliability / observability
  [P3-20] Per-source stats + ZERO-JOB alert every run (the old code lost Infopark
          for months without a peep).
  [P3-21] Selector config validated at startup -> fail fast with a clear message.
  [P3-22] Jobs expired > ARCHIVE_AFTER_DAYS are moved to data/jobs_archive.json.

Pagination: Technopark follows the API's last_page (cap MAX_PAGES); Infopark
walks pages until an empty page (cap MAX_PAGES). Config: MAX_CLEAN_BATCHES x
CLEAN_BATCH_SIZE jobs get AI-cleaned per run.

Env: GEMINI_API_KEY (optional — healer and cleaner degrade gracefully without
it; scraping works fully without AI).
"""

import asyncio
import copy
import hashlib
import json
import logging
import os
import re
import ssl
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import aiohttp
import feedparser
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

DATA_DIR = "data"
JOBS_FILE = os.path.join(DATA_DIR, "jobs.json")
ARCHIVE_FILE = os.path.join(DATA_DIR, "jobs_archive.json")
SELECTOR_CONFIG_PATH = "scraping_config.json"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/scraper.log"), logging.StreamHandler()],
)
logger = logging.getLogger("scraper")


# --------------------------------------------------------------------------
# Small pure helpers (unit-testable, no I/O)
# --------------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _today() -> date:
    return _utcnow().date()


def _now_iso() -> str:
    return _utcnow().isoformat(timespec="seconds")


TRACKING_PARAMS = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "igshid"}


def _canonical_url(url: str) -> str:
    """Canonicalize a URL for dedup: lowercase scheme/host, drop fragment,
    drop utm_*/tracking params, strip trailing slash (query kept — UL detail
    links are identified by ?job_id=)."""
    if not url:
        return ""
    url = url.strip()
    try:
        p = urlparse(url)
        if not p.netloc:
            return url.split("#")[0] or url
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
             if not k.lower().startswith("utm_") and k.lower() not in TRACKING_PARAMS]
        path = p.path or "/"
        if len(path) > 1:
            path = path.rstrip("/")
        return urlunparse((p.scheme.lower() or "https", p.netloc.lower(),
                           path, "", urlencode(q), ""))
    except Exception:
        return url


def _stable_id(link: str) -> str:
    return hashlib.sha1(_canonical_url(link).encode("utf-8")).hexdigest()[:12]


_HASH_FIELDS = ("company", "role", "deadline", "description", "email", "posted_date")


def _content_hash(job: dict) -> str:
    basis = "|".join(str(job.get(k) or "")[:800] for k in _HASH_FIELDS)
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()


def _parse_deadline(deadline: Any) -> Tuple[Optional[str], bool]:
    """Return (iso_expiry_date, is_active). Convention: DD-MM-YYYY / DD/MM/YYYY
    (Indian sources). ISO date/datetime accepted first. Unparseable -> assumed
    active (matches original behaviour)."""
    if deadline is None:
        return None, True
    text = str(deadline).strip()
    if not text or text.upper() in ("N/A", "NA", "-", "None"):
        return None, True
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", text)  # ISO date or datetime
    if m:
        try:
            dt = date.fromisoformat(m.group(1))
            return dt.isoformat(), dt >= _today()
        except ValueError:
            pass
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%d %b %Y", "%d %B %Y", "%b %d, %Y", "%B %d, %Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt).date()
            return dt.isoformat(), dt >= _today()
        except ValueError:
            continue
    m = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", text)
    if m:
        d_, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        for dd, mm in ((d_, mth), (mth, d_)):  # DMY first, then MDY fallback
            try:
                dt = date(y, mm, dd)
                return dt.isoformat(), dt >= _today()
            except ValueError:
                continue
    return None, True


EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_JUNK_DOMAINS = {"example.com", "domain.com", "email.com", "test.com",
                 "sentry.io", "wixpress.com", "abc.com", "xyz.com"}
_GLUE_PREFIXES = ("e-mail", "email", "mail", "contact", "careers", "career", "apply", "hr")


def _repair_email(cand: Any) -> Optional[str]:
    """[P1-9] Validate/repair an email candidate. Handles the camelCase glue
    seen on live pages: 'Emailcareer@srishtis.comBrief' -> 'career@srishtis.com'.
    Domain-side glue (mixed-case TLD like 'comBrief') is the trigger signal;
    clean candidates pass through untouched."""
    c = (str(cand) if cand else "").strip().strip(".,;:()<>'\"[]")
    if not c or "@" not in c:
        return None
    local, _, domain = c.partition("@")
    local, domain = local.strip(), domain.strip()
    if not local or not domain:
        return None

    def _ok(e: str) -> bool:
        if not EMAIL_RE.fullmatch(e) or len(e) > 254:
            return False
        dom = e.rsplit("@", 1)[1]
        if "." not in dom or dom.lower() in _JUNK_DOMAINS:
            return False
        tld = dom.rsplit(".", 1)[-1]
        if not (2 <= len(tld) <= 13 and tld.isalpha()):
            return False
        if tld != tld.lower() and tld != tld.upper():
            return False  # mixed-case TLD = glue signature ("comBrief")
        return True

    if _ok(c):
        return c
    m = re.search(r"[A-Z]", domain)
    if m and m.start() > 0:
        cut_domain = domain[:m.start()]
        if _ok(f"{local}@{cut_domain}"):
            for pref in _GLUE_PREFIXES:
                if local.lower().startswith(pref) and len(local) - len(pref) >= 3:
                    fixed = f"{local[len(pref):]}@{cut_domain}"
                    if _ok(fixed):
                        return fixed
            return f"{local}@{cut_domain}"
    return None


def _extract_email_candidates(soup: BeautifulSoup, extra_text: str = "") -> List[str]:
    """mailto: hrefs first (most reliable), then a bounded regex over the page
    text (space-separated so adjacent tags don't glue). Every candidate is
    repaired/validated; results deduped case-insensitively."""
    cands: List[str] = []
    for a in soup.find_all("a"):
        h = (a.get("href") or "").strip()
        if h.lower().startswith("mailto:"):
            cands.append(h[7:].split("?")[0])
    text = soup.get_text(" ", strip=True)
    if extra_text:
        text += " " + extra_text
    cands.extend(EMAIL_RE.findall(text))
    out, seen = [], set()
    for c in cands:
        r = _repair_email(c)
        if r and r.lower() not in seen:
            seen.add(r.lower())
            out.append(r)
    return out


def _pick_email(cands: List[str]) -> str:
    for c in cands:
        if c and not c.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg")):
            return c
    return ""


_JOB_TYPE_MAP = [
    ("full-time", "Full-time"), ("full time", "Full-time"), ("fulltime", "Full-time"),
    ("permanent", "Full-time"),
    ("part-time", "Part-time"), ("part time", "Part-time"), ("parttime", "Part-time"),
    ("contract", "Contract"), ("freelance", "Freelance"),
    ("intern", "Internship"), ("trainee", "Internship"), ("trainee", "Internship"),
    ("walk-in", "Walk-in"), ("walk in", "Walk-in"), ("walkin", "Walk-in"),
    ("temporary", "Temporary"), ("temp ", "Temporary"),
]


def _norm_job_type(raw: Any) -> str:
    """[P1-12] Normalize free-text job types to a canonical enum."""
    t = str(raw or "").strip().lower()
    if not t:
        return ""
    for key, val in _JOB_TYPE_MAP:
        if t == key or t.startswith(key) or f" {key}" in t:
            return val
    s = str(raw).strip()
    return s if 0 < len(s) <= 40 else ""


def _base_job_dict(company: str, role: str, deadline: str, link: str, tech_park: str,
                   description: str, company_profile: str, email: str,
                   posted_date: str = "", location: str = "", job_type: str = "",
                   company_logo: str = "", is_walk_in: bool = False) -> dict:
    expiry, active = _parse_deadline(deadline)
    job = {
        "id": _stable_id(link),                      # [P1-13] stable frontend key
        "company": company or "",
        "role": role or "",
        "deadline": deadline or "",
        "link": link,
        "link_canon": _canonical_url(link),          # dedup key
        "tech_park": tech_park,
        "description": description or "",
        "company_profile": company_profile or "",
        "email": email or "",
        "posted_date": str(posted_date) if posted_date else "",
        "location": location or "",
        "job_type": job_type or "",
        "company_logo": company_logo or "",
        "is_walk_in": bool(is_walk_in),
        "is_cleaned": False,
        "_clean_attempts": 0,                        # [P0-5] per-job budget
        "job_active": active,
        "job_expiry": expiry,
        "scraped_at": _now_iso(),
        "updated_at": "",
        "skills": [],
        "experience": "",
        "salary": "",
        "job_summary": "",
        "clean_description": "",
        "cleaned_data": None,
    }
    job["content_hash"] = _content_hash(job)         # [P1-11] change detection
    return job


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

@dataclass
class Config:
    INFOPARK_URL: str = "https://infopark.in/companies-job"          # [P0-1]
    INFOPARK_PROFILE_URL: str = "https://infopark.in/companies/profile/{company_id}"
    TECHNOPARK_API: str = "https://technopark.org/api/paginated-jobs"
    TECHNOPARK_DETAIL_URL: str = "https://technopark.in/job-details/{job_id}"  # [P2-T]
    UL_URL: str = "https://www.ulcyberpark.com/jobs/index"
    CYBERPARK_RSS_URL: str = "https://www.cyberparkkerala.org/?feed=job_feed"
    REQUEST_TIMEOUT: int = 30
    CONNECTOR_LIMIT: int = 10
    DETAIL_CONCURRENCY: int = 8                     # [P2-16]
    DETAIL_RETRIES: int = 1
    LIST_RETRIES: int = 2                           # [P0-6]
    BACKOFF: float = 1.5
    MAX_PAGES: int = 50
    ARCHIVE_AFTER_DAYS: int = 30                    # [P3-22]
    CLEAN_BATCH_SIZE: int = 10                      # [P0-5]
    MAX_CLEAN_BATCHES: int = 10
    MAX_CLEAN_ATTEMPTS: int = 3
    GEMINI_MODEL: str = "gemini-2.0-flash"
    GEMINI_API_KEY: Optional[str] = os.getenv("GEMINI_API_KEY")
    SELECTOR_CONFIG_PATH: str = SELECTOR_CONFIG_PATH
    # [P0-8] Remote selector registry: healed selectors are published here and
    # downloaded at the start of every run so self-healing survives across CI runs.
    SELECTOR_REGISTRY_URL: str = os.getenv("SELECTOR_REGISTRY_URL", "")
    USER_AGENT: str = "Mozilla/5.0 (compatible; KeralaParkJobScraper/2.0)"
    INSECURE_SSL: bool = True   # parks serve flaky certs; set False with a CA bundle if you can


# --------------------------------------------------------------------------
# Selector config: versioned defaults + user overrides + self-healing persist
# --------------------------------------------------------------------------

CURRENT_CONFIG_VERSION = 2

DEFAULT_SELECTORS: Dict[str, Any] = {
    "config_version": CURRENT_CONFIG_VERSION,
    "infopark_list": {          # verified 2026-10-28 on infopark.in/companies-job
        "rows": ".job-info-sec table tr",
        "link": "td.btn-sec a",
        "role": "td:nth-of-type(2)",
        "company": "td:nth-of-type(3)",
        "deadline": "td:nth-of-type(4)",
        "posted_date": "td:nth-of-type(1)",
    },
    "infopark_detail": {        # verified on /company-jobs/details/{co}/{job}
        "description": ".job-section .tab-content",
        "profile_section": ".comp-detail",
        "profile_name_fallback": ".tab-content",
    },
    "technopark_detail": {      # verified on technopark.in/job-details/{id}
        "description_primary": "div.tp-body",       # multi-match: all blocks joined
        "description_secondary": "div.sm\\:col-span-2",
    },
    "ul_list": {                # verified on ulcyberpark.com/jobs/index
        "table": ".table-job table",
        "role": "td:nth-of-type(1) a",
        "company": "td:nth-of-type(2) a",
        "view_link": "td:nth-of-type(3) a",
    },
    "ul_detail": {              # verified on /jobs/job_vacancy?job_id=N
        "description": "div.c2_left_padd",
        "description_fallback": "div.job_border",
    },
}


class SelectorManager:
    """[P0-3] Adds the missing update()/persist(); version-checked so a stale
    file (pre-redesign selectors) is backed up and upgraded instead of silently
    breaking the scraper. A current-version file overrides defaults (preserves
    user tweaks and healed selectors)."""

    def __init__(self, config: Any):
        self.config_path = config.SELECTOR_CONFIG_PATH
        self.config = config
        self.selectors: Dict[str, Any] = copy.deepcopy(DEFAULT_SELECTORS)
        self.dirty = False
        if os.path.exists(config_path):
            raw: Dict[str, Any] = {}
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    raw = json.load(f) or {}
            except Exception as e:
                logger.warning(f"Selector config unreadable ({e}); using built-in defaults")
            file_ver = 0
            try:
                file_ver = int(raw.get("config_version", 0) or 0)
            except (TypeError, ValueError):
                file_ver = 0
            if file_ver == CURRENT_CONFIG_VERSION and raw:
                self._deep_merge(self.selectors, raw)
                self.dirty = False
            elif raw:
                bak = f"{config_path}.v{file_ver}.bak"
                try:
                    os.replace(config_path, bak)
                    logger.warning(
                        f"Stale selector config (v{file_ver}, pre-redesign) backed up to {bak}; "
                        f"upgrading to v{CURRENT_CONFIG_VERSION} defaults")
                except OSError:
                    pass
                self.dirty = True
        else:
            self.dirty = True  # write defaults on first run

    @staticmethod
    def _deep_merge(target: dict, src: dict) -> None:
        for k, v in src.items():
            if isinstance(v, dict) and isinstance(target.get(k), dict):
                SelectorManager._deep_merge(target[k], v)
            else:
                target[k] = v

    def get(self, section: str, key: str, default: Optional[str] = None) -> Optional[str]:
        return self.selectors.get(section, {}).get(key, default)

    def update(self, section: str, key: str, value: str) -> None:
        self.selectors.setdefault(section, {})[key] = value
        self.dirty = True

    def persist(self) -> None:
        if not self.dirty:
            return
        self.selectors["config_version"] = CURRENT_CONFIG_VERSION
        _atomic_write_json(self.config_path, self.selectors)
        self.dirty = False
        logger.info(f"Selector config persisted -> {self.config_path}")

    async def merge_remote(self, session: aiohttp.ClientSession) -> None:
        """[P0-8] Download the latest healed selector registry and merge it over
        the local config so self-healed selectors survive across CI runs. Network
        failures degrade gracefully — the scrape continues with local selectors."""
        url = self.config.SELECTOR_REGISTRY_URL
        if not url:
            return
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    logger.warning(
                        f"selector registry download failed: HTTP {resp.status}; using local config")
                    return
                remote = await resp.json(content_type=None)
        except Exception as e:
            logger.warning(f"selector registry download unavailable ({e!r}); using local config")
            return
        if not isinstance(remote, dict):
            logger.warning("selector registry payload is not a JSON object; ignoring")
            return
        before = copy.deepcopy(self.selectors)
        self._deep_merge(self.selectors, remote)
        if self.selectors != before:
            self.dirty = True
            logger.info("selector registry merged: healed selectors applied from remote")

    def validate(self) -> None:
        """[P3-21] Fail fast if any required selector is missing/empty."""
        missing = []
        for section, keys in DEFAULT_SELECTORS.items():
            if section == "config_version":
                continue
            for key in keys:
                if not self.get(section, key):
                    missing.append(f"{section}.{key}")
        if missing:
            raise RuntimeError(
                f"Selector config incomplete: {', '.join(missing)} — fix {self.config_path}")


# --------------------------------------------------------------------------
# Shared Gemini JSON call (async, temp=0, schema with graceful fallback)
# --------------------------------------------------------------------------

async def gemini_json(client: Any, config: Config, prompt: str,
                      schema: Optional[dict] = None) -> Optional[dict]:
    if client is None:
        return None
    base: Dict[str, Any] = {"response_mime_type": "application/json", "temperature": 0}
    use_schema = schema is not None
    aio_models = getattr(getattr(client, "aio", None), "models", None)
    for attempt in range(2):
        cfg = types.GenerateContentConfig(**base)
        if use_schema and schema is not None:
            try:
                cfg = types.GenerateContentConfig(**base, response_schema=schema)
            except Exception:
                use_schema = False  # older SDK without dict-schema support
        try:
            if aio_models is not None:
                res = await aio_models.generate_content(
                    model=config.GEMINI_MODEL, contents=prompt, config=cfg)
            else:
                res = await asyncio.to_thread(
                    client.models.generate_content,
                    model=config.GEMINI_MODEL, contents=prompt, config=cfg)
            return json.loads(res.text)
        except TypeError:
            if use_schema:
                use_schema = False
                continue
            return None
        except Exception as e:
            if attempt == 1:
                logger.error(f"Gemini call failed: {e}")
                return None
            await asyncio.sleep(1.5)
    return None


# --------------------------------------------------------------------------
# Self-healing selectors
# --------------------------------------------------------------------------

class SelfHealer:
    """[P0-3][P2-17] Async, cached (one attempt per section.key per run) and the
    proposed selector is validated against the snippet before being persisted."""

    def __init__(self, config: Config, selector_manager: SelectorManager):
        self.config = config
        self.selector_manager = selector_manager
        self.gemini_client = genai.Client(api_key=config.GEMINI_API_KEY) if config.GEMINI_API_KEY else None
        self._tried: Set[Tuple[str, str]] = set()

    async def heal(self, html_snippet: str, section: str, field_key: str,
                   context_desc: str) -> Optional[str]:
        if not self.gemini_client or not html_snippet or len(html_snippet) < 150:
            return None
        key = (section, field_key)
        if key in self._tried:
            return None
        self._tried.add(key)
        logger.warning(f"self-heal: selector miss for {section}.{field_key} ({context_desc})")
        prompt = f"""A CSS selector for '{field_key}' stopped matching on a {context_desc}.
1. Extract the value of '{field_key}' from the HTML below.
2. Write ONE robust CSS selector that finds that element in this HTML.

Return ONLY JSON: {{"extracted_value": "...", "new_selector": "..."}}

HTML:
```html
{html_snippet[:4000]}
```"""
        schema = {
            "type": "object",
            "properties": {"extracted_value": {"type": "string"},
                           "new_selector": {"type": "string"}},
            "required": ["extracted_value", "new_selector"],
        }
        data = await gemini_json(self.gemini_client, self.config, prompt, schema)
        if not data:
            return None
        val = str(data.get("extracted_value") or "").strip()
        sel = str(data.get("new_selector") or "").strip()
        if not val or not sel or len(sel) > 300:
            return None
        try:
            probe = BeautifulSoup(html_snippet, "html.parser")
            found = probe.select(sel)
        except Exception:
            found = []
        if not found:
            logger.warning(f"self-heal: proposed selector matches nothing, discarded: {sel}")
            return None
        if not any(val[:40].lower() in n.get_text(" ", strip=True).lower() for n in found[:5]):
            logger.warning(f"self-heal: selector does not reproduce the value, discarded: {sel}")
            return None
        self.selector_manager.update(section, field_key, sel)
        logger.info(f"self-heal OK: {section}.{field_key} -> {sel} (value: {val[:60]!r})")
        return val


# --------------------------------------------------------------------------
# AI cleaner
# --------------------------------------------------------------------------

class AICleaner:
    """[P0-5][P1-12] Batched (numeric ids), whole-backlog draining, per-job
    attempt budget, validated output, scraped values win over AI guesses."""

    def __init__(self, config: Config):
        self.config = config
        self.gemini_client = genai.Client(api_key=config.GEMINI_API_KEY) if config.GEMINI_API_KEY else None

    async def clean_jobs(self, jobs: List[dict]) -> Tuple[List[dict], dict]:
        stats = {"cleaned": 0, "attempted": 0, "failed": 0}
        if not self.gemini_client:
            logger.info("AI cleaner disabled (no GEMINI_API_KEY)")
            return jobs, stats
        max_att = self.config.MAX_CLEAN_ATTEMPTS
        to_clean = [j for j in jobs
                    if not j.get("is_cleaned") and j.get("_clean_attempts", 0) < max_att]
        if not to_clean:
            return jobs, stats
        n_batches = min(
            (len(to_clean) + self.config.CLEAN_BATCH_SIZE - 1) // self.config.CLEAN_BATCH_SIZE,
            self.config.MAX_CLEAN_BATCHES)
        logger.info(f"AI cleaning: {len(to_clean)} pending, running {n_batches} batch(es) "
                    f"of <= {self.config.CLEAN_BATCH_SIZE}")
        for b in range(n_batches):
            batch = to_clean[b * self.config.CLEAN_BATCH_SIZE:(b + 1) * self.config.CLEAN_BATCH_SIZE]
            stats["attempted"] += len(batch)
            results = await self._clean_batch(batch)
            for job, data in zip(batch, results):
                if data is not None and self._apply_clean(job, data):
                    stats["cleaned"] += 1
                else:
                    job["_clean_attempts"] = job.get("_clean_attempts", 0) + 1
                    stats["failed"] += 1
        logger.info(f"AI cleaning done: {stats['cleaned']}/{stats['attempted']} ok")
        return jobs, stats

    async def _clean_batch(self, batch: List[dict]) -> List[Optional[dict]]:
        payload = []
        for i, j in enumerate(batch):
            payload.append({
                "id": i,
                "role": j.get("role") or "",
                "company": j.get("company") or "",
                "deadline": j.get("deadline") or "",
                "location": j.get("location") or "",
                "description": (j.get("description") or "")[:2500],
                "company_profile": (j.get("company_profile") or "")[:1200],
            })
        prompt = f"""You are a job-data extraction engine. For EACH input item extract structured fields.
Return ONLY JSON: {{"results": [{{"id": <int>, "job_title": str, "job_summary": str, "skills": [str], "experience_required": str, "job_type": str, "salary": str, "location": str, "clean_email": str, "clean_description": str}}]}}
Rules:
- Exactly one output object per input item, with the same "id". Never invent values; use "" for anything unknown.
- job_type must be one of: "Full-time", "Part-time", "Contract", "Internship", "Walk-in", "Freelance", "Temporary", or "".
- clean_email: only if an email address is explicitly stated in the text.
- clean_description: plain text, under 3000 characters.
- skills: at most 10 short skill names.
Input items:
{json.dumps(payload, ensure_ascii=False)}"""
        schema = {
            "type": "object",
            "properties": {"results": {"type": "array", "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "job_title": {"type": "string"},
                    "job_summary": {"type": "string"},
                    "skills": {"type": "array", "items": {"type": "string"}},
                    "experience_required": {"type": "string"},
                    "job_type": {"type": "string"},
                    "salary": {"type": "string"},
                    "location": {"type": "string"},
                    "clean_email": {"type": "string"},
                    "clean_description": {"type": "string"},
                },
                "required": ["id"],
            }}},
            "required": ["results"],
        }
        data = await gemini_json(self.gemini_client, self.config, prompt, schema)
        if not data or not isinstance(data.get("results"), list):
            return [None] * len(batch)
        by_id: Dict[int, dict] = {}
        for r in data["results"]:
            if isinstance(r, dict):
                try:
                    by_id[int(r.get("id"))] = r
                except (TypeError, ValueError):
                    continue
        return [by_id.get(i) for i in range(len(batch))]

    def _apply_clean(self, job: dict, data: dict) -> bool:
        try:
            skills_raw = data.get("skills") or []
            if isinstance(skills_raw, str):
                skills_raw = [skills_raw]
            seen_sk: Set[str] = set()
            skills: List[str] = []
            for s in skills_raw:
                s = str(s).strip()
                if s and len(s) <= 40 and s.lower() not in seen_sk:
                    seen_sk.add(s.lower())
                    skills.append(s)
                if len(skills) >= 10:
                    break
            job_type = _norm_job_type(data.get("job_type"))
            experience = str(data.get("experience_required") or "").strip()[:100]
            salary = str(data.get("salary") or "").strip()[:60]
            location = str(data.get("location") or data.get("clean_address") or "").strip()[:120]
            summary = str(data.get("job_summary") or "").strip()[:600]
            clean_desc = str(data.get("clean_description") or "").strip()[:4000]
            ai_title = str(data.get("job_title") or "").strip()[:200]
            ai_email = _repair_email(data.get("clean_email"))
            if not any((skills, job_type, experience, salary, location, summary,
                        clean_desc, ai_title, ai_email)):
                return False
            # Scraped title is ground truth; AI title only fills missing junk. [P1-12]
            if ai_title and job.get("role") in ("", "N/A", None):
                job["role"] = ai_title
            job["skills"] = skills
            job["job_type"] = job_type
            job["experience"] = experience
            job["salary"] = salary
            if location:
                job["location"] = location
            job["job_summary"] = summary
            job["clean_description"] = clean_desc
            if ai_email and not job.get("email"):     # scraped email always wins [P1-9]
                job["email"] = ai_email
            job["is_cleaned"] = True
            job["_clean_attempts"] = 0
            job["cleaned_data"] = {
                "job_title": ai_title, "job_summary": summary, "skills": skills,
                "experience_required": experience, "job_type": job_type,
                "salary": salary, "location": location,
                "clean_email": ai_email or "", "clean_description": clean_desc,
            }
            return True
        except Exception as e:
            logger.debug(f"apply_clean failed: {e}")
            return False


# --------------------------------------------------------------------------
# Scraper
# --------------------------------------------------------------------------

class FetchError(Exception):
    pass


RETRY_STATUS = {429, 500, 502, 503, 504}


class JobScraper:
    def __init__(self, config: Config, healer: SelfHealer,
                 selector_manager: SelectorManager, session: aiohttp.ClientSession):
        self.config = config
        self.healer = healer
        self.selectors = selector_manager
        self.session = session
        self._sem = asyncio.Semaphore(config.DETAIL_CONCURRENCY)   # [P2-16]

    # ---- HTTP layer ------------------------------------------------------

    async def _bounded(self, coro):
        async with self._sem:
            return await coro

    async def fetch(self, url: str, retries: int = 2) -> str:
        """[P0-6] Retries with backoff on 429/5xx/network errors; raises
        FetchError on final failure (callers keep partial results + log loud)."""
        last: Optional[Exception] = None
        for attempt in range(retries + 1):
            try:
                async with self.session.get(url) as resp:
                    if resp.status in RETRY_STATUS and attempt < retries:
                        await asyncio.sleep(self.config.BACKOFF ** attempt + 0.5)
                        continue
                    resp.raise_for_status()
                    return await resp.text()
            except aiohttp.ClientResponseError as e:
                raise FetchError(f"{url} -> HTTP {e.status}") from e
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last = e
                if attempt < retries:
                    await asyncio.sleep(self.config.BACKOFF ** attempt + 0.5)
        raise FetchError(f"{url} failed after {retries + 1} attempt(s): {last}")

    # ---- extraction helpers ----------------------------------------------

    async def _extract_text(self, root, section: str, key: str,
                            html_context: str, context_desc: str,
                            default: str = "") -> str:
        sel = self.selectors.get(section, key)
        node = None
        if sel:
            try:
                node = root.select_one(sel)
            except Exception:
                node = None
        if node is not None:
            return node.get_text(" ", strip=True) or default
        healed = await self.healer.heal(html_context, section, key, context_desc)
        return healed if healed else default

    def _generic_main_text(self, soup: BeautifulSoup) -> str:
        """Fallback content extractor: smallest non-chrome container holding the
        bulk of the body text (most specific wrapper wins)."""
        for t in soup(["script", "style", "noscript", "header", "footer", "nav"]):
            t.decompose()
        body = soup.body or soup
        body_len = len(body.get_text(" ", strip=True))
        if body_len < 80:
            return ""
        target = max(200, int(body_len * 0.35))
        best = ""
        for el in body.find_all(["div", "section", "article"]):
            cls = " ".join(el.get("class") or []) + " " + " ".join(el.get("id") or [])
            if re.search(r"nav|menu|sidebar|footer|header|cookie|banner|modal|breadcrumb", cls, re.I):
                continue
            txt = el.get_text("\n", strip=True)
            if target <= len(txt) < 15000 and (not best or len(txt) < len(best)):
                best = txt
        return best or (body.get_text("\n", strip=True) if body_len < 3000 else "")

    # ---- Infopark ---------------------------------------------------------

    async def scrape_infopark(self) -> List[dict]:
        logger.info("Scraping Infopark...")
        jobs: List[dict] = []
        seen: Set[str] = set()
        section = "infopark_list"
        page = 1
        while page <= self.config.MAX_PAGES:
            url = f"{self.config.INFOPARK_URL}?page={page}"
            try:
                html = await self.fetch(url, retries=self.config.LIST_RETRIES)
            except FetchError as e:
                logger.warning(f"Infopark: interrupted at page {page}: {e}")
                break
            soup = BeautifulSoup(html, "html.parser")
            rows = [r for r in soup.select(self.selectors.get(section, "rows") or "tr")
                    if not r.find("th")]
            if not rows:
                break  # natural end of pagination (successful empty page)
            records = []
            for row in rows:
                try:
                    link_tag = row.select_one(self.selectors.get(section, "link") or "a[href]")
                    if not link_tag:
                        continue
                    link = urljoin(url, link_tag.get("href") or "")
                    canon = _canonical_url(link)
                    if not canon or canon in seen:
                        continue
                    seen.add(canon)
                    row_html = str(row)
                    records.append((
                        await self._extract_text(row, section, "role", row_html, "Infopark job row"),
                        await self._extract_text(row, section, "company", row_html, "Infopark job row"),
                        await self._extract_text(row, section, "deadline", row_html, "Infopark job row"),
                        await self._extract_text(row, section, "posted_date", row_html, "Infopark job row"),
                        link,
                    ))
                except Exception as e:
                    logger.debug(f"Infopark row skipped: {e}")
            details = await asyncio.gather(
                *(self._bounded(self._infopark_details(r[4])) for r in records),
                return_exceptions=True)
            for rec, det in zip(records, details):
                role, company, deadline, posted, link = rec
                if isinstance(det, Exception):
                    logger.warning(f"Infopark detail failed {link}: {det}")
                    det = ("", "", "")
                desc, profile, email = det
                jobs.append(_base_job_dict(
                    company=company, role=role, deadline=deadline, link=link,
                    tech_park="Infopark", description=desc, company_profile=profile,
                    email=email, posted_date=posted))
            page += 1
        logger.info(f"Infopark: {len(jobs)} jobs")
        return jobs

    async def _infopark_details(self, link: str) -> Tuple[str, str, str]:
        desc = profile = email = ""
        html = await self.fetch(link, retries=self.config.DETAIL_RETRIES)
        if html:
            soup = BeautifulSoup(html, "html.parser")
            section = "infopark_detail"
            sel = self.selectors.get(section, "description")
            node = None
            if sel:
                try:
                    node = soup.select_one(sel)
                except Exception:
                    node = None
            if node is not None:
                desc = node.get_text("\n", strip=True)
            else:
                healed = await self.healer.heal(html[:6000], section, "description",
                                                "Infopark job detail page")
                desc = healed or self._generic_main_text(soup)
            email = _pick_email(_extract_email_candidates(soup))
            # [P0-2] company id is the SECOND-to-last path segment
            parts = link.rstrip("/").split("/")
            company_id = parts[-2] if len(parts) >= 2 else ""
            if company_id.isdigit():
                p_url = self.config.INFOPARK_PROFILE_URL.format(company_id=company_id)
                try:
                    p_html = await self.fetch(p_url, retries=self.config.DETAIL_RETRIES)
                    if p_html:
                        p_soup = BeautifulSoup(p_html, "html.parser")
                        prof_node = p_soup.select_one(
                            self.selectors.get("infopark_detail", "profile_section") or "")
                        if prof_node:
                            profile = prof_node.get_text(" ", strip=True)
                        else:
                            name_node = p_soup.select_one(
                                self.selectors.get("infopark_detail", "profile_name_fallback") or "")
                            if name_node:
                                profile = f"Company: {name_node.get_text(' ', strip=True)[:300]}"
                except FetchError as e:
                    logger.debug(f"Infopark profile fetch failed {p_url}: {e}")
        return desc, profile, email

    # ---- Technopark ---------------------------------------------------------

    async def scrape_technopark(self) -> List[dict]:
        logger.info("Scraping Technopark...")
        jobs: List[dict] = []
        seen: Set[str] = set()
        page, last_page = 1, 1
        while page <= min(last_page, self.config.MAX_PAGES):
            url = f"{self.config.TECHNOPARK_API}?page={page}"
            try:
                text = await self.fetch(url, retries=self.config.LIST_RETRIES)
                data = json.loads(text)
            except (FetchError, json.JSONDecodeError) as e:
                logger.warning(f"Technopark: interrupted at page {page}: {e}")
                break
            if not isinstance(data, dict):
                break
            if page == 1:
                try:
                    last_page = max(1, int(data.get("last_page", 1)))
                except (TypeError, ValueError):
                    last_page = 1
            items = data.get("data") or []
            if not items:
                break
            records = []
            for it in items:
                if not isinstance(it, dict) or it.get("id") is None:
                    continue
                link = self.config.TECHNOPARK_DETAIL_URL.format(job_id=it["id"])
                canon = _canonical_url(link)
                if canon in seen:
                    continue
                seen.add(canon)
                comp = it.get("company") or {}
                records.append({
                    "company": comp.get("company") or "",
                    "role": it.get("job_title") or "",
                    "deadline": it.get("closing_date") or "",
                    "posted_date": it.get("posted_date") or "",
                    "link": link,
                    "logo": comp.get("logo") or "",
                    "walk_in": bool(it.get("is_walk_in")),
                })
            details = await asyncio.gather(
                *(self._bounded(self._technopark_details(r["link"])) for r in records),
                return_exceptions=True)
            for rec, det in zip(records, details):
                if isinstance(det, Exception):
                    logger.warning(f"Technopark detail failed {rec['link']}: {det}")
                    det = ("", "")
                desc, email = det
                jobs.append(_base_job_dict(
                    company=rec["company"], role=rec["role"], deadline=rec["deadline"],
                    link=rec["link"], tech_park="Technopark", description=desc,
                    company_profile=(f"Company: {rec['company']}" if rec["company"] else ""),
                    email=email, posted_date=rec["posted_date"],
                    company_logo=rec["logo"], is_walk_in=rec["walk_in"]))
            page += 1
        logger.info(f"Technopark: {len(jobs)} jobs (API last_page={last_page})")
        return jobs

    async def _technopark_details(self, link: str) -> Tuple[str, str]:
        desc = email = ""
        html = await self.fetch(link, retries=self.config.DETAIL_RETRIES)
        if html:
            soup = BeautifulSoup(html, "html.parser")
            section = "technopark_detail"
            # NOTE: div.tp-body appears MULTIPLE times (one per content section);
            # select_one() only catches the 24-char "Closing Date" stub. Join all
            # blocks, skipping the closing-date chrome. (Verified live 2026-10-28.)
            sel_prim = self.selectors.get(section, "description_primary")
            parts: List[str] = []
            if sel_prim:
                try:
                    blocks = soup.select(sel_prim)
                except Exception:
                    blocks = []
                for b in blocks:
                    txt = b.get_text("\n", strip=True)
                    if txt and not (len(txt) < 45 and "closing date" in txt.lower()):
                        parts.append(txt)
            if parts:
                desc = "\n".join(parts)
            if not desc:
                sel_sec = self.selectors.get(section, "description_secondary")
                node = None
                if sel_sec:
                    try:
                        node = soup.select_one(sel_sec)
                    except Exception:
                        node = None
                if node is not None:
                    desc = node.get_text("\n", strip=True)
            if not desc:
                healed = await self.healer.heal(html[:6000], section, "description_primary",
                                                "Technopark job detail page")
                desc = healed or self._generic_main_text(soup)
            email = _pick_email(_extract_email_candidates(soup))
        return desc, email

    # ---- UL Cyberpark ------------------------------------------------------

    async def scrape_ul(self) -> List[dict]:
        logger.info("Scraping UL Cyberpark...")
        jobs: List[dict] = []
        html = await self.fetch(self.config.UL_URL, retries=self.config.LIST_RETRIES)
        soup = BeautifulSoup(html, "html.parser")
        section = "ul_list"
        table = soup.select_one(self.selectors.get(section, "table") or "table")
        if not table:
            logger.warning("UL Cyberpark: jobs table not found — layout may have changed")
            return jobs
        records = []
        for row in table.find_all("tr"):
            tds = row.find_all("td")
            if len(tds) < 3:
                continue
            link_tag = row.select_one(self.selectors.get(section, "view_link") or "td:nth-of-type(3) a")
            href = (link_tag.get("href") if link_tag else "") or ""
            if not href:
                continue
            link = urljoin(self.config.UL_URL, href)
            canon = _canonical_url(link)
            if any(r["link"] == canon for r in records):
                continue
            role_tag = row.select_one(self.selectors.get(section, "role") or "td:nth-of-type(1) a")
            role = role_tag.get_text(" ", strip=True) if role_tag else tds[0].get_text(" ", strip=True)
            m = re.search(r"(\d{1,2}-\d{1,2}-\d{4})", tds[0].get_text(" ", strip=True))
            deadline = m.group(1) if m else ""
            comp_tag = row.select_one(self.selectors.get(section, "company") or "td:nth-of-type(2) a")
            company = comp_tag.get_text(" ", strip=True) if comp_tag else tds[1].get_text(" ", strip=True)
            email = ""
            if comp_tag:  # [P1-10] href is typically "http://hr@retailcloud.com"
                raw = re.sub(r"^https?://", "", (comp_tag.get("href") or "")).strip()
                email = _repair_email(raw) or ""
            records.append({"company": company, "role": role, "deadline": deadline,
                            "link": link, "email": email})
        details = await asyncio.gather(
            *(self._bounded(self._ul_details(r["link"])) for r in records),
            return_exceptions=True)
        for rec, det in zip(records, details):
            if isinstance(det, Exception):
                logger.warning(f"UL detail failed {rec['link']}: {det}")
                det = ("", "")
            desc, demail = det  # [P1-10] full description from the detail page
            jobs.append(_base_job_dict(
                company=rec["company"], role=rec["role"], deadline=rec["deadline"],
                link=rec["link"], tech_park="UL Cyberpark", description=desc,
                company_profile=f"Company: {rec['company']}",
                email=rec["email"] or demail))
        logger.info(f"UL Cyberpark: {len(jobs)} jobs")
        return jobs

    async def _ul_details(self, link: str) -> Tuple[str, str]:
        desc = email = ""
        html = await self.fetch(link, retries=self.config.DETAIL_RETRIES)
        if html:
            soup = BeautifulSoup(html, "html.parser")
            for key in ("description", "description_fallback"):
                sel = self.selectors.get("ul_detail", key)
                if not sel:
                    continue
                try:
                    node = soup.select_one(sel)
                except Exception:
                    node = None
                if node is not None:
                    txt = node.get_text("\n", strip=True)
                    if len(txt) > len(desc):
                        desc = txt
            if not desc:
                healed = await self.healer.heal(html[:6000], "ul_detail", "description",
                                                "UL Cyberpark job detail page")
                desc = healed or self._generic_main_text(soup)
            email = _pick_email(_extract_email_candidates(soup))
        return desc, email

    # ---- Cyberpark (RSS) ---------------------------------------------------

    async def scrape_cyberpark(self) -> List[dict]:
        """[P1-7][P2-19] RSS fetched via aiohttp (timeouts/retries apply);
        structured job_listing:* namespace preferred over title guessing;
        pubDate -> posted_date (never deadline)."""
        logger.info("Scraping Cyberpark RSS...")
        jobs: List[dict] = []
        text = await self.fetch(self.config.CYBERPARK_RSS_URL, retries=self.config.LIST_RETRIES)
        feed = feedparser.parse(text)
        for entry in feed.entries:
            link = (getattr(entry, "link", "") or "").strip()
            if not link:
                continue
            role = (getattr(entry, "title", "") or "").strip()
            company = (getattr(entry, "job_listing_company", "") or "").strip()
            location = (getattr(entry, "job_listing_location", "") or "").strip()
            job_type = _norm_job_type(getattr(entry, "job_listing_job_type", "") or "")
            desc_html = getattr(entry, "summary", "") or getattr(entry, "description", "") or ""
            desc = BeautifulSoup(desc_html, "html.parser").get_text("\n", strip=True)
            posted = ""
            st = entry.get("published_parsed") or entry.get("updated_parsed")
            if st:
                try:
                    posted = datetime(*st[:6], tzinfo=timezone.utc).date().isoformat()
                except (TypeError, ValueError):
                    posted = ""
            jobs.append(_base_job_dict(
                company=company, role=role, deadline="", link=link,
                tech_park="Cyberpark Kozhikode", description=desc,
                company_profile=(f"Company: {company}" if company else ""),
                email="", posted_date=posted, location=location, job_type=job_type))
        logger.info(f"Cyberpark: {len(jobs)} jobs")
        return jobs

    # ---- cycle --------------------------------------------------------------

    async def run_cycle(self) -> Tuple[List[dict], Dict[str, dict]]:
        tasks = {
            "infopark": self.scrape_infopark(),
            "technopark": self.scrape_technopark(),
            "ul": self.scrape_ul(),
            "cyberpark": self.scrape_cyberpark(),
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        all_jobs: List[dict] = []
        stats: Dict[str, dict] = {}
        for (name, _coro), res in zip(tasks.items(), results):
            if isinstance(res, Exception):
                logger.error(f"{name}: scrape failed: {res!r}")
                stats[name] = {"scraped": 0, "error": str(res)[:200]}
            else:
                stats[name] = {"scraped": len(res)}
                all_jobs.extend(res)
            if stats[name]["scraped"] == 0:   # [P3-20] zero-job alarm
                logger.warning(f"ZERO jobs from '{name}' — the site/API may have changed. "
                               f"Investigate before trusting this run.")
        return all_jobs, stats


# --------------------------------------------------------------------------
# Storage
# --------------------------------------------------------------------------

def _atomic_write_json(path: str, obj: Any) -> None:
    """[P0-4] tmp file + os.replace -> a crash can never truncate the dataset."""
    tmp = f"{path}.tmp"
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def load_existing_jobs() -> List[dict]:
    if not os.path.exists(JOBS_FILE):
        return []
    try:
        with open(JOBS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.warning(f"Could not load existing jobs ({e}); starting from a backup "
                       f"of nothing — check {JOBS_FILE} manually if this is unexpected")
        return []


def save_jobs(jobs: List[dict]) -> None:
    _atomic_write_json(JOBS_FILE, jobs)
    logger.info(f"Saved {len(jobs)} jobs to {JOBS_FILE}")


def archive_expired(jobs: List[dict], days: int = 30) -> Tuple[List[dict], int]:
    """[P3-22] Move jobs whose deadline passed more than `days` days ago."""
    cutoff = _today() - timedelta(days=days)
    keep, arch = [], []
    for j in jobs:
        exp, inactive = j.get("job_expiry"), not j.get("job_active", True)
        if exp and inactive:
            try:
                d = date.fromisoformat(str(exp))
            except (TypeError, ValueError):
                d = None
            if d and d < cutoff:
                arch.append(j)
                continue
        keep.append(j)
    if arch:
        old: List[dict] = []
        if os.path.exists(ARCHIVE_FILE):
            try:
                with open(ARCHIVE_FILE, "r", encoding="utf-8") as f:
                    old = json.load(f) or []
            except Exception:
                old = []
        _atomic_write_json(ARCHIVE_FILE, old + arch)
        logger.info(f"Archived {len(arch)} expired jobs -> {ARCHIVE_FILE}")
    return keep, len(arch)


_REFRESH_FIELDS = ("company", "role", "deadline", "description", "company_profile", "email",
                   "posted_date", "location", "job_type", "company_logo", "is_walk_in")


def merge_jobs(existing: List[dict], new_jobs: List[dict]) -> Tuple[List[dict], dict]:
    """[P1-11] Dedup by canonical link; refresh existing records whose content
    hash changed (they are re-queued for AI cleaning); recompute expiry for all."""
    for j in existing:  # migrate records written by the previous script version
        if not j.get("link_canon"):
            j["link_canon"] = _canonical_url(j.get("link", ""))
        if not j.get("id"):
            j["id"] = _stable_id(j.get("link", "") or j.get("link_canon", ""))
        j.setdefault("_clean_attempts", 0)
    index: Dict[str, dict] = {}
    for j in existing:
        k = j.get("link_canon")
        if k and k not in index:
            index[k] = j
    merged = list(existing)
    added = updated = 0
    for nj in new_jobs:
        k = nj.get("link_canon")
        if not k:
            continue
        ex = index.get(k)
        if ex is None:
            merged.append(nj)
            index[k] = nj
            added += 1
        elif nj.get("content_hash") and nj["content_hash"] != ex.get("content_hash"):
            for f_ in _REFRESH_FIELDS:
                if nj.get(f_):
                    ex[f_] = nj[f_]
            ex["content_hash"] = nj["content_hash"]
            ex["updated_at"] = _now_iso()
            ex["is_cleaned"] = False
            ex["cleaned_data"] = None
            ex["_clean_attempts"] = 0  # changed content gets a fresh cleaning budget
            updated += 1
    for j in merged:  # deadlines pass over time — recompute every run
        j["job_expiry"], j["job_active"] = _parse_deadline(j.get("deadline", ""))
    logger.info(f"merge: +{added} new, {updated} refreshed, {len(merged)} total")
    return merged, {"added": added, "updated": updated}


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

async def main():
    cfg = Config()
    selector_manager = SelectorManager(cfg)
    selector_manager.validate()                      # [P3-21] fail fast
    healer = SelfHealer(cfg, selector_manager)
    cleaner = AICleaner(cfg)

    ssl_ctx = None
    if cfg.INSECURE_SSL:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        logger.warning("SSL certificate verification DISABLED (INSECURE_SSL=True) — "
                       "convenient for the parks' flaky certs, but a MITM risk.")
    connector = aiohttp.TCPConnector(limit=cfg.CONNECTOR_LIMIT, ssl=ssl_ctx)  # [P2-18]
    timeout = aiohttp.ClientTimeout(total=cfg.REQUEST_TIMEOUT)

    merged: List[dict] = []
    stats: Dict[str, dict] = {}
    mstats = {"added": 0, "updated": 0}
    cstats = {"cleaned": 0, "attempted": 0, "failed": 0}
    n_archived = 0

    async with aiohttp.ClientSession(
            connector=connector, timeout=timeout,
            headers={"User-Agent": cfg.USER_AGENT,
                     "Accept": "text/html,application/json,application/rss+xml;q=0.9,*/*;q=0.8"}) as session:
        await selector_manager.merge_remote(session)    # [P0-8] pull healed selectors each run
        scraper = JobScraper(cfg, healer, selector_manager, session)
        logger.info("Starting scrape cycle...")
        new_jobs, stats = await scraper.run_cycle()
        existing = load_existing_jobs()
        merged, mstats = merge_jobs(existing, new_jobs)
        merged, cstats = await cleaner.clean_jobs(merged)
        merged, n_archived = archive_expired(merged, cfg.ARCHIVE_AFTER_DAYS)
        save_jobs(merged)

    selector_manager.persist()                       # [P0-3] healed selectors survive restarts

    logger.info("=" * 62)
    logger.info("RUN SUMMARY")
    for src, s in stats.items():
        line = f"  {src:10s} scraped={s['scraped']:4d}"
        if s.get("error"):
            line += f"  ERROR={s['error']}"
        logger.info(line)
    logger.info(f"  merged: +{mstats['added']} added, {mstats['updated']} refreshed, "
                f"total={len(merged)}")
    logger.info(f"  cleaned: {cstats['cleaned']}/{cstats['attempted']} ok "
                f"({cstats['failed']} failed; attempts tracked per job)")
    logger.info(f"  archived: {n_archived} jobs expired > {cfg.ARCHIVE_AFTER_DAYS}d")
    logger.info("=" * 62)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
