# Techpark Jobs Pipeline 🚀

A serverless job scraper that aggregates job listings from major IT parks in Kerala (Infopark, Technopark, UL Cyberpark, and Cyberpark Kozhikode) into a committed `data/jobs.json` file.

## Architecture (Serverless)

1. **GitHub Actions** runs `scrape.py` every 6 hours (and on `workflow_dispatch`).
2. The scraper writes/updates `data/jobs.json` in this repo.
3. Your Vercel front-end loads data from the raw GitHub URL:
   `https://raw.githubusercontent.com/<owner>/<repo>/<branch>/data/jobs.json`

No always-on server, no MySQL, no Redis.

## Features

- Multi-source aggregation from Infopark, Technopark, UL Cyberpark, and Cyberpark RSS.
- Asynchronous scraping with `aiohttp` / `asyncio`.
- Smart deduplication by job link against the committed `data/jobs.json`.
- Optional AI cleaning via Gemini when `GEMINI_API_KEY` is set.
- Self-healing CSS selectors for extraction.
- Commits updated `data/jobs.json` automatically when data changes.

## Secrets (GitHub Actions)

Add these in your repo **Settings → Secrets and variables → Actions**:

| Secret | Description |
|--------|-------------|
| `GEMINI_API_KEY` | Gemini API key for AI cleaning / self-healing (optional) |

The `GITHUB_TOKEN` is provided automatically and used to commit `data/jobs.json`.

## Running Locally

```bash
# Install deps
pip install -r requirements.txt

# Create .env (optional, for AI cleaning / self-healing)
cat > .env <<EOF
GEMINI_API_KEY=your_gemini_key
EOF

# Run once
python scraper_patched.py
```

This creates/updates `data/jobs.json` and archives expired jobs to `data/jobs_archive.json`.

## Data Format

`data/jobs.json` is a JSON array of job objects:

```json
[
  {
    "company": "Acme Corp",
    "role": "Software Engineer",
    "deadline": "15-12-2026",
    "link": "https://...",
    "link_canon": "https://...",
    "id": "a1b2c3d4e5f6",
    "tech_park": "Infopark",
    "description": "...",
    "company_profile": "...",
    "email": "hr@acme.com",
    "posted_date": "2026-10-25",
    "location": "Kochi",
    "job_type": "Full-time",
    "company_logo": "https://.../logo.png",
    "is_walk_in": false,
    "is_cleaned": false,
    "job_active": true,
    "job_expiry": "2026-12-15",
    "scraped_at": "2026-10-28T10:03:08Z",
    "updated_at": "",
    "job_type": "Full-time",
    "experience": "2-4 years",
    "skills": ["Python", "React"],
    "salary": "₹6-8 LPA",
    "job_summary": "...",
    "clean_description": "...",
    "content_hash": "sha1...",
    "cleaned_data": { ... }
  }
]

| Field | Description |
|-------|-------------|
| `id` | Stable frontend key (sha1 of canonical link) |
| `link_canon` | Canonicalized URL used for dedup |
| `posted_date` | Date the job was posted (ISO) |
| `company_logo` | Company logo URL |
| `is_walk_in` | Whether it's a walk-in interview |
| `is_cleaned` | Whether AI cleaning was applied |
| `job_active` | Whether the deadline hasn't passed yet |
| `job_expiry` | Parsed expiry date (ISO) or `null` |
| `scraped_at` | UTC timestamp when the job was first scraped |
| `updated_at` | Last content-refresh timestamp |
| `job_type` | Full-time / Part-time / Contract (AI-extracted) |
| `experience` | Years of experience required (AI-extracted) |
| `skills` | List of required skills (AI-extracted) |
| `salary` | Salary range (AI-extracted) |
| `location` | Job location (AI-extracted) |
| `job_summary` | AI summary (AI-extracted) |
| `clean_description` | AI-cleaned description (AI-extracted) |
| `content_hash` | sha1 of key fields; changed records are re-cleaned |
| `cleaned_data` | Full AI-cleaned object (when `is_cleaned` is true) |

Expired jobs (> `ARCHIVE_AFTER_DAYS`, default 30) are moved to `data/jobs_archive.json`.

## Loading in Vercel

Fetch the raw file client-side (or at build time with ISR):

```js
const res = await fetch(
  "https://raw.githubusercontent.com/<owner>/<repo>/<branch>/data/jobs.json"
);
const jobs = await res.json();
```

> Note: GitHub raw URLs are CDN-cached. Add a cache-busting query param or use short revalidation if you need near-real-time freshness.

## Configuration

Edit `scraping_config.json` for CSS selectors per source. Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `GEMINI_API_KEY` | — | Gemini key for cleaning/self-healing |

## Scheduling

Defined in `.github/workflows/scrape.yml` — runs every 6 hours via `cron: '0 */6 * * *'`.
