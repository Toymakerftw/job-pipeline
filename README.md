# Techpark Jobs Pipeline 🚀

A robust, asynchronous Python scraper that aggregates job listings from major IT parks in Kerala (Infopark, Technopark, UL Cyberpark, and Cyberpark Kozhikode) into a single SQLite database.

## Features

- **Multi-Source Aggregation**: Fetches data from Infopark, Technopark, UL Cyberpark, and Cyberpark RSS feeds.
- **Asynchronous Scraping**: Built with `aiohttp` and `asyncio` for high-performance data retrieval.
- **Data Persistence**: Stores job details, company profiles, and contact emails in a local SQLite database (`jobs.db`).
- **Deduplication**: Uses unique job links to ensure no duplicate entries are stored.
- **Clean Text**: Improved parsing logic to preserve formatting and readability in job descriptions.
- **Dockerized**: Ready for deployment on any server using Docker and Docker Compose.

## Prerequisites

- Python 3.9+ (if running locally)
- Docker & Docker Compose (for containerized execution)

## Setup & Usage

### Local Execution

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the Scraper**:
   ```bash
   python fetch_jobs.py
   ```

### Docker Execution (Recommended)

Running with Docker ensures all dependencies are correctly managed and allows for easy scheduling via cron on a server.

1. **Build the Container**:
   ```bash
   docker-compose build
   ```

2. **Run the Scraper**:
   ```bash
   docker-compose up
   ```
   *Note: The `jobs.db` file is volume-mounted, so your data persists on the host machine even after the container stops.*

## Database Schema

The `jobs.db` database contains a `jobs` table with the following structure:

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INTEGER | Primary key |
| `company` | TEXT | Name of the hiring company |
| `role` | TEXT | Job title / Role |
| `deadline` | TEXT | Application deadline |
| `link` | TEXT | UNIQUE URL to the job posting |
| `tech_park` | TEXT | Source (e.g., Infopark, Technopark) |
| `description`| TEXT | Full job description |
| `company_profile` | TEXT | Address and contact details |
| `email` | TEXT | Extracted contact email |

## Configuration

You can override the default target URLs by creating a `.env` file in the root directory:

```env
INFOPARK_URL=https://infopark.in/companies/job-search
TECHNOPARK_URL=https://technopark.org/api/paginated-jobs
```

## Scheduling

To run this as a daily cron job on a Linux server:

```bash
0 0 * * * cd /path/to/project && /usr/local/bin/docker-compose up > /dev/null 2>&1
```