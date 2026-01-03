# Techpark Jobs Pipeline 🚀

A robust, asynchronous Python scraper that aggregates job listings from major IT parks in Kerala (Infopark, Technopark, UL Cyberpark, and Cyberpark Kozhikode) into a central MySQL database.

## Features

- **Multi-Source Aggregation**: Fetches data from Infopark, Technopark, UL Cyberpark, and Cyberpark RSS feeds.
- **Asynchronous Scraping**: Built with `aiohttp` and `asyncio` for high-performance data retrieval.
- **Data Persistence**: Stores job details, company profiles, and contact emails in a MySQL database.
- **Deduplication**: Uses unique job links to ensure no duplicate entries are stored.
- **Clean Text**: Improved parsing logic to preserve formatting and readability in job descriptions.
- **Dockerized**: Ready for deployment on any server using Docker and Docker Compose.

## Prerequisites

- Python 3.9+ (if running locally)
- Docker & Docker Compose (Recommended)

## Setup & Usage

### Docker Execution (Recommended)

This project uses a full stack containerization. `docker-compose` will spin up two containers:
1. `job-scraper`: The Python script.
2. `db`: A MySQL 8.0 database.

1. **Build the Container**:
   ```bash
   docker-compose build
   ```

2. **Run the Stack**:
   ```bash
   docker-compose up -d
   ```
   *Note: This binds port 3306 on your host machine. If you have a local MySQL server running, stop it first to avoid conflicts (`sudo systemctl stop mysql`).*

### Local Execution (Manual)

If you prefer running the script manually but want to use the Docker database:

1. Start the DB: `docker-compose up -d db`
2. Install Python deps: `pip install -r requirements.txt`
3. Run script: `python fetch_jobs.py` (It will connect to `localhost:3306` which is mapped to the container).

## Connecting Vercel (or External Apps)

The MySQL database is exposed on **port 3306** of your host machine. To connect your Vercel application:

1. **Firewall**: Ensure your server allows incoming traffic on port 3306 (e.g., `sudo ufw allow 3306` or update AWS Security Groups).
2. **Connection Details**:
   - **Host**: Your Server's Public IP Address
   - **Port**: `3306`
   - **User**: `kljobs_user`
   - **Password**: `PX#lGJi5D68lH@`
   - **Database**: `kljobs_db`

*Security Note: For production, restrict firewall access to specific IPs if possible.*

## Database Schema

The `jobs` table in the MySQL database has the following structure:

| Column | Type | Description |
| :--- | :--- | :--- |
| `id` | INT | Primary key, Auto Increment |
| `company` | TEXT | Name of the hiring company |
| `role` | TEXT | Job title / Role |
| `deadline` | TEXT | Application deadline |
| `link` | VARCHAR(255) | UNIQUE URL to the job posting |
| `tech_park` | TEXT | Source (e.g., Infopark, Technopark) |
| `description`| MEDIUMTEXT | Full job description |
| `company_profile` | TEXT | Address and contact details |
| `email` | TEXT | Extracted contact email |

## Configuration

You can configure the scraper via environment variables in a `.env` file or pass them directly:

```env
# Database Configuration
DB_HOST=localhost
DB_USER=kljobs_user
DB_PASSWORD=PX#lGJi5D68lH@
DB_NAME=kljobs_db

# Gemini API Configuration (For Data Cleaning)
GEMINI_API_KEY=your_gemini_api_key_here

# Target URLs (Optional)
INFOPARK_URL=https://infopark.in/companies/job-search
TECHNOPARK_URL=https://technopark.org/api/paginated-jobs
```

## Scheduling

To run this as a daily cron job on a Linux server:

```bash
0 0 * * * cd /path/to/project && /usr/local/bin/docker-compose up > /dev/null 2>&1
```