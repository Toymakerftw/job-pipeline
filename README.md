```markdown
# techpark-jobs-pipeline

This Python script scrapes job postings from the Infopark and Technopark websites, extracts relevant details, and stores them in a SQLite database. This project is designed with potential DevOps practices in mind, aiming for automation and efficient data handling.

## Prerequisites

-   Python 3.7+
-   `aiohttp`
-   `beautifulsoup4`
-   `python-dotenv`
-   `sqlite3`

You can install the required packages using pip:

```bash
pip install aiohttp beautifulsoup4 python-dotenv
```

## Setup

1.  **Clone the repository:**

    ```bash
    git clone <repository_url>
    cd techpark-jobs-pipeline
    ```

2.  **Create a `.env` file:**

    Create a `.env` file in the root directory of the project and add the following environment variables:

    ```
    INFOPARK_URL=[https://infopark.in/companies/job-search](https://infopark.in/companies/job-search)
    TECHNOPARK_URL=[https://technopark.org/api/paginated-jobs](https://technopark.org/api/paginated-jobs)
    ```

    (If you are using the default urls, this step is optional.)

3.  **Run the script:**

    ```bash
    python scraper.py
    ```

    This will create a `jobs.db` SQLite database file and populate it with job postings.

## Database Schema

The `jobs.db` database contains a `jobs` table with the following schema:

| Column          | Type    | Description                               |
| --------------- | ------- | ----------------------------------------- |
| `id`            | INTEGER | Primary key, auto-incrementing            |
| `company`       | TEXT    | Company name                              |
| `role`          | TEXT    | Job role                                  |
| `deadline`      | TEXT    | Application deadline                      |
| `link`          | TEXT    | Link to the job posting                   |
| `tech_park`     | TEXT    | Technology park (Infopark or Technopark) |
| `description`   | TEXT    | Job description                           |
| `company_profile` | TEXT    | Company profile details                  |

## Logging

The script uses the `logging` module to log informational messages and errors. Logs are written to the console.

## Usage

After running the script, you can use any SQLite browser or Python's `sqlite3` module to query the `jobs.db` database.

Example query to get all jobs from Infopark:

```sql
SELECT * FROM jobs WHERE tech_park = 'Infopark';
```
