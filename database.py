import sqlite3
import pandas as pd
from datetime import datetime

DB_PATH = "jobs.db"

def get_connection():
    """Returns a database connection"""
    return sqlite3.connect(DB_PATH)

def setup_database():
    """Creates tables if they don't exist yet. Safe to call multiple times."""
    conn = get_connection()
    c = conn.cursor()

    # Raw MeroJob data table
    c.execute("""
        CREATE TABLE IF NOT EXISTS merojob_raw (
            id          TEXT,
            title       TEXT,
            company     TEXT,
            location    TEXT,
            categories  TEXT,
            deadline    TEXT,
            job_level   TEXT,
            vacancies   TEXT,
            salary_min  TEXT,
            salary_max  TEXT,
            currency    TEXT,
            skills      TEXT,
            job_url     TEXT,
            scraped_at  TEXT,
            PRIMARY KEY (id, scraped_at)
        )
    """)

    # Raw KumariJob data table
    c.execute("""
        CREATE TABLE IF NOT EXISTS kumari_raw (
            job_id      TEXT,
            job_title   TEXT,
            company     TEXT,
            link        TEXT,
            salary      TEXT,
            experience  TEXT,
            industry    TEXT,
            job_level   TEXT,
            education   TEXT,
            scraped_at  TEXT,
            PRIMARY KEY (job_id, scraped_at)
        )
    """)

    conn.commit()
    conn.close()
    print("✅ Database ready at", DB_PATH)


def save_merojob_data(jobs_list):
    """
    Saves a list of MeroJob job dicts to the database.
    Skips duplicates automatically (same id + scraped_at).
    """
    conn = get_connection()
    scraped_at = datetime.now().isoformat()
    saved = 0
    for job in jobs_list:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO merojob_raw
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                str(job.get('id', '')),
                job.get('title', 'N/A'),
                job.get('company', 'N/A'),
                job.get('location', 'N/A'),
                job.get('categories', 'N/A'),
                job.get('deadline', 'N/A'),
                job.get('job_level', 'N/A'),
                str(job.get('vacancies', 'N/A')),
                str(job.get('salary_min', 'N/A')),
                str(job.get('salary_max', 'N/A')),
                job.get('currency', 'N/A'),
                job.get('skills', ''),
                job.get('job_url', ''),
                scraped_at
            ))
            saved += 1
        except Exception as e:
            print(f"  Skip MeroJob id={job.get('id')}: {e}")
    conn.commit()
    conn.close()
    print(f"✅ Saved {saved} MeroJob listings to DB")


def save_kumari_data(jobs_dict):
    """
    Saves a dict of KumariJob jobs (keyed by job_id) to the database.
    Skips duplicates automatically.
    """
    conn = get_connection()
    scraped_at = datetime.now().isoformat()
    saved = 0
    for job_id, job in jobs_dict.items():
        try:
            conn.execute("""
                INSERT OR IGNORE INTO kumari_raw
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                str(job_id),
                job.get('Job Title', 'N/A'),
                job.get('Company', 'N/A'),
                job.get('Link', 'N/A'),
                job.get('Salary', 'N/A'),
                job.get('Experience', 'N/A'),
                job.get('Industry', 'N/A'),
                job.get('Job Level', 'N/A'),
                job.get('Education', 'N/A'),
                scraped_at
            ))
            saved += 1
        except Exception as e:
            print(f"  Skip KumariJob id={job_id}: {e}")
    conn.commit()
    conn.close()
    print(f"✅ Saved {saved} KumariJob listings to DB")


def load_clean_jobs():
    """
    Loads the cleaned, merged jobs table as a pandas DataFrame.
    Returns an empty DataFrame if the table doesn't exist yet.
    """
    conn = get_connection()
    try:
        df = pd.read_sql("SELECT * FROM jobs_clean", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


if __name__ == "__main__":
    setup_database()
    print("Database initialized successfully.")
