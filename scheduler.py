"""
scheduler.py — Master Pipeline Orchestrator
=============================================
This is the MAIN file you run to start the entire project.

What it does:
  1. Creates the database (first time only)
  2. Immediately runs the full pipeline (scrape → clean)
  3. Keeps running, repeating the pipeline every 7 days

How to run:
  python scheduler.py

Keep this running in one terminal window.
In another terminal, run: streamlit run dashboard.py
"""

import schedule
import time
import sys
from datetime import datetime

# ── Import the other modules ───────────────────────────────
from database import setup_database

try:
    from merojob_scraper import scrape_jobs as scrape_mero
except ImportError as e:
    print(f"❌ Could not import merojob_scraper: {e}")
    sys.exit(1)

try:
    from scrape_kumari import scrape_kumari_jobs as scrape_kumari
except ImportError as e:
    print(f"❌ Could not import scrape_kumari: {e}")
    sys.exit(1)

try:
    from clean_data import clean_and_merge
except ImportError as e:
    print(f"❌ Could not import clean_data: {e}")
    sys.exit(1)


# ══════════════════════════════════════════════════════════════
#  FULL PIPELINE FUNCTION
# ══════════════════════════════════════════════════════════════

def full_pipeline():
    """
    Runs all three stages in order:
      1. Scrape MeroJob
      2. Scrape KumariJob
      3. Clean and merge data into jobs_clean table
    """
    start = datetime.now()
    print("\n" + "═" * 55)
    print(f"  🚀 PIPELINE STARTED — {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 55)

    # ── Stage 1: Scrape MeroJob ────────────────────────────
    print("\n▶ [1/3] Scraping MeroJob...")
    try:
        mero_count = scrape_mero()
    except Exception as e:
        print(f"  ❌ MeroJob scraping failed: {e}")
        mero_count = 0

    # ── Stage 2: Scrape KumariJob ──────────────────────────
    print("\n▶ [2/3] Scraping KumariJob...")
    try:
        kumari_count = scrape_kumari()
    except Exception as e:
        print(f"  ❌ KumariJob scraping failed: {e}")
        kumari_count = 0

    # ── Stage 3: Clean and merge ───────────────────────────
    print("\n▶ [3/3] Cleaning and processing data...")
    try:
        df = clean_and_merge()
        clean_count = len(df) if df is not None else 0
    except Exception as e:
        print(f"  ❌ Cleaning failed: {e}")
        clean_count = 0

    # ── Summary ────────────────────────────────────────────
    end = datetime.now()
    duration = (end - start).seconds
    print("\n" + "═" * 55)
    print(f"  ✅ PIPELINE COMPLETE — {end.strftime('%H:%M:%S')}")
    print(f"     Duration:      {duration} seconds")
    print(f"     MeroJob jobs:  {mero_count}")
    print(f"     KumariJob jobs:{kumari_count}")
    print(f"     Clean total:   {clean_count}")
    print(f"     Dashboard:     Refresh your browser to see updated data")
    print("═" * 55 + "\n")


# ══════════════════════════════════════════════════════════════
#  STARTUP
# ══════════════════════════════════════════════════════════════

print("=" * 55)
print("  Nepal Job Market Dashboard — Pipeline Scheduler")
print("=" * 55)

# Step 1: Make sure database exists
print("\n🗄  Setting up database...")
setup_database()

# Step 2: Run once immediately on startup
print("\n📡 Running initial data collection (this takes a few minutes)...\n")
full_pipeline()

# Step 3: Schedule to run every 7 days
schedule.every(7).days.do(full_pipeline)

print("📅 Scheduler is active. Next run: in 7 days.")
print("\n" + "─" * 55)
print("  ✅ Now open a NEW terminal and run:")
print("     streamlit run dashboard.py")
print("─" * 55)
print("\n  Keeping scheduler running... (Ctrl+C to stop)\n")

# Keep the scheduler alive (checks every minute)
while True:
    try:
        schedule.run_pending()
        time.sleep(60)
    except KeyboardInterrupt:
        print("\n\n⛔ Scheduler stopped by user. Goodbye!")
        sys.exit(0)
