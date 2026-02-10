# Nepal Job Market Dashboard

Real-time job market analytics dashboard built with Python and Streamlit.
Scrapes live data from Nepali job portals, processes it, and visualizes trends.

## Setup

```bash
pip install -r requirements.txt
```

## Run

```bash
python scheduler.py
streamlit run dashboard.py
```

## Stack

Python · Streamlit · Plotly · SQLite · BeautifulSoup

---
<p>
      <span class="accent"> Dipa Khanal:</span> scheduler.py, database.py, clean_data.py — the pipeline<br><br>
      <span class="accent">Niraj Nath:</span> merojob_scraper.py — modify to save to DB<br><br>
      <span class="accent">Shailaj Dahal:</span> scrape_kumari.py — modify to save to DB<br><br>
      <span class="accent">Swarnim Shrestha:</span> eda_analysis.py + dashboard.py — charts and frontend
    </p>
