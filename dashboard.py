"""
dashboard.py — Live Streamlit Dashboard
=========================================
Reads from the jobs_clean table in jobs.db and displays:
  - KPI metrics (total jobs, companies, median salary, categories)
  - Sidebar filters (source, category, job level)
  - 5 interactive charts
  - Searchable job listings table
  - Download button for filtered data

Run with:
  streamlit run dashboard.py

Opens at: http://localhost:8501
Auto-refreshes every 5 minutes (cache TTL).
"""

import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from collections import Counter
from datetime import datetime

# ══════════════════════════════════════════════════════════════
#  PAGE CONFIGURATION  (must be first Streamlit call)
# ══════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Nepal Job Market Dashboard",
    page_icon="🇳🇵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ══════════════════════════════════════════════════════════════
#  LOAD DATA  (cached for 5 minutes so dashboard stays fast)
# ══════════════════════════════════════════════════════════════

@st.cache_data(ttl=300)
def load_data():
    """Loads the clean jobs table from the database."""
    try:
        conn = sqlite3.connect("jobs.db")
        df = pd.read_sql("SELECT * FROM jobs_clean", conn)
        conn.close()
        df['scraped_at'] = pd.to_datetime(df['scraped_at'], errors='coerce')
        df['salary_min'] = pd.to_numeric(df['salary_min'], errors='coerce')
        df['salary_max'] = pd.to_numeric(df['salary_max'], errors='coerce')
        return df
    except Exception as e:
        return pd.DataFrame()

df = load_data()

# ══════════════════════════════════════════════════════════════
#  HEADER
# ══════════════════════════════════════════════════════════════

st.title("🇳🇵 Nepal Job Market Analytics Dashboard")
st.caption(
    f"📅 Dashboard loaded at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  |  "
    f"Data sources: **MeroJob** + **KumariJob**  |  "
    f"Auto-refreshes every 5 minutes"
)

if df.empty:
    st.error("⚠️ No data found in database.")
    st.info(
        "**To get started:**\n"
        "1. Open a terminal in this folder\n"
        "2. Run: `python scheduler.py`\n"
        "3. Wait for scraping to complete\n"
        "4. Come back and refresh this page"
    )
    st.stop()

# ══════════════════════════════════════════════════════════════
#  SIDEBAR FILTERS
# ══════════════════════════════════════════════════════════════

st.sidebar.header("🔍 Filter Jobs")
st.sidebar.markdown("---")

# Source filter
all_sources = sorted(df['source'].dropna().unique().tolist())
source_filter = st.sidebar.multiselect(
    "📡 Data Source",
    options=all_sources,
    default=all_sources,
    help="Choose which website(s) to include"
)

# Category filter (show top 30 most common)
top_cats = df['category'].value_counts().head(30).index.tolist()
category_filter = st.sidebar.multiselect(
    "💼 Job Category",
    options=top_cats,
    default=[],
    help="Leave empty to show all categories"
)

# Job Level filter
all_levels = sorted(df['job_level'].dropna().unique().tolist())
level_filter = st.sidebar.multiselect(
    "📊 Job Level",
    options=all_levels,
    default=[],
    help="Leave empty to show all levels"
)

# Location filter
top_locs = df[df['location'] != 'Unknown']['location'].value_counts().head(15).index.tolist()
loc_filter = st.sidebar.multiselect(
    "📍 Location",
    options=top_locs,
    default=[],
    help="Leave empty to show all cities"
)

st.sidebar.markdown("---")
st.sidebar.markdown("**About this project:**")
st.sidebar.markdown(
    "Real-time job market analytics for Nepal, "
    "built by scraping MeroJob and KumariJob. "
    "Data updates every 7 days automatically."
)

# ══════════════════════════════════════════════════════════════
#  APPLY FILTERS
# ══════════════════════════════════════════════════════════════

filtered = df.copy()

if source_filter:
    filtered = filtered[filtered['source'].isin(source_filter)]
if category_filter:
    filtered = filtered[filtered['category'].isin(category_filter)]
if level_filter:
    filtered = filtered[filtered['job_level'].isin(level_filter)]
if loc_filter:
    filtered = filtered[filtered['location'].isin(loc_filter)]

# ══════════════════════════════════════════════════════════════
#  KPI METRICS  (top row of numbers)
# ══════════════════════════════════════════════════════════════

st.markdown("### 📊 Key Metrics")
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        "📋 Total Jobs",
        f"{len(filtered):,}",
        delta=f"{len(filtered) - len(df):+,}" if len(filtered) != len(df) else None
    )
with col2:
    st.metric("🏢 Companies Hiring", f"{filtered['company'].nunique():,}")
with col3:
    median_sal = filtered['salary_min'].median()
    st.metric(
        "💰 Median Salary",
        f"NPR {median_sal:,.0f}" if pd.notna(median_sal) else "N/A"
    )
with col4:
    st.metric("🗂 Categories", filtered['category'].nunique())
with col5:
    st.metric("🌆 Cities", filtered[filtered['location'] != 'Unknown']['location'].nunique())

st.markdown("---")

# ══════════════════════════════════════════════════════════════
#  ROW 1: Categories + Location
# ══════════════════════════════════════════════════════════════

st.markdown("### 📈 Job Distribution")
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    top_cats_chart = filtered['category'].value_counts().head(12)
    if len(top_cats_chart) > 0:
        fig = px.bar(
            x=top_cats_chart.values,
            y=top_cats_chart.index,
            orientation='h',
            title="Top Job Categories",
            color=top_cats_chart.values,
            color_continuous_scale='Viridis',
            labels={'x': 'Number of Jobs', 'y': 'Category'}
        )
        fig.update_layout(showlegend=False, height=420, margin=dict(l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)

with chart_col2:
    loc_data = filtered[filtered['location'] != 'Unknown']['location'].value_counts().head(10)
    if len(loc_data) > 0:
        fig = px.pie(
            values=loc_data.values,
            names=loc_data.index,
            title="Jobs by City",
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Safe
        )
        fig.update_layout(height=420, margin=dict(l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════
#  ROW 2: Job Level + Salary
# ══════════════════════════════════════════════════════════════

chart_col3, chart_col4 = st.columns(2)

with chart_col3:
    level_data = filtered['job_level'].value_counts()
    if len(level_data) > 0:
        fig = px.bar(
            x=level_data.index,
            y=level_data.values,
            title="Jobs by Experience Level",
            color=level_data.index,
            color_discrete_sequence=px.colors.qualitative.Pastel,
            labels={'x': 'Level', 'y': 'Number of Jobs'},
            text=level_data.values
        )
        fig.update_traces(showlegend=False, textposition='outside')
        fig.update_layout(height=400, margin=dict(l=0, r=0), plot_bgcolor='white')
        st.plotly_chart(fig, use_container_width=True)

with chart_col4:
    salary_data = filtered[
        filtered['salary_min'].notna() &
        (filtered['salary_min'] > 0) &
        (filtered['salary_min'] < 500_000)
    ]
    if len(salary_data) > 5:
        fig = px.histogram(
            salary_data,
            x='salary_min',
            nbins=25,
            title="Salary Distribution (NPR/month)",
            color_discrete_sequence=['#f0a500'],
            labels={'salary_min': 'Minimum Salary (NPR)'}
        )
        median = salary_data['salary_min'].median()
        fig.add_vline(
            x=median,
            line_dash="dash",
            line_color="#e74c3c",
            annotation_text=f"Median: NPR {median:,.0f}"
        )
        fig.update_layout(height=400, plot_bgcolor='white', margin=dict(l=0, r=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Not enough salary data to display distribution chart.")

# ══════════════════════════════════════════════════════════════
#  SKILLS CHART (full width)
# ══════════════════════════════════════════════════════════════

st.markdown("### 🛠 In-Demand Skills")
all_skills = []
for s in filtered['skills'].dropna():
    if s and s.strip():
        for skill in s.split(','):
            skill = skill.strip()
            if skill and skill.lower() not in ('n/a', 'na', ''):
                all_skills.append(skill)

if len(all_skills) > 5:
    skill_series = pd.Series(Counter(all_skills)).sort_values(ascending=False).head(20)
    fig = px.bar(
        x=skill_series.index,
        y=skill_series.values,
        title="Top 20 Skills Required by Employers",
        color=skill_series.values,
        color_continuous_scale='Plasma',
        labels={'x': 'Skill', 'y': 'Number of Jobs'},
        text=skill_series.values
    )
    fig.update_traces(textposition='outside')
    fig.update_layout(
        height=420,
        showlegend=False,
        xaxis_tickangle=-35,
        plot_bgcolor='white',
        margin=dict(l=0, r=0)
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info(
        "Skills data comes from MeroJob only. "
        "Make sure MeroJob scraper has run and returned results with skills."
    )

# ══════════════════════════════════════════════════════════════
#  JOB LISTINGS TABLE  (searchable)
# ══════════════════════════════════════════════════════════════

st.markdown("---")
st.markdown("### 📋 Browse All Job Listings")

search_term = st.text_input(
    "🔎 Search jobs (by title or company name)",
    placeholder="e.g. Software Engineer, Bajaj, IT Manager..."
)

display_df = filtered.copy()
if search_term:
    mask = (
        display_df['title'].str.contains(search_term, case=False, na=False) |
        display_df['company'].str.contains(search_term, case=False, na=False)
    )
    display_df = display_df[mask]
    st.caption(f"Found **{len(display_df)}** results for '{search_term}'")

# Select columns to show in the table
show_cols = ['title', 'company', 'location', 'category', 'job_level',
             'salary_min', 'salary_max', 'experience', 'source', 'job_url']
show_cols = [c for c in show_cols if c in display_df.columns]  # safety check

st.dataframe(
    display_df[show_cols].rename(columns={
        'title': 'Job Title',
        'company': 'Company',
        'location': 'Location',
        'category': 'Category',
        'job_level': 'Level',
        'salary_min': 'Min Salary (NPR)',
        'salary_max': 'Max Salary (NPR)',
        'experience': 'Experience',
        'source': 'Source',
        'job_url': 'Link'
    }),
    use_container_width=True,
    height=500
)

# ── Download Button ────────────────────────────────────────
csv_data = display_df[show_cols].to_csv(index=False, encoding='utf-8-sig')
st.download_button(
    label=f"⬇️ Download {len(display_df)} jobs as CSV",
    data=csv_data,
    file_name=f"nepal_jobs_{datetime.now().strftime('%Y%m%d')}.csv",
    mime="text/csv"
)

# ══════════════════════════════════════════════════════════════
#  FOOTER
# ══════════════════════════════════════════════════════════════

st.markdown("---")
st.caption(
    "Nepal Job Market Analytics Dashboard | "
    "Data scraped from MeroJob.com and KumariJob.com | "
    f"Showing {len(filtered):,} of {len(df):,} total jobs"
)
