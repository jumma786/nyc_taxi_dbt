# LinkedIn post

## Main version

🚕 I just shipped an end-to-end analytics engineering project on 9.4 MILLION NYC taxi trips — and it's live in your browser right now.

Most "taxi dataset" projects stop at loading the data and writing a GROUP BY. I wanted to build the thing an actual analytics team would ship: version-controlled, tested, orchestrated, and with a dashboard people can actually use.

Here's the stack 👇

🧱 dbt — layered models (staging → intermediate → marts), a star schema, and an incremental fact table that only processes NEW trips on each run
🦆 DuckDB — a free, embedded warehouse for local development (reads Parquet directly, zero setup)
☁️ BigQuery — the same models run in the cloud, driven entirely by env vars (no secrets in the repo)
🗓️ Airflow — an idempotent ELT pipeline: extract → load → build → verify
🔁 GitHub Actions — a full dbt build + tests on every push
📊 Streamlit — an interactive dashboard, deployed free to the cloud with NO database attached

But my favourite part? The tests found a real bug. 🐛

On one month of data, everything passed. On the full 9.4M rows, two dbt tests FAILED:
→ 3 corrupt trips where the total was less than fare + tip + tolls
→ a duplicate primary key (the raw data literally ships duplicate rows)

That's the entire point of testing your data. I fixed both at the staging layer, and CI now builds all 3 months so it catches this on every push.

A few things the data revealed:
💵 $258M in fares across Q1 2024
🏙️ Manhattan = ~76% of all revenue
⏰ Trips peak at 6 PM
💳 New Yorkers tip ~20.5% on average

▶️ Live dashboard: https://nyctaxidbt-as6niaviukjr4sinhjdpyz.streamlit.app/
💻 Full code + README: https://github.com/jumma786/nyc_taxi_dbt

If you're learning analytics/data engineering, clone it and run `dbt build` — it works free on your laptop in minutes.

What would you build on top of it next? 👇

#dataengineering #analyticsengineering #dbt #dataanalytics #python #sql #duckdb #bigquery #streamlit #datascience #ETL #dataviz


---

## Short version (if you prefer punchy)

I built an end-to-end analytics pipeline on 9.4M NYC taxi trips — and it's live. 🚕

dbt for transformations (layered models + a star schema + incremental loads)
DuckDB locally, BigQuery in the cloud — one codebase, zero secrets
Airflow orchestration + GitHub Actions CI
A Streamlit dashboard, deployed free with no database attached

Best part: my dbt tests caught real dirty data — corrupt totals and duplicate keys hiding in the raw TLC files. That's why you test your data. ✅

$258M in fares · Manhattan = 76% of revenue · trips peak at 6 PM.

▶️ Live: https://nyctaxidbt-as6niaviukjr4sinhjdpyz.streamlit.app/
💻 Code: https://github.com/jumma786/nyc_taxi_dbt

#dataengineering #analyticsengineering #dbt #python #duckdb #streamlit #dataanalytics
