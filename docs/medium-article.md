# I Built an End-to-End Analytics Pipeline on 9.4 Million NYC Taxi Trips — Here's the Whole Stack

*dbt for the transformations, DuckDB for a free local warehouse, Airflow for orchestration, GitHub Actions for CI, and a Streamlit dashboard anyone can open in a browser. Here's how it fits together — and the data-quality bug I only found by running it on the full dataset.*

---

## Why I built this

Everyone learning data engineering eventually meets the NYC taxi dataset. It's the "hello world" of the field. The problem is that most projects stop at *"I loaded it into a warehouse and wrote a `GROUP BY`."*

I wanted to go further and build the thing an actual analytics team would ship: a **layered, tested, version-controlled transformation pipeline** with a **consumption layer** on top — and make it run on my laptop for free while staying portable to a real cloud warehouse.

This is a walkthrough of what I built, the decisions behind it, and what broke.

## The 30-second overview

Raw NYC yellow-taxi trip records (public Parquet from the NYC Taxi & Limousine Commission) flow through four dbt layers into analytics-ready tables, which a Streamlit dashboard reads:

```
raw Parquet  →  staging  →  intermediate  →  marts  →  Streamlit dashboard
 (~9.4M trips)   (views)      (views)        (tables)   (KPIs + charts)
```

The stack:

| Layer | Tool | Why |
|---|---|---|
| Transformation | **dbt** | Version-controlled SQL, tests, lineage, incremental models |
| Warehouse (dev) | **DuckDB** | Free, embedded, reads Parquet directly — zero setup |
| Warehouse (prod) | **BigQuery** | Same models, cloud scale, env-var driven |
| Orchestration | **Airflow** | Scheduled extract → load → build → verify |
| CI | **GitHub Actions** | `dbt build` on every push |
| BI / serving | **Streamlit** | Interactive dashboard, deployable to the cloud |

## The core: layered dbt modelling

The heart of the project is dbt, structured the way analytics engineers actually structure it — each layer with a single job.

**Staging** (`stg_trips`, `stg_zones`) — thin cleanup, materialized as cheap views. This is where I generate a **surrogate key** for each trip (the TLC data has no primary key) by hashing vendor + timestamps + locations + amount, cast types consistently, and apply a **data-quality gate** that drops obviously invalid rows: null timestamps, dropoffs before pickups, negative distances.

**Intermediate** (`int_trips_enriched`) — joins trips to the zone lookup *twice* (pickup and dropoff), so every trip gains a human-readable borough and zone instead of a bare numeric ID. It also derives metrics like trip duration (via a reusable Jinja macro), pickup hour, and tip percentage.

**Marts** — the finished products, materialized as tables:

- **`fct_trips`** — the fact table, one row per trip.
- **`dim_zone`** — the dimension (borough/zone).
- **`agg_daily_revenue`** — a business summary: revenue, trips, and averages per day per borough.

That's a classic **star schema** — fact plus dimension — the pattern that has underpinned analytics warehouses for decades.

## The part that impressed me most: incremental models

`fct_trips` is an **incremental model**. The first run full-loads it. On every run after that, dbt only processes trips *newer than the latest pickup already loaded*:

```sql
{% if is_incremental() %}
where pickup_at > (select coalesce(max(pickup_at), '1900-01-01') from {{ this }})
{% endif %}
```

Download another month of data, re-run, and instead of reprocessing 9 million rows it touches only the new ones. This is the single feature that separates "I used dbt" from "I understand dbt."

## Testing that actually caught bugs

I wired up dbt tests — the generic ones (`unique`, `not_null`, `relationships`, `accepted_range`) plus a **custom reconciliation test** asserting that `total_amount` is at least the sum of its components (fare + tip + tolls, within a cent of tolerance).

And here's the best part of the whole project.

When I first built against a single month of data, everything passed. When I ran the **full three months (9.4M rows)**, two tests **failed**:

1. **`assert_total_amount_reconciles`** — 3 trips where the total was *less* than fare + tip + tolls. Corrupt records.
2. **`unique_fct_trips_trip_id`** — a duplicate surrogate key. The TLC data literally ships duplicate trip rows.

This is exactly why you write tests. The failures weren't a problem with my code — they were the tests **doing their job**, catching real dirty data that a `GROUP BY` demo would have silently averaged into wrong numbers.

I fixed it at the boundary, in staging: a reconciliation gate that drops the corrupt rows, and a dedup that keeps one row per surrogate key. A full `dbt build` now passes all 22 nodes clean — and I bumped CI to build all three months so it exercises that path on every push.

## One repo, two warehouses

The dbt source is **target-aware**. On my laptop it resolves to reading local Parquet with DuckDB; in the cloud it points at a BigQuery table:

```sql
{%- if target.type == 'duckdb' -%}
  read_parquet('data/yellow_tripdata_*.parquet', union_by_name=true)
{%- else -%}
  {{ target.project }}.{{ target.dataset }}.yellow_tripdata
{%- endif -%}
```

Same models, same tests, two backends — DuckDB for free iteration, BigQuery for scale. The production config is entirely env-var driven, so no secrets ever touch the repo.

## The consumption layer: a dashboard anyone can open

Marts are only useful if someone can *see* them, so I built a **Streamlit dashboard**: KPI tiles (total revenue, trips, passengers, average tip %), revenue by borough, trips by hour of day, and a daily revenue trend — with date-range and borough filters, and a colorblind-safe palette where each borough keeps a fixed hue.

The interesting engineering problem: the dashboard reads a 160 MB local DuckDB file with 9.4M rows — you can't deploy *that* to a free host. So I gave the app **two backends behind one code path**. Locally it attaches the full warehouse read-only. In the cloud, where there's no warehouse, it reads tiny **pre-aggregated Parquet files** (~85 KB total) committed to the repo. The app deploys to Streamlit Community Cloud with no database and no raw data — and the code that draws the charts doesn't know or care which backend it's on.

**👉 You can play with the live dashboard here: [nyctaxidbt.streamlit.app](https://nyctaxidbt-as6niaviukjr4sinhjdpyz.streamlit.app/)**

## What the data says

A few things jumped out once the dashboard was live:

- **$258M** in total fares across ~9.4M trips in Q1 2024.
- **Manhattan dominates** — roughly $196M of that revenue, dwarfing every other borough.
- Trips peak at **6 PM**, the evening rush.
- Average tip is **~20.5%** — New Yorkers tip well.

## What I'd tell someone starting this

- **Structure your transformations in layers.** Staging → intermediate → marts isn't bureaucracy; it's what makes the project legible and testable.
- **Write tests, then run on the full dataset.** Small samples lie. My tests only earned their keep at 9.4M rows.
- **Separate compute from your laptop's limits.** DuckDB reading Parquet is astonishingly capable for local work, and designing for a second warehouse from day one kept the door open to scale.
- **Ship a consumption layer.** Clean tables are invisible; a dashboard is what makes people care.

## The stack, one more time

dbt · DuckDB · BigQuery · Airflow · GitHub Actions · Streamlit · Python.

The full project — models, tests, macros, the Airflow DAG, the loader, and the dashboard — is on GitHub:

**👉 [github.com/jumma786/nyc_taxi_dbt](https://github.com/jumma786/nyc_taxi_dbt)**

If you're learning analytics engineering, clone it, run `dbt build`, and open the dashboard. It runs free on your laptop in a couple of minutes.

*Thanks for reading. I'm Jumma Mohammad Teli — I write about data engineering and analytics. Connect with me on [LinkedIn](https://linkedin.com/in/jumma-mohammad).*
