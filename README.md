# NYC Taxi Analytics — dbt + DuckDB

An end-to-end analytics engineering project transforming raw NYC TLC yellow taxi
trip data into analytics-ready marts with dbt, running locally on DuckDB.

**Author:** Jumma Mohammad Teli · [GitHub](https://github.com/jumma786) · [LinkedIn](https://linkedin.com/in/jumma-mohammad)

## What it demonstrates

- Layered dbt architecture: **staging → intermediate → marts**
- **Incremental model** (`fct_trips`) using monthly Parquet partitions
- Dimensional modelling (`dim_zone`, `fct_trips`, `agg_daily_revenue`)
- **Testing**: generic tests (`unique`, `not_null`, `relationships`,
  `accepted_range`) plus a custom singular reconciliation test
- Reusable **Jinja macro** (`trip_duration_minutes`)
- Source-to-mart **lineage** via `dbt docs`
- **Orchestration** with Airflow: scheduled extract → load → dbt build → verify
- **CI** with GitHub Actions running `dbt build` on every push
- **BI layer**: an interactive Streamlit dashboard served from the DuckDB marts

## Architecture

```
sources (Parquet)  ->  staging (views)  ->  intermediate (views)  ->  marts (tables)
  yellow_tripdata       stg_trips            int_trips_enriched        dim_zone
  taxi_zone_lookup      stg_zones                                      fct_trips (incremental)
                                                                       agg_daily_revenue
```

## Quickstart

```bash
# 1. Environment
python -m pip install dbt-duckdb

# 2. Get data (3 months of 2024 by default)
python scripts/download_data.py --year 2024 --months 1 2 3

# 3. Install dbt packages
dbt deps --profiles-dir .

# 4. Build everything (seeds, models, tests)
dbt build --profiles-dir .

# 5. Docs + lineage graph
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

## Incremental runs

The first `dbt build` full-loads `fct_trips`. Download another month and
re-run — only newer trips are processed:

```bash
python scripts/download_data.py --year 2024 --months 4
dbt run --select fct_trips+ --profiles-dir .
```

## Dashboard (BI layer)

An interactive **Streamlit** dashboard reads the dbt marts directly from the
DuckDB file — the consumption layer on top of `agg_daily_revenue` and
`fct_trips`:

- KPI tiles (total revenue, trips, passengers, avg tip %)
- Revenue by borough, trips by hour of day, and a daily revenue trend
- Sidebar filters for pickup date range and borough
- Colorblind-safe categorical palette; boroughs colored by a fixed hue order

```bash
pip install -r dashboard/requirements.txt
# build the marts first (see Quickstart), then:
streamlit run dashboard/app.py        # opens at http://localhost:8501
```

The DuckDB path defaults to `nyc_taxi.duckdb` at the project root; override with
the `NYC_TAXI_DUCKDB` environment variable.

## Orchestration (Airflow)

The `airflow/` directory turns this into a scheduled ELT pipeline. The
`nyc_taxi_elt` DAG runs monthly and executes four stages:

```
extract  ->  load_check  ->  dbt_build  ->  freshness_check
(download    (verify file    (dbt build:   (assert mart has
 Parquet to   landed & non-   models +      rows for the
 raw zone)    empty)          tests)        loaded month)
```

- **Idempotent**: re-running a month skips an existing download; the
  incremental `fct_trips` only processes new pickups.
- **Retries** on transient failures; `max_active_runs=1` to avoid clobbering
  the DuckDB file.
- Ingestion is a distinct stage from transformation (extract/load vs dbt's T).

Run it locally:

```bash
cd airflow
docker compose up          # UI at http://localhost:8080
# trigger the nyc_taxi_elt DAG from the UI, or:
# airflow dags trigger nyc_taxi_elt
```

## Analytics engineering vs data engineering framing

The same repo supports two portfolio stories:

- **Analytics engineering** — lead with the dbt layering, testing, lineage,
  and marts. (`dbt build` + `dbt docs`.)
- **Data engineering** — lead with the Airflow-orchestrated ELT: scheduled
  ingestion into a raw landing zone, in-warehouse transformation, data-quality
  gates, and idempotent incremental loads.

To push the DE story further: swap DuckDB for BigQuery/Snowflake (a landing
bucket in GCS/S3 + a warehouse target in `profiles.yml`), and split extract/load
into a dedicated loader (e.g. `dlt`).

## Two targets: dev (DuckDB) and prod (BigQuery)

- **dev** — DuckDB, local, free. Reads Parquet directly. Use for all iteration.
- **prod** — BigQuery, cloud. Raw data is landed in a **GCS bucket** and loaded
  into BigQuery by a **dlt** loader (`loader/load_to_bq.py`); dbt then transforms
  in-warehouse. Fully driven by environment variables — no secrets in the repo.

The `raw.yellow_tripdata` source is target-aware: it resolves to a local
`read_parquet(...)` on dev and to `<project>.<dataset>.yellow_tripdata` on prod
(see `models/staging/_sources.yml`).

See **`loader/CLOUD_RUNBOOK.md`** for the full GCS + BigQuery setup and commands.

## Data source

NYC Taxi & Limousine Commission — Trip Record Data (public).
Files: `yellow_tripdata_YYYY-MM.parquet` and `taxi_zone_lookup.csv`.

## Project layout

```
models/
  staging/       stg_trips, stg_zones, _sources.yml, _staging.yml
  intermediate/  int_trips_enriched
  marts/         dim_zone, fct_trips, agg_daily_revenue, _marts.yml
macros/          trip_duration_minutes
tests/           assert_total_amount_reconciles (custom)
seeds/           taxi_zone_lookup.csv
scripts/         download_data.py
airflow/
  dags/          nyc_taxi_elt_dag.py
  docker-compose.yml, requirements.txt
loader/
  load_to_bq.py  (dlt: Parquet -> GCS -> BigQuery)
  CLOUD_RUNBOOK.md, requirements.txt
dashboard/
  app.py         (Streamlit BI layer over the DuckDB marts)
  requirements.txt
```
