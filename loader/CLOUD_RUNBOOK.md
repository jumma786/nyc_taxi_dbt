# Cloud pipeline runbook — GCS + BigQuery (prod)

The `dev` target (DuckDB) needs nothing but Python. This runbook covers the
`prod` target: land raw taxi data in a **GCS bucket**, load it into
**BigQuery** with dlt, then run dbt against BigQuery.

Nothing secret lives in this repo. All cloud config is supplied via environment
variables at run time, and the service-account key stays on your machine.

## 1. One-time GCP setup (you do this)

1. Create or pick a GCP project.
2. Create a BigQuery dataset, e.g. `nyc_taxi` (choose EU or US — keep it
   consistent with `DBT_BQ_LOCATION`).
3. Create a GCS bucket, e.g. `gs://jumma-nyc-taxi-raw`.
4. Create a **service account** with roles:
   - BigQuery Data Editor
   - BigQuery Job User
   - Storage Object Admin (on the bucket)
5. Download its JSON key to a path outside the repo, e.g.
   `~/.gcp/nyc-taxi-sa.json`. **Do not commit it.** (`.gitignore` already
   excludes `*.json` key patterns — double-check before pushing.)

## 2. Environment variables

```bash
export DBT_BQ_PROJECT=your-gcp-project-id
export DBT_BQ_DATASET=nyc_taxi
export DBT_BQ_LOCATION=EU
export DBT_BQ_KEYFILE=$HOME/.gcp/nyc-taxi-sa.json
export DLT_GCS_BUCKET=gs://jumma-nyc-taxi-raw/nyc_taxi
export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.gcp/nyc-taxi-sa.json
```

## 3. Install cloud deps

```bash
pip install -r loader/requirements.txt
```

## 4. Extract -> load (dlt: Parquet -> GCS -> BigQuery)

```bash
# download the raw Parquet locally first (same script as dev)
python scripts/download_data.py --year 2024 --months 1 2 3

# land in GCS and load into BigQuery <dataset>.yellow_tripdata
python loader/load_to_bq.py --year 2024 --months 1 2 3
```

`load_to_bq.py` uses dlt `merge` on `trip_id`, so re-running a month is
idempotent — no duplicate rows.

## 5. Transform (dbt against BigQuery)

```bash
dbt deps --profiles-dir .
dbt build --profiles-dir . --target prod
dbt docs generate --profiles-dir . --target prod
```

The `raw.yellow_tripdata` source automatically resolves to
`<project>.<dataset>.yellow_tripdata` on the prod target (see
`models/staging/_sources.yml`).

## 6. Cost notes

- BigQuery free tier: 1 TB query/month + 10 GB storage. Three months of yellow
  taxi data is a few GB and well within free limits for this project.
- The marts are materialised as tables; `fct_trips` is incremental, so repeated
  runs only scan/append new months.

## Full-cloud orchestration

The Airflow DAG (`airflow/dags/nyc_taxi_elt_dag.py`) runs the dev/DuckDB path by
default. For the cloud path, swap the `dbt_build` task's target to `prod` and
add a `load_to_bq` task between `extract` and `dbt_build`:

```
extract -> load_to_bq (dlt: GCS + BigQuery) -> dbt_build --target prod -> freshness
```

Set the same env vars on the Airflow workers (via the compose `environment:`
block or Airflow Variables/Connections).
