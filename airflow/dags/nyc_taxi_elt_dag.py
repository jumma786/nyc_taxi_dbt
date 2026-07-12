"""
NYC Taxi ELT pipeline — Airflow orchestration.

Monthly schedule. For each logical run it:
  1. extract   — download that month's yellow-taxi Parquet to the raw landing zone
  2. load_check — verify the file landed and is readable
  3. dbt_build  — run the full dbt project (models + tests) against DuckDB
  4. freshness  — assert the mart actually contains rows for the loaded month

The DAG is idempotent: re-running a month re-downloads (skipped if present)
and dbt's incremental fct_trips only processes new pickups.

Environment assumptions (override via Airflow Variables or env vars):
  PROJECT_DIR  — path to the dbt project root (default: this repo)
  DBT_TARGET   — dbt target/profile output name (default: dev)
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_DIR = os.environ.get(
    "PROJECT_DIR", str(Path(__file__).resolve().parents[2])
)
DBT_TARGET = os.environ.get("DBT_TARGET", "dev")

default_args = {
    "owner": "jumma",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}


def _extract(logical_year: int, logical_month: int, **_) -> str:
    """Download the month's Parquet into data/ (the raw landing zone)."""
    import subprocess

    cmd = [
        "python",
        f"{PROJECT_DIR}/scripts/download_data.py",
        "--year",
        str(logical_year),
        "--months",
        str(logical_month),
    ]
    subprocess.run(cmd, check=True, cwd=PROJECT_DIR)
    fname = f"data/yellow_tripdata_{logical_year}-{logical_month:02d}.parquet"
    path = Path(PROJECT_DIR) / fname
    if not path.exists():
        raise FileNotFoundError(f"Expected {path} after extract")
    return str(path)


def _load_check(**context) -> None:
    """Confirm the landed Parquet is non-empty and readable."""
    import duckdb

    path = context["ti"].xcom_pull(task_ids="extract")
    con = duckdb.connect()
    n = con.sql(f"select count(*) from read_parquet('{path}')").fetchone()[0]
    if n == 0:
        raise ValueError(f"Landed file {path} has 0 rows")
    print(f"Loaded {n:,} raw rows from {path}")


def _freshness_check(logical_year: int, logical_month: int, **_) -> None:
    """After dbt build, confirm the mart has rows for the loaded month."""
    import duckdb

    db = Path(PROJECT_DIR) / "nyc_taxi.duckdb"
    con = duckdb.connect(str(db))
    month_start = f"{logical_year}-{logical_month:02d}-01"
    n = con.sql(
        f"""
        select count(*) from fct_trips
        where pickup_date >= date '{month_start}'
          and pickup_date <  date '{month_start}' + interval 1 month
        """
    ).fetchone()[0]
    print(f"fct_trips rows for {month_start}: {n:,}")
    if n == 0:
        raise ValueError(f"No fct_trips rows for {month_start} after build")


with DAG(
    dag_id="nyc_taxi_elt",
    description="NYC taxi ELT: extract Parquet, land, dbt build + test, verify.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "duckdb", "elt", "nyc-taxi"],
) as dag:

    # For a real backfill, derive year/month from the data interval:
    #   {{ data_interval_start.year }}, {{ data_interval_start.month }}
    # Defaults below keep manual triggers simple.
    op_kwargs = {"logical_year": 2024, "logical_month": 1}

    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract,
        op_kwargs=op_kwargs,
    )

    load_check = PythonOperator(
        task_id="load_check",
        python_callable=_load_check,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"dbt build --profiles-dir . --target {DBT_TARGET}"
        ),
    )

    freshness = PythonOperator(
        task_id="freshness_check",
        python_callable=_freshness_check,
        op_kwargs=op_kwargs,
    )

    extract >> load_check >> dbt_build >> freshness
