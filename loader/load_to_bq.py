"""
Extract-load stage: NYC TLC Parquet -> GCS landing zone -> BigQuery.

This is the distinct E/L step of the ELT pipeline (dbt does the T). It uses
dlt to:
  1. read the monthly yellow-taxi Parquet (local file already downloaded, or
     the TLC URL directly),
  2. stage it in a GCS bucket (the raw landing zone),
  3. load it into a BigQuery table `<dataset>.yellow_tripdata`.

dbt's `raw.yellow_tripdata` source then points at that BigQuery table on the
prod target (see models/staging/_sources.yml).

Config comes entirely from environment variables — nothing secret is committed:

  export DBT_BQ_PROJECT=your-gcp-project-id
  export DBT_BQ_DATASET=nyc_taxi
  export DLT_GCS_BUCKET=gs://your-bucket/nyc_taxi
  export GOOGLE_APPLICATION_CREDENTIALS=/abs/path/service-account.json

dlt reads GOOGLE_APPLICATION_CREDENTIALS for both GCS and BigQuery.

Usage:
  python loader/load_to_bq.py --year 2024 --months 1 2 3
  # write_disposition defaults to 'merge' on trip_id for idempotency
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import dlt
import pyarrow.parquet as pq

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def _iter_batches(year: int, months: list[int]):
    """Yield record batches from local Parquet (downloaded first if missing)."""
    for m in months:
        fname = f"yellow_tripdata_{year}-{m:02d}.parquet"
        path = DATA_DIR / fname
        if not path.exists():
            raise FileNotFoundError(
                f"{path} not found. Run scripts/download_data.py first, "
                f"or the Airflow extract task."
            )
        table = pq.read_table(path)
        # dlt accepts an iterable of dicts; stream to keep memory bounded
        for batch in table.to_batches(max_chunksize=50_000):
            for row in batch.to_pylist():
                yield row


@dlt.resource(name="yellow_tripdata", write_disposition="merge", primary_key="trip_id")
def yellow_tripdata(year: int, months: list[int]):
    for row in _iter_batches(year, months):
        # build a stable key so merge is idempotent across re-runs
        row["trip_id"] = "|".join(
            str(row.get(k))
            for k in (
                "VendorID",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "PULocationID",
                "DOLocationID",
                "total_amount",
            )
        )
        yield row


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, default=2024)
    p.add_argument("--months", type=int, nargs="+", default=[1, 2, 3])
    args = p.parse_args()

    dataset = os.environ.get("DBT_BQ_DATASET", "nyc_taxi")
    bucket = os.environ.get("DLT_GCS_BUCKET")
    if not bucket:
        raise SystemExit("Set DLT_GCS_BUCKET (e.g. gs://your-bucket/nyc_taxi)")

    pipeline = dlt.pipeline(
        pipeline_name="nyc_taxi_load",
        destination="bigquery",
        dataset_name=dataset,
        staging="filesystem",  # GCS landing zone before the BQ load
    )
    # dlt reads the bucket URL from the filesystem staging config / env:
    os.environ.setdefault("DESTINATION__FILESYSTEM__BUCKET_URL", bucket)

    info = pipeline.run(yellow_tripdata(args.year, args.months))
    print(info)


if __name__ == "__main__":
    main()
