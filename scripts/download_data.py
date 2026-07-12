"""
Download NYC TLC Yellow Taxi trip data (Parquet) and the taxi zone lookup.

The TLC publishes one Parquet file per month:
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet

Zone lookup (CSV):
  https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

Usage:
  python scripts/download_data.py --year 2024 --months 1 2 3
"""
import argparse
import urllib.request
from pathlib import Path

BASE = "https://d37ci6vzurychx.cloudfront.net"
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SEEDS_DIR = Path(__file__).resolve().parent.parent / "seeds"


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        print(f"  skip (exists): {dest.name}")
        return
    print(f"  downloading: {url}")
    urllib.request.urlretrieve(url, dest)
    print(f"  saved: {dest} ({dest.stat().st_size/1e6:.1f} MB)")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, default=2024)
    p.add_argument("--months", type=int, nargs="+", default=[1, 2, 3])
    args = p.parse_args()

    print("Trip data:")
    for m in args.months:
        fname = f"yellow_tripdata_{args.year}-{m:02d}.parquet"
        download(f"{BASE}/trip-data/{fname}", DATA_DIR / fname)

    print("Zone lookup:")
    download(
        f"{BASE}/misc/taxi_zone_lookup.csv",
        SEEDS_DIR / "taxi_zone_lookup.csv",
    )
    print("Done.")


if __name__ == "__main__":
    main()
