"""Bronze ingestion: pulls daily EOD prices from Tiingo for a watchlist and lands
raw JSON in Cloud Storage, one blob per ticker per run date."""
import argparse
import datetime as dt
import json
import os

import requests
from google.cloud import storage

TIINGO_BASE_URL = "https://api.tiingo.com/tiingo/daily"
WATCHLIST = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]


def fetch_prices(ticker: str, api_key: str, lookback_days: int = 5) -> list[dict]:
    # Lookback window (not just "today") covers weekends/holidays and any pipeline
    # downtime; validate_and_dedupe.py collapses the resulting overlap.
    start_date = (dt.date.today() - dt.timedelta(days=lookback_days)).isoformat()
    response = requests.get(
        f"{TIINGO_BASE_URL}/{ticker}/prices",
        params={"startDate": start_date, "token": api_key},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    records = response.json()
    for record in records:
        record["ticker"] = ticker
    return records


def write_to_gcs(bucket_name: str, ticker: str, run_date: str, records: list[dict]) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"bronze/{ticker}/{run_date}.json")
    # newline-delimited JSON so Spark's spark.read.json() can parse it directly
    blob.upload_from_string(
        "\n".join(json.dumps(record) for record in records),
        content_type="application/json",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--api-key", default=os.environ.get("TIINGO_API_KEY"))
    parser.add_argument("--run-date", default=dt.date.today().isoformat())
    args = parser.parse_args()

    if not args.api_key:
        raise SystemExit("Tiingo API key required: pass --api-key or set TIINGO_API_KEY")

    for ticker in WATCHLIST:
        records = fetch_prices(ticker, args.api_key)
        write_to_gcs(args.bucket, ticker, args.run_date, records)
        print(f"Wrote {len(records)} records for {ticker} to gs://{args.bucket}/bronze/{ticker}/{args.run_date}.json")


if __name__ == "__main__":
    main()
