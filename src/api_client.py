"""Step 3 (Person 1): collect asteroid records from NASA NeoWs /feed.

- sends requests in 7-day windows, going back in time
- handles timeouts, 429 (rate limit), 4xx and 5xx errors
- flattens the nested JSON into one row per (asteroid, close approach)
- saves the raw table to data/raw_data.csv BEFORE any cleaning

Run:  python src/api_client.py
"""
import os
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

API_URL = "https://api.nasa.gov/neo/rest/v1/feed"
RAW_CSV = ROOT / "data" / "raw_data.csv"
MIN_RECORDS = 200        # collect at least this many records
WINDOW_DAYS = 7          # NeoWs allows max 7 days per request
MAX_WINDOWS = 12         # safety limit (API rate limits)

COLUMNS = [
    "record_id", "neo_id", "name", "nasa_jpl_url", "absolute_magnitude_h",
    "diameter_min_km", "diameter_max_km", "is_hazardous", "is_sentry",
    "close_approach_date", "velocity_kmh", "miss_distance_km", "orbiting_body",
]


class ApiError(Exception):
    """Raised when NASA API request cannot be completed."""


def fetch_window(start, end, api_key, max_retries=3):
    """Download one 7-day window. Returns the JSON as a dict."""
    params = {"start_date": start.isoformat(), "end_date": end.isoformat(), "api_key": api_key}

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(API_URL, params=params, timeout=30)
        except requests.Timeout:
            print(f"  timeout (attempt {attempt}/{max_retries}), retrying...")
            time.sleep(5)
            continue
        except requests.ConnectionError as error:
            print(f"  connection error (attempt {attempt}/{max_retries}): {error}")
            time.sleep(5)
            continue

        if response.status_code == 429:                 # too many requests
            try:
                wait = int(response.headers.get("Retry-After", 60))
            except ValueError:
                wait = 60
            print(f"  429 rate limit, waiting {wait} s...")
            time.sleep(wait)
            continue

        if 400 <= response.status_code < 500:           # our mistake: do not retry
            raise ApiError(f"Client error {response.status_code}: {response.text[:200]}")

        if response.status_code >= 500:                 # NASA server problem: retry
            print(f"  server error {response.status_code} (attempt {attempt}/{max_retries})")
            time.sleep(5)
            continue

        return response.json()

    raise ApiError(f"Window {start} - {end} failed after {max_retries} attempts")


def flatten(feed_json):
    """Nested JSON -> flat list of rows (one row per asteroid close approach).

    near_earth_objects -> {date: [asteroid, ...]}
    asteroid.estimated_diameter.kilometers.{min, max}
    asteroid.close_approach_data[0].{date, relative_velocity, miss_distance, orbiting_body}
    Values are kept as received (strings stay strings): cleaning is done later.
    """
    rows = []
    for _day, asteroids in feed_json.get("near_earth_objects", {}).items():
        for a in asteroids:
            km = a.get("estimated_diameter", {}).get("kilometers", {})
            for ca in a.get("close_approach_data", []):
                rows.append({
                    "record_id": f"{a.get('id')}_{ca.get('close_approach_date')}",
                    "neo_id": a.get("id"),
                    "name": a.get("name"),
                    "nasa_jpl_url": a.get("nasa_jpl_url"),
                    "absolute_magnitude_h": a.get("absolute_magnitude_h"),
                    "diameter_min_km": km.get("estimated_diameter_min"),
                    "diameter_max_km": km.get("estimated_diameter_max"),
                    "is_hazardous": a.get("is_potentially_hazardous_asteroid"),
                    "is_sentry": a.get("is_sentry_object"),
                    "close_approach_date": ca.get("close_approach_date"),
                    "velocity_kmh": ca.get("relative_velocity", {}).get("kilometers_per_hour"),
                    "miss_distance_km": ca.get("miss_distance", {}).get("kilometers"),
                    "orbiting_body": ca.get("orbiting_body"),
                })
    return rows


def collect_and_save(min_records=MIN_RECORDS, out_path=RAW_CSV):
    """Collect >= min_records rows, save raw CSV, return the DataFrame."""
    api_key = os.getenv("NASA_API_KEY", "DEMO_KEY")
    if api_key == "DEMO_KEY":
        print("Warning: using DEMO_KEY (very small limits). Put your key in .env")

    rows = []
    end = date.today()
    windows = 0
    while len(rows) < min_records and windows < MAX_WINDOWS:
        start = end - timedelta(days=WINDOW_DAYS - 1)
        new_rows = flatten(fetch_window(start, end, api_key))
        rows += new_rows
        windows += 1
        print(f"Window {start} .. {end}: {len(new_rows)} records (total {len(rows)})")
        end = start - timedelta(days=1)          # next window goes further back
        time.sleep(1)                            # be polite to the API

    if len(rows) < min_records:
        print(f"Warning: collected only {len(rows)} records (< {min_records})")

    df = pd.DataFrame(rows, columns=COLUMNS)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Saved {len(df)} raw records to {out_path}")
    return df


if __name__ == "__main__":
    collect_and_save()
