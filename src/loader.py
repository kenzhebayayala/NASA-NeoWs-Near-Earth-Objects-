"""Step 6 (Person 1): load the CLEANED data into SQLite.

- table schema: column names, types, PRIMARY KEY (record_id)
- idempotent: running it twice with the same data does not create duplicates
  (INSERT OR IGNORE on the primary key)

Use:   from src.loader import load
       load(clean_df)
Or:    python src/loader.py data/clean_data.csv
"""
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "asteroids.db"
TABLE = "asteroids"

CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    record_id            TEXT PRIMARY KEY,      -- neo_id + '_' + close_approach_date
    neo_id               TEXT NOT NULL,
    name                 TEXT NOT NULL,
    nasa_jpl_url         TEXT,
    absolute_magnitude_h REAL,
    diameter_min_km      REAL,
    diameter_max_km      REAL,
    is_hazardous         INTEGER,               -- 0 / 1
    is_sentry            INTEGER,               -- 0 / 1
    close_approach_date  TEXT NOT NULL,         -- YYYY-MM-DD
    velocity_kmh         REAL,
    miss_distance_km     REAL,
    orbiting_body        TEXT
)
"""

COLUMNS = [
    "record_id", "neo_id", "name", "nasa_jpl_url", "absolute_magnitude_h",
    "diameter_min_km", "diameter_max_km", "is_hazardous", "is_sentry",
    "close_approach_date", "velocity_kmh", "miss_distance_km", "orbiting_body",
]


def _to_python(value):
    """pandas / numpy value -> plain Python value that SQLite accepts."""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (bool, np.bool_)):
        return int(value)
    if isinstance(value, np.generic):
        return value.item()
    return value


def count_rows(db_path=DB_PATH):
    with sqlite3.connect(db_path) as conn:
        return conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]


def load(df, db_path=DB_PATH):
    """Insert the DataFrame into SQLite. Returns a dict with the counts."""
    missing = [c for c in COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame has no columns: {missing}")

    rows = [tuple(_to_python(v) for v in row) for row in df[COLUMNS].itertuples(index=False)]

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(CREATE_TABLE)
        before = conn.total_changes
        conn.executemany(
            f"INSERT OR IGNORE INTO {TABLE} VALUES ({', '.join('?' * len(COLUMNS))})", rows
        )
        inserted = conn.total_changes - before
        total = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]

    result = {
        "received": len(rows),
        "inserted": inserted,
        "skipped_duplicates": len(rows) - inserted,
        "total_in_db": total,
    }
    print("Loader:", result)
    return result


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python src/loader.py data/clean_data.csv")
    load(pd.read_csv(sys.argv[1]))
