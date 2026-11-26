"""Step 5 (Person 2): SQL checks. Compares SQLite with data/clean.csv.

Run:  python src/sql_checks.py
"""
import sqlite3
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
con = sqlite3.connect(ROOT / "data" / "asteroids.db")

total = con.execute("SELECT COUNT(*) FROM asteroids").fetchone()[0]
unique = con.execute("SELECT COUNT(DISTINCT record_id) FROM asteroids").fetchone()[0]
haz = con.execute("SELECT COUNT(*) FROM asteroids WHERE is_hazardous = 1").fetchone()[0]
clean = pd.read_csv(ROOT / "data" / "clean.csv")

print(f"SQLite rows: {total}, unique record_id: {unique}, hazardous: {haz}")
print(f"clean.csv rows: {len(clean)}, hazardous: {int(clean.is_hazardous.sum())}")
assert total == unique == len(clean), "Row counts do not match!"
assert haz == int(clean.is_hazardous.sum()), "Hazardous counts do not match!"
print("OK: SQLite matches clean.csv")
