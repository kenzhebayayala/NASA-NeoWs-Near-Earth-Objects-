"""Idempotency test: load the same data twice, the row count must not change.

Run:  python tests/test_idempotency.py
"""
import sys
import tempfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.loader import count_rows, load  # noqa: E402

df = pd.DataFrame({
    "record_id": ["1_2026-11-01", "2_2026-11-01", "3_2026-11-02"],
    "neo_id": ["1", "2", "3"],
    "name": ["A", "B", "C"],
    "nasa_jpl_url": [None, None, None],
    "absolute_magnitude_h": [20.1, 22.5, 19.0],
    "diameter_min_km": [0.1, 0.05, 0.3],
    "diameter_max_km": [0.2, 0.11, 0.6],
    "is_hazardous": [False, True, False],
    "is_sentry": [False, False, False],
    "close_approach_date": pd.to_datetime(["2026-11-01", "2026-11-01", "2026-11-02"]),
    "velocity_kmh": [50000.5, 61000.0, 43000.2],
    "miss_distance_km": [1.2e6, 3.4e6, 7.7e6],
    "orbiting_body": ["Earth", "Earth", "Earth"],
})

with tempfile.TemporaryDirectory() as folder:
    db = Path(folder) / "test.db"

    first = load(df, db)
    count_1 = count_rows(db)

    second = load(df, db)          # the SAME data again
    count_2 = count_rows(db)

    print(f"\nRows after 1st load: {count_1}")
    print(f"Rows after 2nd load: {count_2}")
    assert count_1 == count_2 == len(df), "Idempotency FAILED: duplicates were created"
    assert second["inserted"] == 0
    print("Idempotency test PASSED")
