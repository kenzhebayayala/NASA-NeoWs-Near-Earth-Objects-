"""Step 4 (Person 2): cleaning with pandas / NumPy.

Reads data/valid.csv (already validated), writes data/clean.csv.
Every operation has a reason (see docs/cleaning_log.md).

Run:  python src/cleaner.py
"""
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
IN_CSV = ROOT / "data" / "valid.csv"
OUT_CSV = ROOT / "data" / "clean.csv"
BY_DATE_CSV = ROOT / "data" / "agg_by_date.csv"
PIVOT_CSV = ROOT / "data" / "pivot_size_hazard.csv"

NUM_COLS = ["absolute_magnitude_h", "diameter_min_km", "diameter_max_km",
            "velocity_kmh", "miss_distance_km"]


def clean(df):
    print(f"Input (valid): {len(df)}")

    # 0. Numeric types first.
    # Reason: the CSV is read as text; median and NumPy need real numbers.
    for col in NUM_COLS:
        df[col] = pd.to_numeric(df[col], errors="raise").astype(float)

    # 1. Missing values.
    # Reason: median is robust to outliers (miss_distance is very skewed).
    missing = df.isna().sum()
    print("Missing before:", missing[missing > 0].to_dict() or "none")
    df["miss_distance_filled"] = df["miss_distance_km"].isna()   # flag: do not hide the fact
    df["miss_distance_km"] = df["miss_distance_km"].fillna(df["miss_distance_km"].median())

    # 2. Duplicates.
    # Reason: record_id is the primary key, one row per approach.
    before = len(df)
    df = df.drop_duplicates(subset="record_id", keep="first")
    print(f"Duplicates removed: {before - len(df)}")

    # 3. Bool, date and id types.
    # Reason: SQLite and analysis need real types. map() instead of astype(bool),
    # because the text "False" would become True with astype(bool).
    for col in ["is_hazardous", "is_sentry"]:
        df[col] = df[col].astype(str).map({"True": True, "False": False}).astype(bool)
    df["close_approach_date"] = pd.to_datetime(df["close_approach_date"], format="%Y-%m-%d")
    df["neo_id"] = df["neo_id"].astype(str)

    # 4. Text normalization.
    # Reason: "(2004 XK14)" -> "2004 XK14", one format for grouping and joins.
    df["name"] = (df["name"].astype(str).str.strip()
                  .str.replace(r"[()]", "", regex=True)
                  .str.replace(r"\s+", " ", regex=True)
                  .str.upper())
    df["orbiting_body"] = df["orbiting_body"].astype(str).str.strip().str.title()

    # 5. NumPy operations.
    # Reason: new features for analysis.
    df["diameter_avg_km"] = np.mean([df["diameter_min_km"], df["diameter_max_km"]], axis=0)
    df["log_velocity"] = np.log10(df["velocity_kmh"])
    df["miss_distance_lunar"] = df["miss_distance_km"] / 384_400     # in Moon distances
    df["size_class"] = np.select(
        [df["diameter_avg_km"] < 0.1, df["diameter_avg_km"] < 0.5],
        ["small", "medium"], default="large")

    return df.reset_index(drop=True)


if __name__ == "__main__":
    df = clean(pd.read_csv(IN_CSV, dtype=str))

    # 6. Multi-condition filter (analysis only, does NOT remove rows from clean.csv).
    big_fast = df[(df["diameter_avg_km"] > 0.3)
                  & (df["velocity_kmh"] > 50_000)
                  & (df["miss_distance_lunar"] < 20)]
    print(f"Big + fast + close (<20 lunar): {len(big_fast)}")

    # 7. GroupBy + aggregation.
    # Reason: how many approaches and how fast per day.
    by_date = (df.groupby("close_approach_date")
                 .agg(n=("record_id", "count"),
                      avg_velocity=("velocity_kmh", "mean"),
                      max_diameter=("diameter_max_km", "max"),
                      hazardous=("is_hazardous", "sum"))
                 .reset_index())
    print(by_date.head().to_string())

    # 8. Reshape (pivot_table).
    # Reason: compare size classes vs hazard in one table.
    pivot = df.pivot_table(index="size_class", columns="is_hazardous",
                           values="velocity_kmh", aggfunc=["count", "mean"],
                           fill_value=0)
    print(pivot.to_string())

    df.to_csv(OUT_CSV, index=False)
    by_date.to_csv(BY_DATE_CSV, index=False)
    pivot.to_csv(PIVOT_CSV)
    print(f"Clean: {len(df)} rows, {df.shape[1]} columns -> {OUT_CSV}")
    print("Columns:", list(df.columns))