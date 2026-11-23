"""Step 3 (Person 2): Pandera validation BEFORE cleaning.

Reads data/consumed.csv, adds 3 deliberately bad records, validates,
splits into data/valid.csv and data/invalid.csv.

Run:  python src/validator.py
"""
from pathlib import Path

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema

ROOT = Path(__file__).resolve().parent.parent
IN_CSV = ROOT / "data" / "consumed.csv"
VALID_CSV = ROOT / "data" / "valid.csv"
INVALID_CSV = ROOT / "data" / "invalid.csv"

NUM_COLS = ["absolute_magnitude_h", "diameter_min_km", "diameter_max_km",
            "velocity_kmh", "miss_distance_km"]


def to_types(df):
    """Kafka gave us text: restore types. Bad values become NaN and fail validation."""
    df = df.copy()
    for col in NUM_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["is_hazardous", "is_sentry"]:
        df[col] = df[col].map({"True": True, "False": False})   # NOT astype(bool)!
    return df


def add_bad_records(df):
    """3 deliberate errors of different kinds. Marked with is_injected=True."""
    base = df.iloc[0]
    bad = pd.DataFrame([base, base, base]).reset_index(drop=True)
    bad.loc[0, "record_id"] = "BAD_NEGATIVE_DIAMETER"
    bad.loc[0, "diameter_max_km"] = -5.0                  # range error
    bad.loc[1, "record_id"] = "BAD_DATE"
    bad.loc[1, "close_approach_date"] = "32-13-2026"      # format error
    # bad.loc[2]: keeps base record_id                    # uniqueness error
    df = df.assign(is_injected=False)
    bad["is_injected"] = True
    return pd.concat([df, bad], ignore_index=True)


schema = DataFrameSchema({
    "record_id": Column(nullable=False, unique=True, report_duplicates="exclude_first"),
    "neo_id": Column(nullable=False, checks=Check.str_matches(r"^\d+$")),
    "name": Column(nullable=False),
    "absolute_magnitude_h": Column(float, Check.in_range(0, 40), nullable=False),
    "diameter_min_km": Column(float, Check.gt(0), nullable=False),
    "diameter_max_km": Column(float, Check.gt(0), nullable=False),
    "is_hazardous": Column(nullable=False, checks=Check.isin([True, False])),
    "close_approach_date": Column(nullable=False, checks=Check(
        lambda s: pd.to_datetime(s, format="%Y-%m-%d", errors="coerce").notna(),
        error="date must be YYYY-MM-DD")),
    "velocity_kmh": Column(float, Check.in_range(0, 300000), nullable=False),
    "miss_distance_km": Column(float, Check.ge(0), nullable=True),
    "orbiting_body": Column(checks=Check.isin(["Earth"])),
})


def validate(df):
    try:
        schema.validate(df.drop(columns="is_injected"), lazy=True)
        failures = pd.DataFrame(columns=["index", "column", "check"])
    except pa.errors.SchemaErrors as err:
        failures = err.failure_cases
    bad_idx = sorted(set(failures["index"].dropna().astype(int)))
    return bad_idx, failures


if __name__ == "__main__":
    raw = pd.read_csv(IN_CSV, dtype=str)
    print(f"Consumed: {len(raw)}")
    df = add_bad_records(to_types(raw))
    print(f"After adding 3 test errors: {len(df)}")

    bad_idx, failures = validate(df)
    invalid = df.loc[bad_idx]
    valid = df.drop(index=bad_idx)

    print(f"Valid: {len(valid)}, Invalid: {len(invalid)}")
    print(f"  injected caught: {int(invalid.is_injected.sum())} of 3")
    print(f"  natural errors:  {int((~invalid.is_injected).sum())}")
    print(failures[["index", "column", "check"]].to_string())

    invalid.to_csv(INVALID_CSV, index=False)
    valid.drop(columns="is_injected").to_csv(VALID_CSV, index=False)