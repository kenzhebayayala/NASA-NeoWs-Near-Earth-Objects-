"""Step 6 (Person 2): EDA. Reads data/clean.csv, saves 3 figures to figures/.

Run:  python src/analysis.py
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
FIG = ROOT / "figures"
FIG.mkdir(exist_ok=True)

df = pd.read_csv(ROOT / "data" / "clean.csv", parse_dates=["close_approach_date"])

# --- Overview ---
print("Shape:", df.shape)
print(df.dtypes)
print(df[["diameter_avg_km", "velocity_kmh", "miss_distance_lunar"]].describe().round(3))
print(df.groupby("is_hazardous")[["diameter_avg_km", "velocity_kmh", "miss_distance_lunar"]]
        .agg(["mean", "median"]).round(3))

# --- Figure 1: diameter histogram (log x-axis) ---
plt.figure(figsize=(8, 5))
bins = np.logspace(np.log10(df["diameter_avg_km"].min()), np.log10(df["diameter_avg_km"].max()), 30)
plt.hist(df["diameter_avg_km"], bins=bins, color="steelblue", edgecolor="white", label="Asteroids")
plt.xscale("log")
plt.title("Distribution of asteroid diameter")
plt.xlabel("Average diameter, km (log scale)")
plt.ylabel("Number of asteroids")
plt.legend()
plt.savefig(FIG / "fig1_diameter_hist.png", dpi=150, bbox_inches="tight")
plt.close()

# --- Figure 2: diameter vs velocity, hazardous vs not ---
plt.figure(figsize=(8, 5))
for flag, color in [(False, "tab:blue"), (True, "tab:red")]:
    g = df[df["is_hazardous"] == flag]
    plt.scatter(g["diameter_avg_km"], g["velocity_kmh"], s=25, alpha=0.7, c=color,
                label=f"Hazardous: {flag} (n={len(g)})")
plt.xscale("log")
plt.title("Diameter vs velocity")
plt.xlabel("Average diameter, km (log scale)")
plt.ylabel("Velocity, km/h")
plt.legend()
plt.savefig(FIG / "fig2_diameter_vs_velocity.png", dpi=150, bbox_inches="tight")
plt.close()

# --- Figure 3: approaches per day, hazardous stacked ---
daily = (df.groupby([df["close_approach_date"].dt.date, "is_hazardous"])
           .size().unstack(fill_value=0)
           .reindex(columns=[False, True], fill_value=0))
ax = daily.plot(kind="bar", stacked=True, figsize=(11, 5), color=["tab:blue", "tab:red"], width=0.85)
ax.set_title("Close approaches per day")
ax.set_xlabel("Date")
ax.set_ylabel("Number of approaches")
ax.legend(["Not hazardous", "Hazardous"])
plt.xticks(rotation=90, fontsize=7)
plt.savefig(FIG / "fig3_approaches_per_day.png", dpi=150, bbox_inches="tight")
plt.close()

print("Saved 3 figures to", FIG)