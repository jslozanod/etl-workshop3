"""
setup_pipeline.py
Runs the full offline ETL pipeline in one shot:
  1. Loads and harmonizes all 5 CSVs
  2. Trains the Random Forest model
  3. Saves unified_happiness.csv and model.pkl

Run this once before starting the Kafka producer/consumer.
"""

import json
import os
import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")

BASE_DIR      = Path(__file__).resolve().parent
RAW_DIR       = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
MODELS_DIR    = BASE_DIR / "models"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
MODELS_DIR.mkdir(parents=True, exist_ok=True)

FEATURES = [
    "gdp_per_capita",
    "social_support",
    "health_life_expectancy",
    "freedom",
    "generosity",
    "corruption",
]
TARGET = "happiness_score"

# ── Step 1: Load raw CSVs ────────────────────────────────────────────────────
print("=" * 55)
print(" STEP 1 — Loading raw datasets")
print("=" * 55)

years = [2015, 2016, 2017, 2018, 2019]
raw = {}
for y in years:
    path = RAW_DIR / f"{y}.csv"
    raw[y] = pd.read_csv(path)
    print(f"  {y}: {raw[y].shape[0]} rows × {raw[y].shape[1]} cols")

# ── Step 2: Harmonize schemas ────────────────────────────────────────────────
print("\n" + "=" * 55)
print(" STEP 2 — Harmonizing schemas")
print("=" * 55)

def harmonize(df, year):
    if year in (2015, 2016):
        return pd.DataFrame({
            "country":                df["Country"],
            "region":                 df["Region"],
            "year":                   year,
            "happiness_rank":         df["Happiness Rank"],
            "happiness_score":        df["Happiness Score"],
            "gdp_per_capita":         df["Economy (GDP per Capita)"],
            "social_support":         df["Family"],
            "health_life_expectancy": df["Health (Life Expectancy)"],
            "freedom":                df["Freedom"],
            "generosity":             df["Generosity"],
            "corruption":             df["Trust (Government Corruption)"],
            "dystopia_residual":      df["Dystopia Residual"],
        })
    elif year == 2017:
        return pd.DataFrame({
            "country":                df["Country"],
            "region":                 np.nan,
            "year":                   year,
            "happiness_rank":         df["Happiness.Rank"],
            "happiness_score":        df["Happiness.Score"],
            "gdp_per_capita":         df["Economy..GDP.per.Capita."],
            "social_support":         df["Family"],
            "health_life_expectancy": df["Health..Life.Expectancy."],
            "freedom":                df["Freedom"],
            "generosity":             df["Generosity"],
            "corruption":             df["Trust..Government.Corruption."],
            "dystopia_residual":      df["Dystopia.Residual"],
        })
    else:  # 2018, 2019
        return pd.DataFrame({
            "country":                df["Country or region"],
            "region":                 np.nan,
            "year":                   year,
            "happiness_rank":         df["Overall rank"],
            "happiness_score":        df["Score"],
            "gdp_per_capita":         df["GDP per capita"],
            "social_support":         df["Social support"],
            "health_life_expectancy": df["Healthy life expectancy"],
            "freedom":                df["Freedom to make life choices"],
            "generosity":             df["Generosity"],
            "corruption":             df["Perceptions of corruption"],
            "dystopia_residual":      np.nan,
        })

unified = pd.concat([harmonize(raw[y], y) for y in years], ignore_index=True)

# Fix 1 null in 2018 corruption
median_corruption = unified[unified["year"] == 2018]["corruption"].median()
unified["corruption"] = unified["corruption"].fillna(median_corruption)
unified["country"] = unified["country"].str.strip()
unified["year"] = unified["year"].astype(int)
unified["happiness_rank"] = unified["happiness_rank"].astype(int)

out_csv = PROCESSED_DIR / "unified_happiness.csv"
unified.to_csv(out_csv, index=False)
print(f"  Unified dataset: {unified.shape[0]} rows × {unified.shape[1]} cols")
print(f"  Saved -> {out_csv}")

# ── Step 3: Train model ──────────────────────────────────────────────────────
print("\n" + "=" * 55)
print(" STEP 3 — Training Random Forest model")
print("=" * 55)

ml_df = unified[FEATURES + [TARGET]].dropna()
X = ml_df[FEATURES]
y = ml_df[TARGET]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.30, random_state=42
)
print(f"  Train: {len(X_train)} rows | Test: {len(X_test)} rows")

pipe = Pipeline([
    ("scaler", StandardScaler()),
    ("model",  RandomForestRegressor(n_estimators=100, random_state=42)),
])
pipe.fit(X_train, y_train)
y_pred = pipe.predict(X_test)

mae  = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2   = r2_score(y_test, y_pred)

print(f"  MAE  = {mae:.4f}")
print(f"  RMSE = {rmse:.4f}")
print(f"  R²   = {r2:.4f}")

# ── Step 4: Save model ───────────────────────────────────────────────────────
model_path = MODELS_DIR / "model.pkl"
joblib.dump(pipe, model_path)

metadata = {
    "model_name": "RandomForestRegressor",
    "features": FEATURES,
    "target": TARGET,
    "test_mae": round(mae, 4),
    "test_rmse": round(rmse, 4),
    "test_r2": round(r2, 4),
    "train_rows": len(X_train),
    "test_rows": len(X_test),
}
with open(MODELS_DIR / "model_metadata.json", "w") as f:
    json.dump(metadata, f, indent=2)

print(f"  Model saved -> {model_path}")

# ── Sanity check ─────────────────────────────────────────────────────────────
sample = pd.DataFrame([{
    "gdp_per_capita": 1.2, "social_support": 0.8,
    "health_life_expectancy": 0.9, "freedom": 0.6,
    "generosity": 0.3, "corruption": 0.1,
}])
pred = pipe.predict(sample)[0]
print(f"\n  Sample prediction (Colombia-like): {pred:.4f}")

print("\n" + "=" * 55)
print(" Pipeline setup complete.")
print(" You can now run the Kafka producer and consumer.")
print("=" * 55)
