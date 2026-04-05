"""
Train and persist the champion bus-delay model for the dashboard.

Reproduces the pipeline from `train_delay_model_train_only.ipynb`:
  1. Loads raw CSVs from db_export_combined
  2. Builds the Enhanced feature set (cyclical time + historical aggregates)
  3. Temporal split (80 / 10 / 10)
  4. Fits TWO Pipelines on the same features:
       - Ridge Enhanced           (linear, L2 loss — baseline)
       - Nystroem + LinearSVR     (RBF kernel approximation, L1 loss — champion)
  5. Evaluates both on the test split
  6. Saves artifacts to dashboard/models/ and dashboard/data/

Run once:
    python train_model.py
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.kernel_approximation import Nystroem
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.svm import LinearSVR

# ---------------------------------------------------------------------------
# Paths and constants
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "db_export_combined"
DASH_ROOT    = Path(__file__).resolve().parent
MODELS_DIR   = DASH_ROOT / "models"
DATA_OUT     = DASH_ROOT / "data"
MODELS_DIR.mkdir(parents=True, exist_ok=True)
DATA_OUT.mkdir(parents=True, exist_ok=True)

SELECTED_ROUTES = [
    "130", "123", "144", "106", "160", "110", "116",
    "006", "019",
    "049", "100",
    "240", "255",
]

# ---------------------------------------------------------------------------
# 1. Load raw data
# ---------------------------------------------------------------------------
def load_raw() -> dict:
    print(f"[1/6] Loading CSVs from {DATA_DIR.name} ...")
    t0 = time.time()

    bus = pd.read_csv(
        DATA_DIR / "bus_delays.csv",
        dtype={"route_id": "Int64", "stop_id": "Int64",
               "trip_id": "Int64", "delay_seconds": "Int64",
               "vehicle_id": "Int64"},
    )
    routes = pd.read_csv(DATA_DIR / "routes.csv")
    stops  = pd.read_csv(DATA_DIR / "stops.csv")
    trips  = pd.read_csv(DATA_DIR / "trips.csv")
    weather = pd.read_csv(DATA_DIR / "weather.csv", dtype={"id": str})
    road   = pd.read_csv(DATA_DIR / "road_conditions.csv")

    # Coerce join keys
    for df, cols in [(routes, ["route_id"]),
                     (stops,  ["stop_id"]),
                     (trips,  ["trip_id"])]:
        for c in cols:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # Mix of microsecond precisions across rows → force ISO8601 / mixed parsing.
    def _parse_ts(s):
        try:
            return pd.to_datetime(s, format="ISO8601", utc=True)
        except (ValueError, TypeError):
            return pd.to_datetime(s, format="mixed", utc=True)
    bus["recorded_at"]     = _parse_ts(bus["recorded_at"])
    weather["recorded_at"] = _parse_ts(weather["recorded_at"])

    print(f"       loaded in {time.time()-t0:.1f}s — bus={len(bus):,}, weather={len(weather):,}, road={len(road):,}")
    return {"bus": bus, "routes": routes, "stops": stops, "trips": trips,
            "weather": weather, "road": road}


# ---------------------------------------------------------------------------
# 2. Build bus_df with Base features
# ---------------------------------------------------------------------------
def build_base(raw: dict) -> pd.DataFrame:
    print("[2/6] Building base feature frame ...")
    bus     = raw["bus"]
    routes  = raw["routes"]
    stops   = raw["stops"]
    trips   = raw["trips"]
    weather = raw["weather"]
    road    = raw["road"]

    bus = bus[bus["route_id"].notna()]
    routes["route_short_name"] = routes["route_short_name"].astype(str).str.zfill(3)

    bus_df = (
        bus.merge(routes[["route_id", "route_short_name", "route_long_name"]], on="route_id", how="inner")
           .merge(stops[["stop_id", "stop_lat", "stop_lon", "stop_name"]], on="stop_id", how="inner")
           .merge(trips[["trip_id", "direction_id"]], on="trip_id", how="inner")
    )
    bus_df = bus_df[bus_df["route_short_name"].isin(SELECTED_ROUTES)].copy()

    # Time features
    bus_df["hour"]         = bus_df["recorded_at"].dt.hour
    bus_df["day_of_week"]  = bus_df["recorded_at"].dt.dayofweek
    bus_df["is_weekend"]   = (bus_df["day_of_week"] >= 5).astype(int)
    bus_df["is_rush_hour"] = bus_df["hour"].isin([7, 8, 9, 16, 17, 18]).astype(int)

    # Stop sequence — average over trips so each stop_id has one value
    stop_seq = (
        pd.read_csv(DATA_DIR / "stop_times.csv", usecols=["stop_id", "stop_sequence"])
        .groupby("stop_id")["stop_sequence"].mean().round().astype("Int64")
    )
    bus_df["stop_sequence"] = bus_df["stop_id"].map(stop_seq).astype("Int64")

    # Weather (10-min bucket, filter >= 2026-01-10 like the notebook)
    weather = weather[weather["recorded_at"] >= "2026-01-10"].copy()
    weather["weather_time"] = weather["recorded_at"].dt.floor("10min")
    weather_agg = weather.groupby("weather_time").agg(
        temperature_c=("temperature_c", "mean"),
        humidity_percent=("humidity_percent", "mean"),
        wind_speed_kmh=("wind_speed_kmh", "mean"),
        precipitation_mm=("precipitation_mm", "mean"),
    ).reset_index()

    bus_df["weather_time"] = bus_df["recorded_at"].dt.floor("10min")
    bus_df = bus_df.merge(weather_agg, on="weather_time", how="left")
    wcols = ["temperature_c", "humidity_percent", "wind_speed_kmh", "precipitation_mm"]
    bus_df[wcols] = bus_df[wcols].ffill().bfill()

    # Static road features (kept for feature parity with the notebook)
    road = road[road["lat"].notna()].copy()

    def _haversine(lat1, lon1, lat2, lon2):
        R = 6371.0
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
        return R * 2 * np.arcsin(np.sqrt(a))

    bus_df["active_incidents"]        = 0
    bus_df["active_construction"]     = 0
    bus_df["nearest_event_distance_km"] = 50.0
    for _, stop in bus_df[["stop_id", "stop_lat", "stop_lon"]].drop_duplicates().iterrows():
        if len(road) == 0:
            continue
        dists = _haversine(stop["stop_lat"], stop["stop_lon"], road["lat"].values, road["lon"].values)
        nearby = road[dists < 5]
        mask = bus_df["stop_id"] == stop["stop_id"]
        bus_df.loc[mask, "active_incidents"]    = int((nearby["event_type"] == "INCIDENT").sum())
        bus_df.loc[mask, "active_construction"] = int((nearby["event_type"] == "CONSTRUCTION").sum())
        bus_df.loc[mask, "nearest_event_distance_km"] = float(dists.min()) if len(dists) else 50.0

    # Clean
    bus_df = bus_df.dropna(subset=["delay_seconds", "stop_lat", "stop_lon", "stop_sequence"])
    bus_df = bus_df.sort_values("recorded_at").reset_index(drop=True)
    print(f"       base frame rows = {len(bus_df):,}")
    return bus_df


# ---------------------------------------------------------------------------
# 3. Add Enhanced features (cyclical + historical aggregates)
# ---------------------------------------------------------------------------
BASE_COLS = [
    "hour", "day_of_week", "is_weekend", "is_rush_hour",
    "route_short_name", "direction_id", "stop_sequence", "stop_lat", "stop_lon",
    "temperature_c", "humidity_percent", "wind_speed_kmh", "precipitation_mm",
    "active_incidents", "active_construction", "nearest_event_distance_km",
]
ENHANCED_EXTRA = [
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "route_hour_mean_delay", "stop_mean_delay", "route_dir_mean_delay",
]
FEATURE_COLS = BASE_COLS + ENHANCED_EXTRA


def build_enhanced(bus_df: pd.DataFrame, train_end: int):
    print("[3/6] Adding Enhanced features ...")
    df = bus_df.copy()

    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["dow_sin"]  = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["dow_cos"]  = np.cos(2 * np.pi * df["day_of_week"] / 7)

    # Aggregates from TRAIN split only
    train = df.iloc[:train_end]
    global_mean = float(train["delay_seconds"].mean())

    route_hour_mean = train.groupby(["route_short_name", "hour"])["delay_seconds"].mean()
    stop_mean       = train.groupby("stop_id")["delay_seconds"].mean()
    route_dir_mean  = train.groupby(["route_short_name", "direction_id"])["delay_seconds"].mean()

    df["route_hour_mean_delay"] = (
        df.set_index(["route_short_name", "hour"]).index.map(route_hour_mean)
    )
    df["route_hour_mean_delay"] = df["route_hour_mean_delay"].fillna(global_mean)
    df["stop_mean_delay"]       = df["stop_id"].map(stop_mean).fillna(global_mean)
    df["route_dir_mean_delay"]  = (
        df.set_index(["route_short_name", "direction_id"]).index.map(route_dir_mean)
    )
    df["route_dir_mean_delay"]  = df["route_dir_mean_delay"].fillna(global_mean)

    aggregates = {
        "route_hour_mean":
            {f"{r}|{h}": float(v) for (r, h), v in route_hour_mean.items()},
        "stop_mean":
            {int(k): float(v) for k, v in stop_mean.items()},
        "route_dir_mean":
            {f"{r}|{int(d)}": float(v) for (r, d), v in route_dir_mean.items()},
        "global_mean": global_mean,
    }
    return df, aggregates


# ---------------------------------------------------------------------------
# 4. Build Pipelines (ColumnTransformer inside)
# ---------------------------------------------------------------------------
def make_pipelines():
    num_cols = [c for c in FEATURE_COLS if c != "route_short_name"]
    preprocessor = ColumnTransformer(
        transformers=[
            ("num",   "passthrough", num_cols),
            ("route", OneHotEncoder(handle_unknown="ignore", sparse_output=False),
             ["route_short_name"]),
        ]
    )

    ridge_pipe = Pipeline([
        ("prep", preprocessor),
        ("ridge", Ridge(alpha=1.0)),
    ])

    nystroem_pipe = Pipeline([
        ("prep", preprocessor),
        ("scaler", StandardScaler()),
        ("nystroem", Nystroem(kernel="rbf", gamma=None,
                              n_components=300, random_state=42)),
        ("svr", LinearSVR(C=1.0, epsilon=0.0, max_iter=2000, random_state=42)),
    ])
    return ridge_pipe, nystroem_pipe


# ---------------------------------------------------------------------------
# 5. Metrics helper
# ---------------------------------------------------------------------------
def eval_model(name, model, X_test, y_test):
    y_pred = model.predict(X_test)
    m = {
        "mae":  float(mean_absolute_error(y_test, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_test, y_pred))),
        "r2":   float(r2_score(y_test, y_pred)),
    }
    print(f"       {name}: MAE={m['mae']:.2f}s  RMSE={m['rmse']:.2f}s  R2={m['r2']:.4f}")
    return m, y_pred


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    raw = load_raw()
    bus_df = build_base(raw)

    # Temporal split boundaries
    n = len(bus_df)
    train_end = int(n * 0.80)
    val_end   = int(n * 0.90)

    bus_enh, aggregates = build_enhanced(bus_df, train_end)

    X = bus_enh[FEATURE_COLS]
    y = bus_enh["delay_seconds"].astype(float)

    X_train, y_train = X.iloc[:train_end],      y.iloc[:train_end]
    X_val,   y_val   = X.iloc[train_end:val_end], y.iloc[train_end:val_end]
    X_test,  y_test  = X.iloc[val_end:],        y.iloc[val_end:]
    print(f"       split: train={len(X_train):,}  val={len(X_val):,}  test={len(X_test):,}")

    ridge_pipe, nystroem_pipe = make_pipelines()

    print("[4/6] Fitting Ridge (Enhanced) ...")
    t0 = time.time()
    ridge_pipe.fit(X_train, y_train)
    print(f"       fit done in {time.time()-t0:.1f}s")
    ridge_metrics, ridge_pred_test = eval_model("Ridge", ridge_pipe, X_test, y_test)

    print("[5/6] Fitting Nystroem(RBF) + LinearSVR (Enhanced) ...")
    t0 = time.time()
    nystroem_pipe.fit(X_train, y_train)
    print(f"       fit done in {time.time()-t0:.1f}s")
    nystroem_metrics, nystroem_pred_test = eval_model("Nystroem+SVR", nystroem_pipe, X_test, y_test)

    print("[6/6] Saving artifacts ...")
    joblib.dump(ridge_pipe,    MODELS_DIR / "ridge_enhanced.joblib")
    joblib.dump(nystroem_pipe, MODELS_DIR / "nystroem_svr_enhanced.joblib")

    with open(MODELS_DIR / "feature_columns.json", "w") as f:
        json.dump({
            "feature_cols": FEATURE_COLS,
            "base_cols": BASE_COLS,
            "enhanced_extra": ENHANCED_EXTRA,
            "selected_routes": SELECTED_ROUTES,
        }, f, indent=2)

    with open(MODELS_DIR / "historical_aggregates.json", "w") as f:
        json.dump(aggregates, f)

    with open(MODELS_DIR / "model_metrics.json", "w") as f:
        json.dump({
            "ridge":    ridge_metrics,
            "nystroem": nystroem_metrics,
            "train_size": int(len(X_train)),
            "val_size":   int(len(X_val)),
            "test_size":  int(len(X_test)),
            "train_start": str(bus_enh["recorded_at"].iloc[0]),
            "train_end":   str(bus_enh["recorded_at"].iloc[train_end - 1]),
            "test_start":  str(bus_enh["recorded_at"].iloc[val_end]),
            "test_end":    str(bus_enh["recorded_at"].iloc[-1]),
        }, f, indent=2)

    # --- Stop metadata lookup (for the selector) ---
    # IMPORTANT: also include the per-stop static road features exactly as
    # they appear in the training data, so inference uses the same values and
    # does not drift out of the scaler's training distribution.
    stops_meta = (
        bus_enh[["route_short_name", "direction_id", "stop_id", "stop_name",
                 "stop_sequence", "stop_lat", "stop_lon",
                 "active_incidents", "active_construction",
                 "nearest_event_distance_km"]]
        .drop_duplicates(subset=["route_short_name", "direction_id", "stop_id"])
        .sort_values(["route_short_name", "direction_id", "stop_sequence"])
        .reset_index(drop=True)
    )
    stops_meta["stop_id"] = stops_meta["stop_id"].astype("Int64")
    stops_meta.to_parquet(DATA_OUT / "stops_meta.parquet", index=False)
    print(f"       stops_meta rows = {len(stops_meta):,}")

    # --- Sampled test predictions for error analysis charts ---
    # Keep <=50k rows to keep the dashboard light.
    rng = np.random.RandomState(42)
    take = min(50_000, len(X_test))
    idx = rng.choice(len(X_test), size=take, replace=False)
    test_out = bus_enh.iloc[val_end:].iloc[idx][
        ["recorded_at", "route_short_name", "direction_id", "stop_id",
         "stop_name", "hour", "day_of_week", "delay_seconds"]
    ].reset_index(drop=True).copy()
    test_out["ridge_pred"]    = ridge_pred_test[idx]
    test_out["nystroem_pred"] = nystroem_pred_test[idx]
    test_out["recorded_at"]   = test_out["recorded_at"].astype(str)
    test_out.to_parquet(DATA_OUT / "test_predictions.parquet", index=False)
    print(f"       test_predictions rows = {len(test_out):,}")

    # --- Historical delay profile per (stop, hour) for the "context" chart ---
    hist = (
        bus_enh.iloc[:train_end]
        .groupby(["route_short_name", "direction_id", "stop_id", "hour"])
        ["delay_seconds"]
        .agg(mean="mean", median="median", p10=lambda s: s.quantile(0.1),
             p90=lambda s: s.quantile(0.9), count="count")
        .reset_index()
    )
    hist.to_parquet(DATA_OUT / "hourly_profile.parquet", index=False)
    print(f"       hourly_profile rows = {len(hist):,}")

    print("\nDone. Artifacts in", MODELS_DIR)
    print("                   ", DATA_OUT)


if __name__ == "__main__":
    main()
