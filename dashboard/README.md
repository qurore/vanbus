# Bus Delay Prediction Dashboard

Interactive Streamlit dashboard for the CSIS 4260 Final Project bus-delay
prediction model.

**Champion model:** Nystroem(RBF) kernel approximation + LinearSVR, trained on
Enhanced features (base + cyclical time + per-stop / per-route historical
delay aggregates).
**Baseline for comparison:** Ridge(alpha=1.0) on the same Enhanced features.

## Directory layout

```
dashboard/
├── app.py              # Streamlit application
├── train_model.py      # One-shot script that fits and persists both models
├── requirements.txt
├── models/
│   ├── nystroem_svr_enhanced.joblib   # scikit-learn Pipeline (preprocessor + scaler + Nystroem + LinearSVR)
│   ├── ridge_enhanced.joblib          # scikit-learn Pipeline (preprocessor + Ridge)
│   ├── feature_columns.json           # ordered feature list + routes
│   ├── historical_aggregates.json     # route-hour / stop / route-dir mean delays (lookup)
│   └── model_metrics.json             # MAE / RMSE / R² on the held-out test split
└── data/
    ├── stops_meta.parquet             # route × direction × stop metadata for the selectors
    ├── hourly_profile.parquet         # per (route, direction, stop, hour) historical P10/mean/P90
    └── test_predictions.parquet       # 50k-row sample of the test split with both models' predictions
```

## First-time setup

1. Create a virtualenv and install dependencies:

   ```bash
   cd /Users/ryshiro/vanbus
   python -m venv .venv
   source .venv/bin/activate
   pip install -r dashboard/requirements.txt
   ```

2. Train and persist the models (runs once, ≈ 3-5 min on the full
   ~1.7M row dataset; writes everything into `dashboard/models/` and
   `dashboard/data/`):

   ```bash
   python dashboard/train_model.py
   ```

   The script reads CSVs from `db_export_combined/` and prints the MAE /
   RMSE / R² of both the Ridge baseline and the Nystroem+LinearSVR champion
   when it finishes.

## Running the dashboard

```bash
streamlit run dashboard/app.py
```

Streamlit opens a browser tab at `http://localhost:8501`.

## What the dashboard shows

Top to bottom:

1. **Controls** — Route, direction, stop, day of week selectors
2. **Weather inputs** (expander, optional) — temperature / humidity / wind / precipitation
3. **Model Performance** — MAE, RMSE, R² of both models on the held-out test split
4. **Prediction cards** — stop historical average, route-hour average, Ridge
   prediction, Nystroem+SVR prediction for a user-picked hour
5. **Hourly prediction chart** — predicted delay across all 24 hours at the
   selected stop, overlaid with the historical P10–P90 envelope and mean
6. **Prediction accuracy** — per-model error histogram on the test sample
7. **Actual vs predicted scatter** — 5k-row sample of test predictions

## Feature set used by both models

Base (16):
- Time: `hour`, `day_of_week`, `is_weekend`, `is_rush_hour`
- Route / stop: `route_short_name`, `direction_id`, `stop_sequence`,
  `stop_lat`, `stop_lon`
- Weather: `temperature_c`, `humidity_percent`, `wind_speed_kmh`,
  `precipitation_mm`
- Road: `active_incidents`, `active_construction`, `nearest_event_distance_km`

Enhanced extras (+7):
- Cyclical time: `hour_sin`, `hour_cos`, `dow_sin`, `dow_cos`
- Historical aggregates (computed from training split only):
  `route_hour_mean_delay`, `stop_mean_delay`, `route_dir_mean_delay`

Feature engineering — see `train_model.py::build_base` and `build_enhanced`.

## Expected metrics

| Model                       | MAE    | RMSE   | R²     |
|-----------------------------|--------|--------|--------|
| Ridge (Enhanced) — baseline | ~108s  | ~169s  | ~0.11  |
| Nystroem+LinearSVR (Enhanced) — champion | **~102s** | ~171s | ~0.09 |

The Nystroem+LinearSVR champion wins on MAE (~6s better) because its
ε-insensitive loss is robust to the fat tail of bus delays; Ridge still wins
marginally on RMSE / R² because squared-error loss rewards the tail. See the
training notebook for the full ablation (Base → Enhancement 1 → Enhancement 2
→ feature selection → Yeo-Johnson target transform).
