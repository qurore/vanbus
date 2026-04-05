"""
Bus Delay Prediction Dashboard

Interactive prediction tool for the Nystroem(RBF)+LinearSVR (Enhanced features)
bus delay model. Select a stop, date, and time → get the expected delay.

Run:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import json
from datetime import date as date_cls, datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Page configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Bus Delay Prediction",
    page_icon="BUS",
    layout="centered",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        text-align: center;
        color: #555555;
        margin-bottom: 1.5rem;
    }
    .result-card {
        background-color: #f0f7ff;
        border: 1px solid #1f77b4;
        border-radius: 12px;
        padding: 28px 20px;
        text-align: center;
        margin-top: 12px;
    }
    .result-label {
        color: #555555;
        font-size: 1rem;
        margin-bottom: 6px;
    }
    .result-value {
        color: #1f77b4;
        font-size: 2.6rem;
        font-weight: 700;
        line-height: 1.2;
    }
    .result-sub {
        color: #666666;
        font-size: 0.9rem;
        margin-top: 6px;
    }
    .stApp { background-color: #ffffff; }
    .stMarkdown, .stText, h1, h2, h3, h4, h5, h6, p { color: #333333 !important; }
    [data-testid="stHeading"] *,
    [data-testid="stHeadingWithActionElements"] *,
    [data-testid="stMarkdownContainer"] h1,
    [data-testid="stMarkdownContainer"] h2,
    [data-testid="stMarkdownContainer"] h3,
    [data-testid="stMarkdownContainer"] h4 { color: #333333 !important; }
    .stTextInput > div > div > input,
    .stSelectbox > div > div,
    .stNumberInput > div > div > input,
    .stDateInput > div > div > input {
        background-color: #ffffff;
        color: #333333;
        border: 1px solid #e0e0e0;
    }
    div[data-baseweb="select"] > div { background-color: #ffffff; border-color: #e0e0e0; }
    div[data-baseweb="input"]  > div { background-color: #ffffff; border-color: #e0e0e0; }
    input { background-color: #ffffff !important; color: #333333 !important; }
</style>
""",
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
ROOT       = Path(__file__).resolve().parent
MODELS_DIR = ROOT / "models"
DATA_DIR   = ROOT / "data"

WEATHER_DEFAULTS = {
    "temperature_c":    8.0,
    "humidity_percent": 80.0,
    "wind_speed_kmh":   10.0,
    "precipitation_mm": 0.2,
}


# ---------------------------------------------------------------------------
# Cached loaders
# ---------------------------------------------------------------------------
@st.cache_resource
def load_artifacts() -> dict:
    """Load the champion model + historical aggregates."""
    required = [
        MODELS_DIR / "nystroem_svr_enhanced.joblib",
        MODELS_DIR / "feature_columns.json",
        MODELS_DIR / "historical_aggregates.json",
    ]
    missing = [p for p in required if not p.exists()]
    if missing:
        st.error(
            "Model artifacts not found. Run `python dashboard/train_model.py` first.\n\n"
            f"Missing: {[p.name for p in missing]}"
        )
        st.stop()

    nystroem = joblib.load(MODELS_DIR / "nystroem_svr_enhanced.joblib")
    with open(MODELS_DIR / "feature_columns.json") as f:
        feature_meta = json.load(f)
    with open(MODELS_DIR / "historical_aggregates.json") as f:
        agg = json.load(f)

    return {
        "nystroem": nystroem,
        "feature_meta": feature_meta,
        "aggregates": agg,
    }


@st.cache_data
def load_stops_meta() -> pd.DataFrame:
    path = DATA_DIR / "stops_meta.parquet"
    if not path.exists():
        st.error("stops_meta.parquet missing — run dashboard/train_model.py first.")
        st.stop()
    return pd.read_parquet(path)


# ---------------------------------------------------------------------------
# Inference helpers
# ---------------------------------------------------------------------------
def build_feature_row(
    *,
    route: str,
    direction: int,
    stop_row: pd.Series,
    hour: int,
    day_of_week: int,
    weather: dict,
    aggregates: dict,
) -> pd.DataFrame:
    """Construct a single-row DataFrame with all Enhanced feature columns."""
    global_mean = aggregates["global_mean"]
    row = {
        "hour":         int(hour),
        "day_of_week":  int(day_of_week),
        "is_weekend":   int(day_of_week >= 5),
        "is_rush_hour": int(hour in {7, 8, 9, 16, 17, 18}),
        "route_short_name": route,
        "direction_id":     int(direction),
        "stop_sequence":    float(stop_row["stop_sequence"]),
        "stop_lat":         float(stop_row["stop_lat"]),
        "stop_lon":         float(stop_row["stop_lon"]),
        "temperature_c":    float(weather["temperature_c"]),
        "humidity_percent": float(weather["humidity_percent"]),
        "wind_speed_kmh":   float(weather["wind_speed_kmh"]),
        "precipitation_mm": float(weather["precipitation_mm"]),
        "active_incidents":          int(stop_row.get("active_incidents", 0) or 0),
        "active_construction":       int(stop_row.get("active_construction", 0) or 0),
        "nearest_event_distance_km": float(stop_row.get("nearest_event_distance_km", 5.0) or 5.0),
        "hour_sin": float(np.sin(2 * np.pi * hour / 24)),
        "hour_cos": float(np.cos(2 * np.pi * hour / 24)),
        "dow_sin":  float(np.sin(2 * np.pi * day_of_week / 7)),
        "dow_cos":  float(np.cos(2 * np.pi * day_of_week / 7)),
        "route_hour_mean_delay":
            float(aggregates["route_hour_mean"].get(f"{route}|{hour}", global_mean)),
        "stop_mean_delay":
            float(aggregates["stop_mean"].get(str(int(stop_row["stop_id"])), global_mean)),
        "route_dir_mean_delay":
            float(aggregates["route_dir_mean"].get(f"{route}|{int(direction)}", global_mean)),
    }
    return pd.DataFrame([row])


def format_delay(seconds: float) -> tuple[str, str, str]:
    """Return (label, big_value, sub_text) for the result card."""
    minutes = seconds / 60.0
    if seconds >= 30:
        return (
            "Expected delay",
            f"{minutes:+.1f} min late",
            f"(about {seconds:.0f} seconds behind schedule)",
        )
    if seconds <= -30:
        return (
            "Expected to be early",
            f"{abs(minutes):.1f} min early",
            f"(about {abs(seconds):.0f} seconds ahead of schedule)",
        )
    return (
        "Expected to be on time",
        "~0 min",
        f"(predicted deviation: {seconds:+.0f} seconds)",
    )


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------
def main():
    st.markdown('<h1 class="main-header">Bus Delay Prediction</h1>',
                unsafe_allow_html=True)
    st.markdown(
        '<p class="sub-header">Select a stop, date, and time to see the predicted delay.</p>',
        unsafe_allow_html=True,
    )

    with st.spinner("Loading model ..."):
        art = load_artifacts()
        stops_meta = load_stops_meta()

    # ---- Route / Direction / Stop ----
    routes = sorted(stops_meta["route_short_name"].unique())
    col_r, col_d = st.columns([1, 1])

    with col_r:
        route = st.selectbox(
            "Route", routes,
            index=routes.index("130") if "130" in routes else 0,
        )
    route_stops = stops_meta[stops_meta["route_short_name"] == route]

    # Look up the terminus (last stop by stop_sequence) for each direction so
    # users see a meaningful destination name instead of "0 / 1".
    terminus_by_dir: dict[int, str] = {}
    for d, grp in route_stops.dropna(subset=["direction_id"]).groupby("direction_id"):
        last_stop = grp.sort_values("stop_sequence").iloc[-1]
        terminus_by_dir[int(d)] = str(last_stop["stop_name"])

    with col_d:
        directions = sorted(route_stops["direction_id"].dropna().unique().astype(int))
        direction = st.selectbox(
            "Direction", directions,
            format_func=lambda x: f"To {terminus_by_dir.get(int(x), '?')}",
        )

    stops_in_dir = (
        route_stops[route_stops["direction_id"] == direction]
        .sort_values("stop_sequence")
        .reset_index(drop=True)
    )
    if len(stops_in_dir) == 0:
        st.warning("No stops for this route/direction.")
        st.stop()

    stop_labels = [
        f"#{int(r.stop_sequence):>3d} — {r.stop_name}"
        for r in stops_in_dir.itertuples()
    ]
    stop_idx = st.selectbox(
        "Stop", options=list(range(len(stops_in_dir))),
        format_func=lambda i: stop_labels[i],
    )
    stop_row = stops_in_dir.iloc[stop_idx]

    # ---- Date & time ----
    col_date, col_time = st.columns([1, 1])
    with col_date:
        selected_date = st.date_input("Date", value=date_cls.today())
    with col_time:
        hour = st.slider("Hour of day", 0, 23, 8, 1)

    day_of_week = selected_date.weekday()  # Mon=0 … Sun=6

    # ---- Optional weather ----
    with st.expander("Weather (optional)"):
        wc1, wc2, wc3, wc4 = st.columns(4)
        with wc1:
            temperature_c = st.number_input(
                "Temperature (°C)",
                value=WEATHER_DEFAULTS["temperature_c"], step=0.5,
            )
        with wc2:
            humidity = st.number_input(
                "Humidity (%)",
                value=WEATHER_DEFAULTS["humidity_percent"],
                step=1.0, min_value=0.0, max_value=100.0,
            )
        with wc3:
            wind = st.number_input(
                "Wind (km/h)",
                value=WEATHER_DEFAULTS["wind_speed_kmh"],
                step=1.0, min_value=0.0,
            )
        with wc4:
            precip = st.number_input(
                "Precipitation (mm)",
                value=WEATHER_DEFAULTS["precipitation_mm"],
                step=0.1, min_value=0.0,
            )
    weather = {
        "temperature_c":    temperature_c,
        "humidity_percent": humidity,
        "wind_speed_kmh":   wind,
        "precipitation_mm": precip,
    }

    # ---- Prediction ----
    feature_row = build_feature_row(
        route=route,
        direction=int(direction),
        stop_row=stop_row,
        hour=int(hour),
        day_of_week=int(day_of_week),
        weather=weather,
        aggregates=art["aggregates"],
    )
    pred_seconds = float(art["nystroem"].predict(feature_row)[0])
    label, big_value, sub_text = format_delay(pred_seconds)

    when = datetime.combine(selected_date, datetime.min.time()).replace(hour=int(hour))
    context_line = (
        f"Route {route} · {stop_row['stop_name']} · "
        f"{when.strftime('%a %Y-%m-%d %H:%M')}"
    )

    st.markdown(
        f"""
<div class="result-card">
    <div class="result-label">{label}</div>
    <div class="result-value">{big_value}</div>
    <div class="result-sub">{sub_text}</div>
    <div class="result-sub" style="margin-top:10px;">{context_line}</div>
</div>
""",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
