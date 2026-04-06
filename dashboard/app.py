"""
Bus Delay Prediction Dashboard

Interactive prediction tool for the Nystroem(RBF)+LinearSVR (Enhanced features)
bus delay model. Pick a stop and how far in the future you want the prediction —
live weather (Open-Meteo) is fetched automatically and fed into the model.
Active DriveBC road events nearby are shown for awareness (display-only; not
fed to the model because training used historical per-stop risk aggregates).

Run:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import html
import json
import math
import re
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

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
    .live-box {
        background-color: #f7f9fc;
        border: 1px solid #e0e6ef;
        border-radius: 8px;
        padding: 10px 14px;
        margin: 8px 0 4px 0;
        color: #444444;
        font-size: 0.92rem;
    }
    .event-box {
        background-color: #fff8e6;
        border: 1px solid #f0d080;
        border-radius: 8px;
        padding: 10px 14px;
        margin-top: 10px;
        color: #5a4a10;
        font-size: 0.9rem;
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

VANCOUVER_TZ    = ZoneInfo("America/Vancouver")
OPEN_METEO_URL  = "https://api.open-meteo.com/v1/forecast"
OPEN511_URL     = "https://api.open511.gov.bc.ca/events"
HTTP_USER_AGENT = "vanbus-delay-dashboard/1.0 (+https://example.local)"


def _http_get_json(url: str, timeout: float = 8.0) -> dict | None:
    """GET a URL with a polite UA and return parsed JSON, or None on failure.

    Response body is explicitly decoded as UTF-8 so Unicode text (e.g. BC
    Indigenous place names with combining diacritics in DriveBC descriptions)
    round-trips cleanly.
    """
    req = urllib.request.Request(
        url,
        headers={"User-Agent": HTTP_USER_AGENT, "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None

# Fallback weather values (used if Open-Meteo is unreachable)
WEATHER_DEFAULTS = {
    "temperature_c":    8.0,
    "humidity_percent": 80.0,
    "wind_speed_kmh":   10.0,
    "precipitation_mm": 0.2,
}

# Training weather distribution bounds (min/max observed in training data,
# 2025-12-12 → 2026-02-27). Used to flag out-of-distribution inputs.
TRAIN_TEMP_MIN = -5.9
TRAIN_TEMP_MAX = 15.9


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
# Live API fetchers (cached so users don't hammer the APIs)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=600, show_spinner=False)  # 10-minute cache
def fetch_weather_forecast(lat: float, lon: float) -> dict | None:
    """Fetch hourly weather forecast (next 48h) from Open-Meteo.

    Returns the parsed JSON dict, or None on any failure (timeout, DNS, etc.).
    Cache key is the rounded lat/lon so nearby stops share a single call.
    """
    params = {
        "latitude":        round(float(lat), 3),
        "longitude":       round(float(lon), 3),
        "hourly":          "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
        "wind_speed_unit": "kmh",
        "forecast_days":   2,
        "timezone":        "America/Vancouver",
    }
    url = f"{OPEN_METEO_URL}?{urllib.parse.urlencode(params)}"
    return _http_get_json(url)


def pick_weather_at(raw: dict | None, target_dt: datetime) -> tuple[dict, bool]:
    """Pick hourly weather values at the hour nearest `target_dt`.

    Returns (weather_dict, is_live). If fetching failed or the target hour isn't
    in the payload, falls back to WEATHER_DEFAULTS and is_live=False.
    """
    if not raw or "hourly" not in raw:
        return dict(WEATHER_DEFAULTS), False

    hourly = raw["hourly"]
    # Open-Meteo returns naive local-time ISO strings when timezone is set,
    # e.g. "2026-04-05T16:00". target_dt is tz-aware America/Vancouver.
    target_key = target_dt.strftime("%Y-%m-%dT%H:00")
    times = hourly.get("time", [])
    try:
        idx = times.index(target_key)
    except ValueError:
        # Fall back to the closest hour we do have.
        if not times:
            return dict(WEATHER_DEFAULTS), False
        idx = 0
    try:
        return {
            "temperature_c":    float(hourly["temperature_2m"][idx]),
            "humidity_percent": float(hourly["relative_humidity_2m"][idx]),
            "wind_speed_kmh":   float(hourly["wind_speed_10m"][idx]),
            "precipitation_mm": float(hourly["precipitation"][idx]),
        }, True
    except (KeyError, IndexError, TypeError):
        return dict(WEATHER_DEFAULTS), False


@st.cache_data(ttl=300, show_spinner=False)  # 5-minute cache
def fetch_nearby_events(lat: float, lon: float, radius_km: float = 5.0) -> list[dict]:
    """Fetch currently-active DriveBC events (Open511) within `radius_km`
    of the given stop. Display-only — NOT fed into the model, because the
    training features are historical per-stop risk aggregates, not live counts.
    """
    dlat = radius_km / 111.0
    dlon = radius_km / (111.0 * max(math.cos(math.radians(float(lat))), 0.01))
    bbox = f"{lon - dlon:.4f},{lat - dlat:.4f},{lon + dlon:.4f},{lat + dlat:.4f}"
    params = {
        "bbox":   bbox,
        "status": "ACTIVE",
        "format": "json",
        "limit":  20,
    }
    url = f"{OPEN511_URL}?{urllib.parse.urlencode(params)}"
    data = _http_get_json(url)
    if not data:
        return []
    return data.get("events", []) or []


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
        # These are the STATIC per-stop risk aggregates computed at training
        # time from two months of accumulated DriveBC events. Do NOT replace
        # with live Open511 counts — magnitudes differ by 10-30x.
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
        '<p class="sub-header">Pick a stop and how far ahead you want the prediction. '
        'Live weather is fetched automatically.</p>',
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

    # ---- "Minutes from now" slider (the only time control) ----
    offset_min = st.slider(
        "Minutes from now",
        min_value=0, max_value=150, value=15, step=5,
        help="How many minutes from right now you want the delay prediction for.",
    )

    now        = datetime.now(VANCOUVER_TZ)
    target_dt  = now + timedelta(minutes=int(offset_min))
    hour       = target_dt.hour
    day_of_week = target_dt.weekday()

    # ---- Live weather (Open-Meteo) and nearby events (Open511) ----
    with st.spinner("Fetching live conditions ..."):
        raw_weather = fetch_weather_forecast(
            float(stop_row["stop_lat"]), float(stop_row["stop_lon"]),
        )
        weather, weather_is_live = pick_weather_at(raw_weather, target_dt)
        nearby_events = fetch_nearby_events(
            float(stop_row["stop_lat"]), float(stop_row["stop_lon"]),
        )

    # Live conditions banner
    if weather_is_live:
        weather_line = (
            f"Live @ {target_dt.strftime('%H:%M')} — "
            f"{weather['temperature_c']:.1f}°C · "
            f"{weather['humidity_percent']:.0f}% humidity · "
            f"{weather['wind_speed_kmh']:.0f} km/h wind · "
            f"{weather['precipitation_mm']:.1f} mm precip"
        )
    else:
        weather_line = (
            "Live weather unavailable — using default values "
            f"({weather['temperature_c']:.0f}°C, "
            f"{weather['humidity_percent']:.0f}%, "
            f"{weather['wind_speed_kmh']:.0f} km/h, "
            f"{weather['precipitation_mm']:.1f} mm)"
        )
    st.markdown(f'<div class="live-box">{weather_line}</div>',
                unsafe_allow_html=True)

    # Out-of-distribution temperature warning (model trained on winter data)
    if weather_is_live and (
        weather["temperature_c"] < TRAIN_TEMP_MIN
        or weather["temperature_c"] > TRAIN_TEMP_MAX
    ):
        st.warning(
            f"Current temperature ({weather['temperature_c']:.1f}°C) is outside "
            f"the training distribution ({TRAIN_TEMP_MIN:.1f}°C – {TRAIN_TEMP_MAX:.1f}°C). "
            "The model was trained on winter 2025-26 data; predictions may be less reliable."
        )

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

    context_line = (
        f"Route {route} · {stop_row['stop_name']} · "
        f"{target_dt.strftime('%a %Y-%m-%d %H:%M %Z')} "
        f"(+{int(offset_min)} min from now)"
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

    # ---- Nearby DriveBC events (display only, collapsible) ----
    if nearby_events:
        header = (
            f"Heads-up: {len(nearby_events)} active DriveBC event(s) within "
            "~5 km of this stop (not used by the model)"
        )
        with st.expander(header, expanded=False):
            for ev in nearby_events:
                etype    = ev.get("event_type", "EVENT")
                severity = ev.get("severity", "")
                roads    = ev.get("roads") or []
                road_nm  = roads[0].get("name", "") if roads else ""
                # Prefer description (full detail); headline is often just the
                # event type like "CONSTRUCTION".
                text = (ev.get("description") or ev.get("headline") or "").strip()
                if not text or text.upper() == etype.upper():
                    text = "(no details)"
                # DriveBC's upstream already serves '¿' where it couldn't encode
                # BC Indigenous place names (e.g. "stal¿¿¿¿w¿¿as¿¿m Bridge").
                # Collapse runs of '¿' into a single middle dot so the text is
                # readable instead of looking corrupted.
                text = re.sub(r"¿+", "·", text)
                # HTML-escape any stray <, >, & before markdown rendering.
                text_safe     = html.escape(text)
                etype_safe    = html.escape(etype)
                severity_safe = html.escape(severity)
                road_safe     = html.escape(road_nm)
                sev_tag  = f" [{severity_safe}]" if severity else ""
                road_tag = f" · {road_safe}" if road_nm else ""
                st.markdown(
                    f"- **{etype_safe}**{sev_tag}{road_tag}  \n  {text_safe}",
                    unsafe_allow_html=False,
                )


if __name__ == "__main__":
    main()
