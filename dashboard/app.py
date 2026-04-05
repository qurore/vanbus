"""
Bus Delay Prediction Dashboard

CSIS 4260 - Final Project
Interactive visualization for the Nystroem(RBF)+LinearSVR (Enhanced features)
bus delay prediction model, with a Ridge (Enhanced) baseline for comparison.

Run:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

# ---------------------------------------------------------------------------
# Page configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Bus Delay Prediction Dashboard",
    page_icon="BUS",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Light theme + custom metric card styling (mirrors the Assignment 1 dashboard)
st.markdown(
    """
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        text-align: center;
        color: #555555;
        margin-bottom: 1.5rem;
    }
    [data-testid="stMetric"] {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
        min-height: 130px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    [data-testid="stMetricLabel"] { color: #333333; }
    [data-testid="stMetricValue"] { color: #1f1f1f; }
    [data-testid="stHorizontalBlock"] > div { flex: 1; min-width: 0; }
    .stApp { background-color: #ffffff; }
    .stMarkdown, .stText, h1, h2, h3, p { color: #333333; }
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
# Paths
# ---------------------------------------------------------------------------
ROOT      = Path(__file__).resolve().parent
MODELS_DIR = ROOT / "models"
DATA_DIR   = ROOT / "data"

# ---------------------------------------------------------------------------
# Constants (feature columns — must match train_model.py)
# ---------------------------------------------------------------------------
BASE_NUMERIC = [
    "hour", "day_of_week", "is_weekend", "is_rush_hour",
    "direction_id", "stop_sequence", "stop_lat", "stop_lon",
    "temperature_c", "humidity_percent", "wind_speed_kmh", "precipitation_mm",
    "active_incidents", "active_construction", "nearest_event_distance_km",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "route_hour_mean_delay", "stop_mean_delay", "route_dir_mean_delay",
]

# Default weather values for prediction when the user doesn't override
WEATHER_DEFAULTS = {
    "temperature_c": 8.0,
    "humidity_percent": 80.0,
    "wind_speed_kmh": 10.0,
    "precipitation_mm": 0.2,
}

DOW_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


# ---------------------------------------------------------------------------
# Cached loaders
# ---------------------------------------------------------------------------
@st.cache_resource
def load_artifacts() -> dict:
    """Load both models, feature metadata, historical aggregates, metrics."""
    missing = [p for p in [
        MODELS_DIR / "nystroem_svr_enhanced.joblib",
        MODELS_DIR / "ridge_enhanced.joblib",
        MODELS_DIR / "feature_columns.json",
        MODELS_DIR / "historical_aggregates.json",
        MODELS_DIR / "model_metrics.json",
    ] if not p.exists()]
    if missing:
        st.error(
            "Model artifacts not found. Run `python dashboard/train_model.py` first.\n\n"
            f"Missing: {[p.name for p in missing]}"
        )
        st.stop()

    nystroem = joblib.load(MODELS_DIR / "nystroem_svr_enhanced.joblib")
    ridge    = joblib.load(MODELS_DIR / "ridge_enhanced.joblib")
    with open(MODELS_DIR / "feature_columns.json") as f:
        feature_meta = json.load(f)
    with open(MODELS_DIR / "historical_aggregates.json") as f:
        agg = json.load(f)
    with open(MODELS_DIR / "model_metrics.json") as f:
        metrics = json.load(f)

    return {
        "nystroem": nystroem,
        "ridge": ridge,
        "feature_meta": feature_meta,
        "aggregates": agg,
        "metrics": metrics,
    }


@st.cache_data
def load_stops_meta() -> pd.DataFrame:
    path = DATA_DIR / "stops_meta.parquet"
    if not path.exists():
        st.error("stops_meta.parquet missing — run dashboard/train_model.py first.")
        st.stop()
    return pd.read_parquet(path)


@st.cache_data
def load_test_predictions() -> pd.DataFrame:
    path = DATA_DIR / "test_predictions.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["recorded_at"] = pd.to_datetime(df["recorded_at"])
    return df


@st.cache_data
def load_hourly_profile() -> pd.DataFrame:
    path = DATA_DIR / "hourly_profile.parquet"
    if not path.exists():
        return pd.DataFrame()
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
        # Static per-stop road features come from stops_meta (computed
        # in training). Using a sentinel like 50 km would put us far
        # outside the scaler's training distribution and collapse the
        # Nystroem RBF kernel to zero.
        "active_incidents":          int(stop_row.get("active_incidents", 0) or 0),
        "active_construction":       int(stop_row.get("active_construction", 0) or 0),
        "nearest_event_distance_km": float(stop_row.get("nearest_event_distance_km", 5.0) or 5.0),
        # Cyclical time
        "hour_sin": float(np.sin(2 * np.pi * hour / 24)),
        "hour_cos": float(np.cos(2 * np.pi * hour / 24)),
        "dow_sin":  float(np.sin(2 * np.pi * day_of_week / 7)),
        "dow_cos":  float(np.cos(2 * np.pi * day_of_week / 7)),
        # Historical aggregates (lookup, fall back to global mean)
        "route_hour_mean_delay":
            float(aggregates["route_hour_mean"].get(f"{route}|{hour}", global_mean)),
        "stop_mean_delay":
            float(aggregates["stop_mean"].get(str(int(stop_row["stop_id"])), global_mean)),
        "route_dir_mean_delay":
            float(aggregates["route_dir_mean"].get(f"{route}|{int(direction)}", global_mean)),
    }
    return pd.DataFrame([row])


def predict_across_hours(
    *,
    nystroem_model,
    ridge_model,
    route: str,
    direction: int,
    stop_row: pd.Series,
    day_of_week: int,
    weather: dict,
    aggregates: dict,
) -> pd.DataFrame:
    """Predict delay for all 24 hours at a given stop / day, both models."""
    rows = [
        build_feature_row(
            route=route, direction=direction, stop_row=stop_row,
            hour=h, day_of_week=day_of_week, weather=weather,
            aggregates=aggregates,
        ).iloc[0]
        for h in range(24)
    ]
    frame = pd.DataFrame(rows)
    frame["ridge_pred"]    = ridge_model.predict(frame)
    frame["nystroem_pred"] = nystroem_model.predict(frame)
    return frame


# ---------------------------------------------------------------------------
# Plotly helpers
# ---------------------------------------------------------------------------
PLOTLY_LAYOUT = dict(
    template="plotly_white",
    paper_bgcolor="white",
    plot_bgcolor="white",
    font=dict(color="#333333"),
    title_font=dict(color="#333333"),
)


def style_axes(fig):
    fig.update_xaxes(gridcolor="#e0e0e0",
                     tickfont=dict(color="#333333"),
                     title_font=dict(color="#333333"))
    fig.update_yaxes(gridcolor="#e0e0e0",
                     tickfont=dict(color="#333333"),
                     title_font=dict(color="#333333"))
    return fig


def hourly_prediction_chart(pred_df: pd.DataFrame, historical: pd.DataFrame,
                             stop_name: str, day_name: str) -> go.Figure:
    fig = go.Figure()

    # Historical envelope (P10 – P90)
    if not historical.empty:
        fig.add_trace(go.Scatter(
            x=list(historical["hour"]) + list(historical["hour"])[::-1],
            y=list(historical["p90"])  + list(historical["p10"])[::-1],
            fill="toself",
            fillcolor="rgba(31, 119, 180, 0.12)",
            line=dict(color="rgba(31, 119, 180, 0)"),
            name="Historical P10–P90",
            hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=historical["hour"], y=historical["mean"],
            name="Historical mean",
            line=dict(color="#1f77b4", width=2),
        ))

    fig.add_trace(go.Scatter(
        x=pred_df["hour"], y=pred_df["ridge_pred"],
        name="Ridge (Enhanced)",
        line=dict(color="#ff7f0e", width=2, dash="dash"),
    ))
    fig.add_trace(go.Scatter(
        x=pred_df["hour"], y=pred_df["nystroem_pred"],
        name="Nystroem + LinearSVR",
        line=dict(color="#2ca02c", width=2.5),
    ))

    fig.update_layout(
        height=420,
        title=f"Predicted delay across the day — {stop_name} ({day_name})",
        xaxis_title="Hour of day",
        yaxis_title="Delay (seconds)",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="center", x=0.5, font=dict(color="#333333")),
        **PLOTLY_LAYOUT,
    )
    fig.update_xaxes(dtick=2)
    return style_axes(fig)


def error_histogram(df: pd.DataFrame, pred_col: str, title: str, color: str) -> go.Figure:
    err = df["delay_seconds"] - df[pred_col]
    fig = px.histogram(err, nbins=60, title=title)
    fig.update_traces(marker_color=color)
    fig.update_layout(
        showlegend=False, height=320,
        xaxis_title="Error (seconds)", yaxis_title="Count",
        **PLOTLY_LAYOUT,
    )
    return style_axes(fig)


def scatter_actual_vs_pred(df: pd.DataFrame, pred_col: str,
                            title: str, color: str) -> go.Figure:
    sample = df.sample(n=min(5000, len(df)), random_state=42)
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=sample["delay_seconds"], y=sample[pred_col],
        mode="markers",
        marker=dict(size=4, opacity=0.45, color=color),
        name="Predictions",
    ))
    lo = float(min(sample["delay_seconds"].min(), sample[pred_col].min()))
    hi = float(max(sample["delay_seconds"].max(), sample[pred_col].max()))
    fig.add_trace(go.Scatter(
        x=[lo, hi], y=[lo, hi],
        mode="lines",
        line=dict(color="red", dash="dash"),
        name="Perfect prediction",
    ))
    fig.update_layout(
        title=title, height=380,
        xaxis_title="Actual delay (seconds)",
        yaxis_title="Predicted delay (seconds)",
        legend=dict(font=dict(color="#333333")),
        **PLOTLY_LAYOUT,
    )
    return style_axes(fig)


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------
def main():
    st.markdown('<h1 class="main-header">Bus Delay Prediction Dashboard</h1>',
                unsafe_allow_html=True)
    st.markdown(
        '<p class="sub-header">CSIS 4260 Final Project &nbsp;|&nbsp; '
        'Nystroem(RBF) + LinearSVR with Enhanced features &nbsp;·&nbsp; '
        'Ridge Enhanced baseline</p>',
        unsafe_allow_html=True,
    )

    with st.spinner("Loading models and data ..."):
        art = load_artifacts()
        stops_meta = load_stops_meta()
        test_preds = load_test_predictions()
        hourly     = load_hourly_profile()

    # ---- Controls row ----
    routes = sorted(stops_meta["route_short_name"].unique())
    col_r, col_d, col_s, col_dow = st.columns([1, 1, 3, 1])

    with col_r:
        route = st.selectbox("Route", routes,
                              index=routes.index("130") if "130" in routes else 0)
    route_stops = stops_meta[stops_meta["route_short_name"] == route]

    with col_d:
        directions = sorted(route_stops["direction_id"].dropna().unique().astype(int))
        direction = st.selectbox(
            "Direction", directions,
            format_func=lambda x: f"{int(x)} ({'outbound' if int(x)==0 else 'inbound'})",
        )
    stops_in_dir = route_stops[route_stops["direction_id"] == direction] \
        .sort_values("stop_sequence").reset_index(drop=True)

    with col_s:
        if len(stops_in_dir) == 0:
            st.warning("No stops for this route/direction.")
            st.stop()
        stop_labels = [
            f"#{int(r.stop_sequence):>3d} — {r.stop_name} (id {int(r.stop_id)})"
            for r in stops_in_dir.itertuples()
        ]
        stop_idx = st.selectbox(
            "Stop", options=list(range(len(stops_in_dir))),
            format_func=lambda i: stop_labels[i],
        )
    stop_row = stops_in_dir.iloc[stop_idx]

    with col_dow:
        dow_idx = st.selectbox("Day of week", list(range(7)),
                                format_func=lambda i: DOW_NAMES[i])

    # ---- Optional weather overrides ----
    with st.expander("Weather inputs (optional)"):
        wc1, wc2, wc3, wc4 = st.columns(4)
        with wc1:
            temperature_c = st.number_input("Temperature (°C)",
                value=WEATHER_DEFAULTS["temperature_c"], step=0.5)
        with wc2:
            humidity = st.number_input("Humidity (%)",
                value=WEATHER_DEFAULTS["humidity_percent"], step=1.0, min_value=0.0, max_value=100.0)
        with wc3:
            wind = st.number_input("Wind (km/h)",
                value=WEATHER_DEFAULTS["wind_speed_kmh"], step=1.0, min_value=0.0)
        with wc4:
            precip = st.number_input("Precipitation (mm)",
                value=WEATHER_DEFAULTS["precipitation_mm"], step=0.1, min_value=0.0)
    weather = {
        "temperature_c":    temperature_c,
        "humidity_percent": humidity,
        "wind_speed_kmh":   wind,
        "precipitation_mm": precip,
    }

    # ---- Model performance section ----
    st.markdown("---")
    st.subheader("Model Performance (held-out test set)")
    perf_cols = st.columns(2)
    rmetrics = art["metrics"]["ridge"]
    nmetrics = art["metrics"]["nystroem"]

    with perf_cols[0]:
        st.markdown("**Ridge (Enhanced) — baseline**")
        r_c1, r_c2, r_c3 = st.columns(3)
        r_c1.metric("MAE",  f"{rmetrics['mae']:.2f}s")
        r_c2.metric("RMSE", f"{rmetrics['rmse']:.2f}s")
        r_c3.metric("R²",   f"{rmetrics['r2']:.4f}")

    with perf_cols[1]:
        st.markdown("**Nystroem(RBF) + LinearSVR (Enhanced) — champion**")
        n_c1, n_c2, n_c3 = st.columns(3)
        delta_mae = nmetrics["mae"] - rmetrics["mae"]
        n_c1.metric("MAE",  f"{nmetrics['mae']:.2f}s",
                     delta=f"{delta_mae:+.2f}s vs Ridge",
                     delta_color="inverse")
        n_c2.metric("RMSE", f"{nmetrics['rmse']:.2f}s")
        n_c3.metric("R²",   f"{nmetrics['r2']:.4f}")

    st.caption(
        f"Trained on {art['metrics']['train_size']:,} rows · "
        f"Test: {art['metrics']['test_size']:,} rows · "
        f"Period: {art['metrics']['train_start'][:10]} → {art['metrics']['test_end'][:10]}"
    )

    # ---- Prediction cards for the selected row ----
    st.markdown("---")
    st.subheader(f"Prediction — Route {route}, stop {stop_row['stop_name']}")

    now_row = build_feature_row(
        route=route, direction=int(direction), stop_row=stop_row,
        hour=12, day_of_week=int(dow_idx), weather=weather,
        aggregates=art["aggregates"],
    )

    # We show the user a full 24h prediction plus a "current hour" slider for the cards
    hour_slider = st.slider("Hour of day for summary cards", 0, 23, 8, 1,
                             help="Affects the four metric cards below; "
                                  "the chart further down always shows the full 24 hours.")
    card_row = build_feature_row(
        route=route, direction=int(direction), stop_row=stop_row,
        hour=hour_slider, day_of_week=int(dow_idx), weather=weather,
        aggregates=art["aggregates"],
    )
    ridge_val    = float(art["ridge"].predict(card_row)[0])
    nystroem_val = float(art["nystroem"].predict(card_row)[0])
    historical_mean = float(card_row["stop_mean_delay"].iloc[0])
    route_hour_mean = float(card_row["route_hour_mean_delay"].iloc[0])

    card_cols = st.columns(4)
    card_cols[0].metric("Stop historical avg", f"{historical_mean:.0f}s",
                         help="Mean delay at this stop in training data (all hours)")
    card_cols[1].metric(f"Route {route} @ {hour_slider:02d}:00",
                         f"{route_hour_mean:.0f}s",
                         help="Mean delay on this route at this hour (training data)")
    card_cols[2].metric("Ridge prediction",
                         f"{ridge_val:.0f}s",
                         delta=f"{ridge_val - historical_mean:+.0f}s vs stop avg")
    card_cols[3].metric("Nystroem+SVR prediction",
                         f"{nystroem_val:.0f}s",
                         delta=f"{nystroem_val - historical_mean:+.0f}s vs stop avg")

    # ---- Hourly prediction chart ----
    pred_df = predict_across_hours(
        nystroem_model=art["nystroem"],
        ridge_model=art["ridge"],
        route=route, direction=int(direction), stop_row=stop_row,
        day_of_week=int(dow_idx), weather=weather,
        aggregates=art["aggregates"],
    )

    if not hourly.empty:
        hist_here = hourly[
            (hourly["route_short_name"] == route) &
            (hourly["direction_id"] == int(direction)) &
            (hourly["stop_id"] == int(stop_row["stop_id"]))
        ].sort_values("hour")
    else:
        hist_here = pd.DataFrame()

    chart = hourly_prediction_chart(
        pred_df, hist_here,
        stop_name=f"{stop_row['stop_name']} (stop #{int(stop_row['stop_sequence'])})",
        day_name=DOW_NAMES[int(dow_idx)],
    )
    st.plotly_chart(chart, use_container_width=True)

    # ---- Prediction accuracy (test set) ----
    if not test_preds.empty:
        st.markdown("---")
        st.subheader("Prediction Accuracy Analysis (test split)")

        err_cols = st.columns(2)
        with err_cols[0]:
            st.plotly_chart(
                error_histogram(test_preds, "ridge_pred",
                                "Ridge — error distribution", "#ff7f0e"),
                use_container_width=True,
            )
        with err_cols[1]:
            st.plotly_chart(
                error_histogram(test_preds, "nystroem_pred",
                                "Nystroem+LinearSVR — error distribution", "#2ca02c"),
                use_container_width=True,
            )

        st.subheader("Actual vs Predicted (5k sample of test set)")
        sc_cols = st.columns(2)
        with sc_cols[0]:
            st.plotly_chart(
                scatter_actual_vs_pred(test_preds, "ridge_pred",
                                        "Ridge (Enhanced)", "#ff7f0e"),
                use_container_width=True,
            )
        with sc_cols[1]:
            st.plotly_chart(
                scatter_actual_vs_pred(test_preds, "nystroem_pred",
                                        "Nystroem + LinearSVR (Enhanced)", "#2ca02c"),
                use_container_width=True,
            )

    st.markdown("---")
    st.caption(
        "Model: `Nystroem(kernel='rbf', n_components=300)` + "
        "`LinearSVR(C=1.0, epsilon=0.0)` trained on Enhanced features "
        "(base + cyclical time + per-stop / per-route historical aggregates). "
        "Baseline: Ridge(alpha=1.0) on the same features. "
        "See `dashboard/train_model.py` for the full training pipeline."
    )


if __name__ == "__main__":
    main()
