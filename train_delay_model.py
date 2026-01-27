#!/usr/bin/env python3
"""
Bus Delay Prediction Model - LightGBM
Predicts delay_seconds based on time, route, weather, and road conditions.
"""
import os
import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import psycopg2
from dotenv import load_dotenv
import joblib
from datetime import datetime

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')

# Selected routes (13 routes × 2 directions = 26 route-directions)
SELECTED_ROUTES = [
    # New Westminster / Burnaby (7)
    '130', '123', '144', '106', '160', '110', '116',
    # Vancouver Downtown (2)
    '006', '019',
    # Vancouver South (2)
    '049', '100',
    # North Vancouver (2)
    '240', '255',
]


def load_bus_delays() -> pd.DataFrame:
    """Load bus delay data for selected routes."""
    print("Loading bus delays...")
    conn = psycopg2.connect(DATABASE_URL)

    # Get route_ids for selected route_short_names
    routes_placeholder = ','.join([f"'{r}'" for r in SELECTED_ROUTES])

    query = f"""
        SELECT
            bd.route_id,
            r.route_short_name,
            bd.stop_id,
            bd.trip_id,
            bd.delay_seconds,
            bd.recorded_at,
            s.stop_lat,
            s.stop_lon,
            t.direction_id
        FROM bus_delays bd
        JOIN routes r ON bd.route_id = r.route_id
        JOIN stops s ON bd.stop_id = s.stop_id
        JOIN trips t ON bd.trip_id = t.trip_id
        WHERE r.route_short_name IN ({routes_placeholder})
          AND bd.route_id IS NOT NULL
    """

    df = pd.read_sql(query, conn)
    conn.close()

    print(f"  Loaded {len(df):,} records")
    return df


def load_stop_sequences() -> pd.DataFrame:
    """Load stop sequences from stop_times."""
    print("Loading stop sequences...")
    conn = psycopg2.connect(DATABASE_URL)

    query = """
        SELECT DISTINCT trip_id, stop_id, stop_sequence
        FROM stop_times
    """

    df = pd.read_sql(query, conn)
    conn.close()

    print(f"  Loaded {len(df):,} stop sequences")
    return df


def load_weather() -> pd.DataFrame:
    """Load weather data."""
    print("Loading weather data...")
    conn = psycopg2.connect(DATABASE_URL)

    query = """
        SELECT
            station_id,
            recorded_at,
            lat as weather_lat,
            lon as weather_lon,
            temperature_c,
            humidity_percent,
            wind_speed_kmh,
            precipitation_mm,
            visibility_km
        FROM weather
        WHERE recorded_at >= '2026-01-10'
    """

    df = pd.read_sql(query, conn)
    conn.close()

    print(f"  Loaded {len(df):,} weather records")
    return df


def load_road_conditions() -> pd.DataFrame:
    """Load road conditions data."""
    print("Loading road conditions...")
    conn = psycopg2.connect(DATABASE_URL)

    query = """
        SELECT
            event_id,
            event_type,
            severity,
            lat as event_lat,
            lon as event_lon,
            created_at,
            updated_at,
            status
        FROM road_conditions
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """

    df = pd.read_sql(query, conn)
    conn.close()

    print(f"  Loaded {len(df):,} road events")
    return df


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add time-based features."""
    print("Adding time features...")

    df['recorded_at'] = pd.to_datetime(df['recorded_at'], utc=True)
    df['hour'] = df['recorded_at'].dt.hour
    df['day_of_week'] = df['recorded_at'].dt.dayofweek
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
    df['is_rush_hour'] = ((df['hour'] >= 7) & (df['hour'] <= 9) |
                          (df['hour'] >= 16) & (df['hour'] <= 18)).astype(int)

    return df


def add_stop_sequence(df: pd.DataFrame, stop_seq_df: pd.DataFrame) -> pd.DataFrame:
    """Add stop sequence feature."""
    print("Adding stop sequences...")

    df = df.merge(stop_seq_df, on=['trip_id', 'stop_id'], how='left')
    df['stop_sequence'] = df['stop_sequence'].fillna(0)

    return df


def find_nearest_weather(bus_df: pd.DataFrame, weather_df: pd.DataFrame) -> pd.DataFrame:
    """Match each bus record with nearest weather station at closest time."""
    print("Matching weather data...")

    weather_df['recorded_at'] = pd.to_datetime(weather_df['recorded_at'], utc=True)

    # Round bus times to 10-minute intervals for matching
    bus_df['weather_time'] = bus_df['recorded_at'].dt.floor('10min')
    weather_df['weather_time'] = weather_df['recorded_at'].dt.floor('10min')

    # Aggregate weather by time (average across stations for simplicity)
    weather_agg = weather_df.groupby('weather_time').agg({
        'temperature_c': 'mean',
        'humidity_percent': 'mean',
        'wind_speed_kmh': 'mean',
        'precipitation_mm': 'mean',
        'visibility_km': 'mean',
    }).reset_index()

    # Merge on time
    df = bus_df.merge(weather_agg, on='weather_time', how='left')

    # Fill missing weather with forward/backward fill
    weather_cols = ['temperature_c', 'humidity_percent', 'wind_speed_kmh',
                    'precipitation_mm', 'visibility_km']
    df[weather_cols] = df[weather_cols].fillna(method='ffill').fillna(method='bfill')

    return df


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate haversine distance in km."""
    R = 6371  # Earth's radius in km

    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))

    return R * c


def add_road_condition_features(bus_df: pd.DataFrame, road_df: pd.DataFrame) -> pd.DataFrame:
    """Add road condition features based on active events near each stop."""
    print("Adding road condition features...")

    # Convert timestamps
    road_df['created_at'] = pd.to_datetime(road_df['created_at'], utc=True)
    road_df['updated_at'] = pd.to_datetime(road_df['updated_at'], utc=True)

    # Initialize columns
    bus_df['active_incidents'] = 0
    bus_df['active_construction'] = 0
    bus_df['nearest_event_distance_km'] = 999.0

    # For efficiency, process in batches by time
    bus_df['event_time'] = bus_df['recorded_at'].dt.floor('1h')

    # Group road events by hour they were active
    # An event is active if created_at <= time and (status == 'ACTIVE' or updated_at >= time)

    unique_times = bus_df['event_time'].unique()

    for i, event_time in enumerate(unique_times):
        if i % 100 == 0:
            print(f"  Processing time {i+1}/{len(unique_times)}...")

        # Find active events at this time
        active_events = road_df[
            (road_df['created_at'] <= event_time) &
            ((road_df['status'] == 'ACTIVE') | (road_df['updated_at'] >= event_time))
        ]

        if len(active_events) == 0:
            continue

        # Get bus records at this time
        bus_mask = bus_df['event_time'] == event_time
        bus_subset = bus_df.loc[bus_mask]

        if len(bus_subset) == 0:
            continue

        # Calculate features for each bus record
        for idx in bus_subset.index:
            stop_lat = bus_df.loc[idx, 'stop_lat']
            stop_lon = bus_df.loc[idx, 'stop_lon']

            # Calculate distances to all active events
            distances = haversine_distance(
                stop_lat, stop_lon,
                active_events['event_lat'].values,
                active_events['event_lon'].values
            )

            # Count events within 5km
            nearby_mask = distances < 5
            nearby_events = active_events[nearby_mask]

            bus_df.loc[idx, 'active_incidents'] = (nearby_events['event_type'] == 'INCIDENT').sum()
            bus_df.loc[idx, 'active_construction'] = (nearby_events['event_type'] == 'CONSTRUCTION').sum()

            if len(distances) > 0:
                bus_df.loc[idx, 'nearest_event_distance_km'] = distances.min()

    return bus_df


def add_road_condition_features_fast(bus_df: pd.DataFrame, road_df: pd.DataFrame) -> pd.DataFrame:
    """Add road condition features (faster vectorized version)."""
    print("Adding road condition features (fast)...")

    # Convert timestamps
    road_df['created_at'] = pd.to_datetime(road_df['created_at'], utc=True)

    # For simplicity, count total active events by type during the data period
    incidents = road_df[road_df['event_type'] == 'INCIDENT']
    construction = road_df[road_df['event_type'] == 'CONSTRUCTION']

    # Initialize with zeros
    bus_df['active_incidents'] = 0
    bus_df['active_construction'] = 0
    bus_df['nearest_event_distance_km'] = 50.0  # Default far distance

    # For each unique stop, calculate distance to nearest event
    unique_stops = bus_df[['stop_id', 'stop_lat', 'stop_lon']].drop_duplicates()

    print(f"  Processing {len(unique_stops)} unique stops...")

    for _, stop in unique_stops.iterrows():
        stop_lat, stop_lon = stop['stop_lat'], stop['stop_lon']

        # Calculate distances to all events
        if len(road_df) > 0:
            distances = haversine_distance(
                stop_lat, stop_lon,
                road_df['event_lat'].values,
                road_df['event_lon'].values
            )

            nearby_mask = distances < 5  # Within 5km
            nearby_events = road_df[nearby_mask]

            n_incidents = (nearby_events['event_type'] == 'INCIDENT').sum()
            n_construction = (nearby_events['event_type'] == 'CONSTRUCTION').sum()
            min_distance = distances.min() if len(distances) > 0 else 50.0

            # Update all records for this stop
            stop_mask = bus_df['stop_id'] == stop['stop_id']
            bus_df.loc[stop_mask, 'active_incidents'] = n_incidents
            bus_df.loc[stop_mask, 'active_construction'] = n_construction
            bus_df.loc[stop_mask, 'nearest_event_distance_km'] = min_distance

    return bus_df


def prepare_features(df: pd.DataFrame) -> tuple:
    """Prepare feature matrix and target."""
    print("Preparing features...")

    feature_cols = [
        # Time features
        'hour', 'day_of_week', 'is_weekend', 'is_rush_hour',
        # Route/stop features
        'route_short_name', 'direction_id', 'stop_sequence', 'stop_lat', 'stop_lon',
        # Weather features
        'temperature_c', 'humidity_percent', 'wind_speed_kmh',
        'precipitation_mm', 'visibility_km',
        # Road condition features
        'active_incidents', 'active_construction', 'nearest_event_distance_km',
    ]

    # Ensure all columns exist
    for col in feature_cols:
        if col not in df.columns:
            print(f"  Warning: {col} not found, filling with 0")
            df[col] = 0

    X = df[feature_cols].copy()
    y = df['delay_seconds'].copy()

    # Convert categorical features to category dtype
    categorical_cols = ['route_short_name']
    for col in categorical_cols:
        X[col] = X[col].astype('category')

    print(f"  Features shape: {X.shape}")
    print(f"  Target shape: {y.shape}")

    return X, y, categorical_cols


def train_model(X_train, y_train, X_val, y_val, categorical_cols):
    """Train LightGBM model."""
    print("\nTraining LightGBM model...")

    # Create datasets
    train_data = lgb.Dataset(
        X_train, label=y_train,
        categorical_feature=categorical_cols
    )
    val_data = lgb.Dataset(
        X_val, label=y_val,
        categorical_feature=categorical_cols,
        reference=train_data
    )

    # Model parameters
    params = {
        'objective': 'regression',
        'metric': 'mae',
        'boosting_type': 'gbdt',
        'num_leaves': 63,
        'learning_rate': 0.05,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'verbose': -1,
        'seed': 42,
    }

    # Train with early stopping
    model = lgb.train(
        params,
        train_data,
        num_boost_round=1000,
        valid_sets=[train_data, val_data],
        valid_names=['train', 'valid'],
        callbacks=[
            lgb.early_stopping(stopping_rounds=50),
            lgb.log_evaluation(period=100),
        ]
    )

    return model


def evaluate_model(model, X_val, y_val):
    """Evaluate model performance."""
    print("\nEvaluating model...")

    y_pred = model.predict(X_val)

    mae = mean_absolute_error(y_val, y_pred)
    rmse = np.sqrt(mean_squared_error(y_val, y_pred))
    r2 = r2_score(y_val, y_pred)

    print(f"  MAE:  {mae:.2f} seconds")
    print(f"  RMSE: {rmse:.2f} seconds")
    print(f"  R²:   {r2:.4f}")

    # Feature importance
    print("\nFeature Importance (Top 10):")
    importance = pd.DataFrame({
        'feature': model.feature_name(),
        'importance': model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)

    for _, row in importance.head(10).iterrows():
        print(f"  {row['feature']:30s}: {row['importance']:.0f}")

    return {'mae': mae, 'rmse': rmse, 'r2': r2}


def main():
    print("=" * 60)
    print("Bus Delay Prediction Model Training")
    print("=" * 60)
    print()

    # Load data
    bus_df = load_bus_delays()
    stop_seq_df = load_stop_sequences()
    weather_df = load_weather()
    road_df = load_road_conditions()

    # Add features
    bus_df = add_time_features(bus_df)
    bus_df = add_stop_sequence(bus_df, stop_seq_df)
    bus_df = find_nearest_weather(bus_df, weather_df)
    bus_df = add_road_condition_features_fast(bus_df, road_df)

    # Drop rows with missing values
    print(f"\nBefore dropna: {len(bus_df):,} rows")
    bus_df = bus_df.dropna(subset=['delay_seconds', 'stop_lat', 'stop_lon'])
    print(f"After dropna: {len(bus_df):,} rows")

    # Prepare features
    X, y, categorical_cols = prepare_features(bus_df)

    # Train/validation split (80/20, time-based would be better but random for now)
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    print(f"\nTrain size: {len(X_train):,}")
    print(f"Validation size: {len(X_val):,}")

    # Train model
    model = train_model(X_train, y_train, X_val, y_val, categorical_cols)

    # Evaluate
    metrics = evaluate_model(model, X_val, y_val)

    # Save model
    model_path = 'delay_model.lgb'
    model.save_model(model_path)
    print(f"\nModel saved to {model_path}")

    # Save feature names
    feature_path = 'delay_model_features.txt'
    with open(feature_path, 'w') as f:
        for feat in model.feature_name():
            f.write(f"{feat}\n")
    print(f"Feature names saved to {feature_path}")

    print("\n" + "=" * 60)
    print("Training complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
