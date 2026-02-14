#!/usr/bin/env python3
"""
Import all CSV data from db_export/ into a new CockroachDB cluster.
Creates all tables first, then imports data.
"""

import os
import sys

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
EXPORT_DIR = "db_export"

# ── Table creation SQL ──────────────────────────────────────────────

CREATE_TABLES_SQL = [
    # bus_delays
    """
    CREATE TABLE IF NOT EXISTS bus_delays (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        route_id STRING,
        stop_id STRING,
        trip_id STRING,
        delay_seconds INT,
        vehicle_id STRING,
        recorded_at TIMESTAMPTZ,
        collected_at TIMESTAMPTZ,
        INDEX idx_route_id (route_id),
        INDEX idx_stop_id (stop_id),
        INDEX idx_recorded_at (recorded_at)
    );
    """,
    # weather
    """
    CREATE TABLE IF NOT EXISTS weather (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        station_id STRING,
        station_name STRING,
        recorded_at TIMESTAMPTZ,
        lat DOUBLE PRECISION,
        lon DOUBLE PRECISION,
        temperature_c DOUBLE PRECISION,
        humidity_percent DOUBLE PRECISION,
        wind_speed_kmh DOUBLE PRECISION,
        wind_direction DOUBLE PRECISION,
        pressure_hpa DOUBLE PRECISION,
        visibility_km DOUBLE PRECISION,
        precipitation_mm DOUBLE PRECISION,
        collected_at TIMESTAMPTZ,
        UNIQUE (station_id, recorded_at)
    );
    """,
    # road_conditions
    """
    CREATE TABLE IF NOT EXISTS road_conditions (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        event_id STRING NOT NULL,
        status STRING,
        severity STRING,
        event_type STRING,
        event_subtype STRING,
        headline STRING,
        description TEXT,
        road_name STRING,
        direction STRING,
        lat DOUBLE PRECISION,
        lon DOUBLE PRECISION,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        collected_at TIMESTAMPTZ,
        UNIQUE (event_id, updated_at)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_road_conditions_event_type ON road_conditions (event_type);",
    "CREATE INDEX IF NOT EXISTS idx_road_conditions_created_at ON road_conditions (created_at);",
    "CREATE INDEX IF NOT EXISTS idx_road_conditions_location ON road_conditions (lat, lon);",
    # routes
    """
    CREATE TABLE IF NOT EXISTS routes (
        route_id TEXT PRIMARY KEY,
        agency_id TEXT,
        route_short_name TEXT,
        route_long_name TEXT,
        route_type INT
    );
    """,
    # trips
    """
    CREATE TABLE IF NOT EXISTS trips (
        trip_id TEXT PRIMARY KEY,
        route_id TEXT,
        service_id TEXT,
        trip_headsign TEXT,
        direction_id INT,
        shape_id TEXT
    );
    """,
    # calendar
    """
    CREATE TABLE IF NOT EXISTS calendar (
        service_id TEXT PRIMARY KEY,
        monday INT,
        tuesday INT,
        wednesday INT,
        thursday INT,
        friday INT,
        saturday INT,
        sunday INT,
        start_date TEXT,
        end_date TEXT
    );
    """,
    # calendar_dates
    """
    CREATE TABLE IF NOT EXISTS calendar_dates (
        service_id TEXT,
        date TEXT,
        exception_type INT,
        PRIMARY KEY (service_id, date)
    );
    """,
    # stop_times
    """
    CREATE TABLE IF NOT EXISTS stop_times (
        trip_id TEXT,
        stop_id TEXT,
        arrival_time TEXT,
        departure_time TEXT,
        stop_sequence INT,
        PRIMARY KEY (trip_id, stop_sequence)
    );
    """,
    # stops
    """
    CREATE TABLE IF NOT EXISTS stops (
        stop_id TEXT PRIMARY KEY,
        stop_code TEXT,
        stop_name TEXT,
        stop_lat DOUBLE PRECISION,
        stop_lon DOUBLE PRECISION,
        zone_id TEXT
    );
    """,
]

# ── Column mappings (CSV columns → DB columns, excluding auto-generated id) ─

TABLE_COLUMNS = {
    "bus_delays": ["id", "route_id", "stop_id", "trip_id", "delay_seconds", "vehicle_id", "recorded_at", "collected_at"],
    "weather": ["id", "station_id", "station_name", "recorded_at", "lat", "lon", "temperature_c", "humidity_percent", "wind_speed_kmh", "wind_direction", "pressure_hpa", "visibility_km", "precipitation_mm", "collected_at"],
    "road_conditions": ["id", "event_id", "status", "severity", "event_type", "event_subtype", "headline", "description", "road_name", "direction", "lat", "lon", "created_at", "updated_at", "collected_at"],
    "routes": ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"],
    "trips": ["trip_id", "route_id", "service_id", "trip_headsign", "direction_id", "shape_id"],
    "calendar": ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"],
    "calendar_dates": ["service_id", "date", "exception_type"],
    "stop_times": ["trip_id", "stop_id", "arrival_time", "departure_time", "stop_sequence"],
    "stops": ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon", "zone_id"],
}

BATCH_SIZE = 5000


def create_tables(conn):
    """Create all tables on the new cluster."""
    print("Creating tables...")
    with conn.cursor() as cur:
        for sql in CREATE_TABLES_SQL:
            cur.execute(sql)
    conn.commit()
    print("  All tables created.\n")


def import_table(conn, table_name: str):
    """Import a single CSV into the database."""
    filepath = os.path.join(EXPORT_DIR, f"{table_name}.csv")
    if not os.path.exists(filepath):
        print(f"  {table_name}: CSV not found (skipped)")
        return 0

    columns = TABLE_COLUMNS[table_name]

    df = pd.read_csv(filepath, dtype=str, keep_default_na=False)
    if df.empty:
        print(f"  {table_name}: empty CSV (skipped)")
        return 0

    # Use only columns that exist in both CSV and table definition
    csv_cols = [c for c in columns if c in df.columns]
    df = df[csv_cols]

    # Replace empty strings with None for proper NULL handling
    df = df.where(df != "", None)

    col_list = ", ".join(csv_cols)
    placeholders = "(" + ", ".join(["%s"] * len(csv_cols)) + ")"

    rows = [tuple(row) for row in df.values]
    total = len(rows)

    with conn.cursor() as cur:
        for i in range(0, total, BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            execute_values(
                cur,
                f"INSERT INTO {table_name} ({col_list}) VALUES %s ON CONFLICT DO NOTHING",
                batch,
                template=placeholders,
                page_size=BATCH_SIZE,
            )
            conn.commit()
            done = min(i + BATCH_SIZE, total)
            if total > BATCH_SIZE:
                print(f"    {done:,}/{total:,} rows...", flush=True)

    print(f"  {table_name}: {total:,} rows imported")
    return total


def main():
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL not set in .env")
        sys.exit(1)

    if not os.path.isdir(EXPORT_DIR):
        print(f"ERROR: {EXPORT_DIR}/ directory not found. Run export_all_data.py first.")
        sys.exit(1)

    print(f"Connecting to new CockroachDB cluster...")
    conn = psycopg2.connect(DATABASE_URL)

    create_tables(conn)

    print(f"Importing data from {EXPORT_DIR}/\n")
    total_rows = 0
    for table_name in TABLE_COLUMNS:
        total_rows += import_table(conn, table_name)

    conn.close()
    print(f"\nDone! Total: {total_rows:,} rows imported.")


if __name__ == "__main__":
    main()
