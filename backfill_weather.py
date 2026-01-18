#!/usr/bin/env python3
"""
Backfill weather data from Environment Canada SWOB API.
Fetches historical data from a specified start date to now.
"""
import os
import ssl
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# Metro Vancouver & surrounding area SWOB stations (15 stations)
STATIONS = [
    {"id": "1100119", "name": "AGASSIZ RCS"},
    {"id": "CYXX", "name": "Abbotsford"},
    {"id": "1102415", "name": "DELTA BURNS BOG"},
    {"id": "1113543", "name": "HOPE AIRPORT"},
    {"id": "1106178", "name": "PITT MEADOWS CS"},
    {"id": "1106200", "name": "POINT ATKINSON"},
    {"id": "1047172", "name": "SECHELT AUT"},
    {"id": "10476F0", "name": "SQUAMISH AIRPORT"},
    {"id": "1108824", "name": "WEST VANCOUVER AUT"},
    {"id": "1108910", "name": "WHITE ROCK"},
    {"id": "1012710", "name": "ESQUIMALT HARBOUR"},
    {"id": "1015630", "name": "NORTH COWICHAN"},
    {"id": "1016943", "name": "SAANICHTON CFIA"},
    {"id": "1018611", "name": "VICTORIA GONZALES CS"},
    {"id": "1018598", "name": "VICTORIA UNIVERSITY CS"},
]

VALID_STATION_IDS = {s["id"] for s in STATIONS}
SWOB_API_URL = "https://api.weather.gc.ca/collections/swob-realtime/items"
DATABASE_URL = os.environ.get('DATABASE_URL')


def fetch_historical_data(start_date: datetime, end_date: datetime) -> list:
    """Fetch historical weather data for a date range."""
    all_records = []

    # API datetime format
    datetime_range = f"{start_date.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_date.strftime('%Y-%m-%dT%H:%M:%SZ')}"

    params = {
        "bbox": "-124.5,48.0,-121.0,50.0",
        "datetime": datetime_range,
        "limit": 10000,
        "f": "json",
    }

    print(f"Fetching data for {datetime_range}...")

    response = requests.get(SWOB_API_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    features = data.get("features", [])
    print(f"  Got {len(features)} raw features")

    # Process and deduplicate by station + recorded_at
    station_data = {}

    for feature in features:
        props = feature.get("properties", {})
        coords = feature.get("geometry", {}).get("coordinates", [])

        station_id = props.get("icao_stn_id-value") or props.get("msc_id-value", "")
        station_name = props.get("stn_nam-value", "")

        if not station_id or not coords:
            continue

        if station_id not in VALID_STATION_IDS:
            continue

        # Parse observation time
        obs_time_str = props.get("date_tm-value")
        if obs_time_str:
            try:
                recorded_at = datetime.fromisoformat(obs_time_str.replace('Z', '+00:00'))
            except:
                continue
        else:
            continue

        # Create unique key
        key = (station_id, recorded_at.isoformat())

        weather = {
            'station_id': station_id,
            'station_name': station_name,
            'recorded_at': recorded_at,
            'lat': coords[1] if len(coords) > 1 else None,
            'lon': coords[0] if len(coords) > 0 else None,
            'temperature_c': props.get("air_temp"),
            'humidity_percent': props.get("rel_hum"),
            'wind_speed_kmh': props.get("avg_wnd_spd_10m_pst10mts"),
            'wind_direction': props.get("avg_wnd_dir_10m_pst2mts") or props.get("avg_wnd_dir_10m_pst10mts"),
            'pressure_hpa': props.get("mslp"),
            'visibility_km': props.get("avg_vis_pst10mts"),
        }

        station_data[key] = weather

    all_records = list(station_data.values())
    print(f"  Filtered to {len(all_records)} unique records for target stations")

    return all_records


def save_to_db(records: list) -> int:
    """Save weather records to database using upsert."""
    if not records:
        return 0

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    collected_at = datetime.now(timezone.utc)

    try:
        # Prepare data for batch insert
        values = [
            (
                r['station_id'],
                r['station_name'],
                r['recorded_at'],
                r['lat'],
                r['lon'],
                r['temperature_c'],
                r['humidity_percent'],
                r['wind_speed_kmh'],
                r['wind_direction'],
                r['pressure_hpa'],
                r['visibility_km'],
                collected_at,
            )
            for r in records
        ]

        # Upsert using ON CONFLICT
        sql = """
            INSERT INTO weather (
                station_id, station_name, recorded_at, lat, lon,
                temperature_c, humidity_percent, wind_speed_kmh,
                wind_direction, pressure_hpa, visibility_km, collected_at
            ) VALUES %s
            ON CONFLICT (station_id, recorded_at) DO UPDATE SET
                temperature_c = EXCLUDED.temperature_c,
                humidity_percent = EXCLUDED.humidity_percent,
                wind_speed_kmh = EXCLUDED.wind_speed_kmh,
                wind_direction = EXCLUDED.wind_direction,
                pressure_hpa = EXCLUDED.pressure_hpa,
                visibility_km = EXCLUDED.visibility_km,
                collected_at = EXCLUDED.collected_at
        """

        execute_values(cur, sql, values, page_size=1000)
        conn.commit()

        return len(records)
    finally:
        cur.close()
        conn.close()


def main():
    # Backfill from January 10, 2026
    start_date = datetime(2026, 1, 10, 0, 0, 0, tzinfo=timezone.utc)
    end_date = datetime.now(timezone.utc)

    print(f"Backfilling weather data from {start_date} to {end_date}")
    print(f"Target stations: {len(VALID_STATION_IDS)}")
    print()

    total_saved = 0

    # Fetch in 1-day chunks to avoid API limits
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=1), end_date)

        try:
            records = fetch_historical_data(current, chunk_end)
            if records:
                saved = save_to_db(records)
                total_saved += saved
                print(f"  Saved {saved} records")
        except Exception as e:
            print(f"  Error: {e}")

        current = chunk_end

    print()
    print(f"Done! Total records saved: {total_saved}")


if __name__ == "__main__":
    main()
