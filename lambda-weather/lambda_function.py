"""
Environment Canada Weather Data Collector - AWS Lambda
Fetches real-time weather from 16 SWOB stations across Metro Vancouver
"""
import os
import json
import ssl
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
import pg8000

# Metro Vancouver SWOB stations (14 stations with complete data)
# Excluded: TSAWWASSEN FERRY AUTO (no temp/humidity), VANCOUVER HARBOUR CS (no wind)
STATIONS = [
    {"id": "1107010", "name": "SANDHEADS CS"},
    {"id": "10459NN", "name": "HOWE SOUND - PAM ROCKS"},
    {"id": "1106200", "name": "POINT ATKINSON"},
    {"id": "1108824", "name": "WEST VANCOUVER AUT"},
    {"id": "1108380", "name": "VANCOUVER SEA ISLAND CCG"},
    {"id": "CYVR", "name": "Vancouver International"},
    {"id": "CYHC", "name": "Vancouver Harbour"},
    {"id": "CZBB", "name": "Vancouver Boundary Bay"},
    {"id": "1102415", "name": "DELTA BURNS BOG"},
    {"id": "1108910", "name": "WHITE ROCK"},
    {"id": "CYPK", "name": "Pitt Meadows"},
    {"id": "1106178", "name": "PITT MEADOWS CS"},
    {"id": "CYNJ", "name": "Langley Regional"},
    {"id": "CYXX", "name": "Abbotsford"},
]

# Station IDs to include (for filtering)
VALID_STATION_IDS = {s["id"] for s in STATIONS}

SWOB_API_URL = "https://api.weather.gc.ca/collections/swob-realtime/items"


def fetch_all_stations() -> list:
    """Fetch weather data from all SWOB stations in Metro Vancouver."""
    # Use bounding box to get all Vancouver area stations
    params = {
        "bbox": "-123.5,48.9,-122.2,49.5",
        "limit": 200,
        "f": "json",
    }

    response = requests.get(SWOB_API_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Process features and get latest reading per station
    station_data = {}

    for feature in data.get("features", []):
        props = feature.get("properties", {})
        coords = feature.get("geometry", {}).get("coordinates", [])

        # Get station identifier
        station_id = props.get("icao_stn_id-value") or props.get("msc_id-value", "")
        station_name = props.get("stn_nam-value", "")

        if not station_id or not coords:
            continue

        # Only include stations with complete data
        if station_id not in VALID_STATION_IDS:
            continue

        # Parse observation time
        obs_time_str = props.get("date_tm-value")
        if obs_time_str:
            try:
                recorded_at = datetime.fromisoformat(obs_time_str.replace('Z', '+00:00'))
            except:
                recorded_at = datetime.now(timezone.utc)
        else:
            recorded_at = datetime.now(timezone.utc)

        # Only keep the latest reading per station
        if station_id in station_data:
            if recorded_at <= station_data[station_id]['recorded_at']:
                continue

        # Extract weather data
        weather = {
            'station_id': station_id,
            'station_name': station_name,
            'recorded_at': recorded_at,
            'lat': coords[1] if len(coords) > 1 else None,
            'lon': coords[0] if len(coords) > 0 else None,
            'temperature_c': props.get("air_temp"),
            'humidity_percent': props.get("rel_hum"),
            'wind_speed_kmh': props.get("avg_wnd_spd_10m_pst10mts"),
            'wind_direction': props.get("avg_wnd_dir_10m_pst10mts") or props.get("avg_wnd_dir_10m_pst2mts"),
            'pressure_hpa': props.get("mslp"),
            'visibility_km': props.get("avg_vis_pst10mts"),
        }

        station_data[station_id] = weather

    return list(station_data.values())


def parse_database_url(database_url: str):
    """Parse DATABASE_URL into pg8000 connection parameters."""
    parsed = urlparse(database_url)
    return {
        'host': parsed.hostname,
        'port': parsed.port or 26257,
        'user': parsed.username,
        'password': parsed.password,
        'database': parsed.path.lstrip('/').split('?')[0],
    }


def save_weather_batch(database_url: str, weather_list: list, collected_at: datetime) -> int:
    """Save multiple weather records to CockroachDB."""
    if not weather_list:
        return 0

    params = parse_database_url(database_url)

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    conn = pg8000.connect(
        host=params['host'],
        port=params['port'],
        user=params['user'],
        password=params['password'],
        database=params['database'],
        ssl_context=ssl_context,
    )
    cur = conn.cursor()

    saved_count = 0
    try:
        for w in weather_list:
            cur.execute('''
                INSERT INTO weather (station_id, station_name, recorded_at, lat, lon,
                                     temperature_c, humidity_percent, wind_speed_kmh,
                                     wind_direction, pressure_hpa, visibility_km, collected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (station_id, recorded_at) DO UPDATE SET
                    temperature_c = EXCLUDED.temperature_c,
                    humidity_percent = EXCLUDED.humidity_percent,
                    wind_speed_kmh = EXCLUDED.wind_speed_kmh,
                    wind_direction = EXCLUDED.wind_direction,
                    pressure_hpa = EXCLUDED.pressure_hpa,
                    visibility_km = EXCLUDED.visibility_km,
                    collected_at = EXCLUDED.collected_at;
            ''', (
                w['station_id'],
                w['station_name'],
                w['recorded_at'].isoformat(),
                w['lat'],
                w['lon'],
                w['temperature_c'],
                w['humidity_percent'],
                w['wind_speed_kmh'],
                w['wind_direction'],
                w['pressure_hpa'],
                w['visibility_km'],
                collected_at.isoformat(),
            ))
            saved_count += 1

        conn.commit()
        return saved_count
    finally:
        cur.close()
        conn.close()


def lambda_handler(event, context):
    """AWS Lambda entry point."""
    collected_at = datetime.now(timezone.utc)

    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Missing DATABASE_URL'})
        }

    try:
        # 1. Fetch weather data from all stations
        weather_list = fetch_all_stations()

        if not weather_list:
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No weather data available'})
            }

        # 2. Save to database
        saved_count = save_weather_batch(database_url, weather_list, collected_at)

        # Summary
        temps = [w['temperature_c'] for w in weather_list if w['temperature_c'] is not None]
        avg_temp = sum(temps) / len(temps) if temps else None

        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'stations_count': saved_count,
                'avg_temperature': round(avg_temp, 1) if avg_temp else None,
                'message': f"Saved weather from {saved_count} stations",
            })
        }

    except Exception as e:
        import traceback
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'traceback': traceback.format_exc()
            })
        }
