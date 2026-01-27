"""
DriveBC Road Conditions Collector - AWS Lambda
Fetches road events from Open511-DriveBC API for Metro Vancouver.
"""
import os
import json
import ssl
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
import pg8000

# Open511-DriveBC API
DRIVEBC_API_URL = "https://api.open511.gov.bc.ca/events"

# Metro Vancouver bounding box (same as weather data)
METRO_VANCOUVER_BBOX = "-124.5,48.0,-121.0,50.0"


def fetch_road_events() -> list:
    """Fetch active road events from DriveBC API for Metro Vancouver."""
    params = {
        "status": "ACTIVE",
        "bbox": METRO_VANCOUVER_BBOX,
        "format": "json",
    }

    response = requests.get(DRIVEBC_API_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    events = []
    for event in data.get("events", []):
        # Extract geography (point coordinates)
        geography = event.get("geography", {})
        coords = geography.get("coordinates", [])

        # Handle different geometry types
        lat, lon = None, None
        if geography.get("type") == "Point" and len(coords) >= 2:
            lon, lat = coords[0], coords[1]
        elif geography.get("type") == "LineString" and coords:
            # Use first point of linestring
            lon, lat = coords[0][0], coords[0][1]

        # Extract road info
        roads = event.get("roads", [])
        road_name = roads[0].get("name") if roads else None
        direction = roads[0].get("direction") if roads else None

        # Extract event subtype
        event_subtypes = event.get("event_subtypes", [])
        event_subtype = event_subtypes[0] if event_subtypes else None

        # Parse timestamps
        created_at = None
        updated_at = None
        if event.get("created"):
            try:
                created_at = datetime.fromisoformat(event["created"].replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass
        if event.get("updated"):
            try:
                updated_at = datetime.fromisoformat(event["updated"].replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass

        parsed_event = {
            'event_id': event.get("id"),
            'status': event.get("status"),
            'severity': event.get("severity"),
            'event_type': event.get("event_type"),
            'event_subtype': event_subtype,
            'headline': event.get("headline"),
            'description': event.get("description"),
            'road_name': road_name,
            'direction': direction,
            'lat': lat,
            'lon': lon,
            'created_at': created_at,
            'updated_at': updated_at,
        }

        events.append(parsed_event)

    return events


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


def save_events_batch(database_url: str, events: list, collected_at: datetime) -> int:
    """Save road events to CockroachDB using upsert."""
    if not events:
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
        for e in events:
            cur.execute('''
                INSERT INTO road_conditions (
                    event_id, status, severity, event_type, event_subtype,
                    headline, description, road_name, direction,
                    lat, lon, created_at, updated_at, collected_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id, updated_at) DO UPDATE SET
                    status = EXCLUDED.status,
                    severity = EXCLUDED.severity,
                    collected_at = EXCLUDED.collected_at
            ''', (
                e['event_id'],
                e['status'],
                e['severity'],
                e['event_type'],
                e['event_subtype'],
                e['headline'],
                e['description'],
                e['road_name'],
                e['direction'],
                e['lat'],
                e['lon'],
                e['created_at'].isoformat() if e['created_at'] else None,
                e['updated_at'].isoformat() if e['updated_at'] else None,
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
        # 1. Fetch road events from DriveBC API
        events = fetch_road_events()

        if not events:
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No active road events'})
            }

        # 2. Save to database
        saved_count = save_events_batch(database_url, events, collected_at)

        # Summary by event type
        by_type = {}
        for e in events:
            t = e['event_type'] or 'UNKNOWN'
            by_type[t] = by_type.get(t, 0) + 1

        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'events_count': saved_count,
                'by_type': by_type,
                'message': f"Saved {saved_count} road events",
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
