#!/usr/bin/env python3
"""
DriveBC Road Conditions Collector
Fetches road events from Open511-DriveBC API for Metro Vancouver.
"""
import os
import ssl
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Open511-DriveBC API
DRIVEBC_API_URL = "https://api.open511.gov.bc.ca/events"

# Metro Vancouver bounding box (same as weather data)
METRO_VANCOUVER_BBOX = "-124.5,48.0,-121.0,50.0"

DATABASE_URL = os.environ.get('DATABASE_URL')


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


def save_events(events: list) -> int:
    """Save road events to CockroachDB using upsert."""
    if not events:
        return 0

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    collected_at = datetime.now(timezone.utc)
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
                e['created_at'],
                e['updated_at'],
                collected_at,
            ))
            saved_count += 1

        conn.commit()
        return saved_count
    finally:
        cur.close()
        conn.close()


def main():
    print("Fetching road events from DriveBC API...")
    print(f"Bounding box: {METRO_VANCOUVER_BBOX}")
    print()

    events = fetch_road_events()
    print(f"Found {len(events)} active events")

    if events:
        # Print summary by event type
        by_type = {}
        for e in events:
            t = e['event_type'] or 'UNKNOWN'
            by_type[t] = by_type.get(t, 0) + 1

        print("\nEvents by type:")
        for t, count in sorted(by_type.items()):
            print(f"  {t}: {count}")

        # Print sample events
        print("\nSample events:")
        for e in events[:5]:
            print(f"  [{e['severity']}] {e['event_type']}: {e['headline']}")
            if e['road_name']:
                print(f"       Road: {e['road_name']} {e['direction'] or ''}")
            print()

        # Save to database
        saved = save_events(events)
        print(f"Saved {saved} events to database")


if __name__ == "__main__":
    main()
