#!/usr/bin/env python3
"""
Backfill road conditions data from DriveBC API.

Fetches ARCHIVED events from Open511-DriveBC API for a specified date range.
"""
import os
from datetime import datetime, timezone
from typing import Optional

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DRIVEBC_API_URL = "https://api.open511.gov.bc.ca/events"
METRO_VANCOUVER_BBOX = "-124.5,48.0,-121.0,50.0"
DATABASE_URL = os.environ.get('DATABASE_URL')


def parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse datetime string from API."""
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except ValueError:
        return None


def fetch_archived_events(start_date: str) -> list:
    """Fetch all archived events since start_date using pagination."""
    all_events = []
    offset = 0
    limit = 500

    while True:
        params = {
            "status": "ARCHIVED",
            "bbox": METRO_VANCOUVER_BBOX,
            "created": f">{start_date}",
            "limit": limit,
            "offset": offset,
            "format": "json",
        }

        print(f"  Fetching offset {offset}...")
        response = requests.get(DRIVEBC_API_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        events = data.get("events", [])
        if not events:
            break

        all_events.extend(events)
        print(f"    Got {len(events)} events (total: {len(all_events)})")

        if len(events) < limit:
            break

        offset += limit

    return all_events


def parse_event(event: dict) -> dict:
    """Parse API event into database record format."""
    # Extract geography
    geography = event.get("geography", {})
    coords = geography.get("coordinates", [])

    lat, lon = None, None
    if geography.get("type") == "Point" and len(coords) >= 2:
        lon, lat = coords[0], coords[1]
    elif geography.get("type") == "LineString" and coords:
        lon, lat = coords[0][0], coords[0][1]

    # Extract road info
    roads = event.get("roads", [])
    road_name = roads[0].get("name") if roads else None
    direction = roads[0].get("direction") if roads else None

    # Extract event subtype
    event_subtypes = event.get("event_subtypes", [])
    event_subtype = event_subtypes[0] if event_subtypes else None

    return {
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
        'created_at': parse_datetime(event.get("created")),
        'updated_at': parse_datetime(event.get("updated")),
    }


def save_to_db(events: list) -> int:
    """Save events to database using upsert."""
    if not events:
        return 0

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    collected_at = datetime.now(timezone.utc)

    try:
        records = []
        for event in events:
            e = parse_event(event)
            records.append((
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

        sql = """
            INSERT INTO road_conditions (
                event_id, status, severity, event_type, event_subtype,
                headline, description, road_name, direction,
                lat, lon, created_at, updated_at, collected_at
            ) VALUES %s
            ON CONFLICT (event_id, updated_at) DO UPDATE SET
                status = EXCLUDED.status,
                severity = EXCLUDED.severity,
                collected_at = EXCLUDED.collected_at
        """

        execute_values(cur, sql, records, page_size=500)
        conn.commit()

        return len(records)
    finally:
        cur.close()
        conn.close()


def main():
    import sys

    start_date = "2026-01-01"
    if len(sys.argv) >= 2:
        start_date = sys.argv[1]

    print(f"Backfilling road conditions from {start_date}")
    print(f"Bounding box: {METRO_VANCOUVER_BBOX}")
    print()

    # Fetch archived events
    print("Fetching ARCHIVED events from DriveBC API...")
    events = fetch_archived_events(start_date)
    print(f"\nTotal archived events: {len(events)}")

    if events:
        # Summary by type
        by_type = {}
        for e in events:
            t = e.get('event_type', 'UNKNOWN')
            by_type[t] = by_type.get(t, 0) + 1

        print("\nBy event type:")
        for t, count in sorted(by_type.items(), key=lambda x: -x[1]):
            print(f"  {t}: {count}")

        # Save to database
        print("\nSaving to database...")
        saved = save_to_db(events)
        print(f"Saved {saved} records")

    # Also fetch current ACTIVE events
    print("\nFetching current ACTIVE events...")
    params = {
        "status": "ACTIVE",
        "bbox": METRO_VANCOUVER_BBOX,
        "format": "json",
    }
    response = requests.get(DRIVEBC_API_URL, params=params, timeout=30)
    response.raise_for_status()
    active_events = response.json().get("events", [])

    if active_events:
        saved = save_to_db(active_events)
        print(f"Saved {saved} active events")

    print("\nDone!")


if __name__ == "__main__":
    main()
