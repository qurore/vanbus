#!/usr/bin/env python3
"""
TransLink GTFS-RT Data Collector
Fetches real-time bus delay data from TransLink API and stores in CockroachDB.
"""

import os
from datetime import datetime, timezone

import requests
from google.transit import gtfs_realtime_pb2
from dotenv import load_dotenv

# Load environment variables from .env file (for local development)
load_dotenv()

# Configuration
TRANSLINK_API_KEY = os.environ.get('TRANSLINK_API_KEY')
DATABASE_URL = os.environ.get('DATABASE_URL')

# TransLink GTFS-RT API V3 endpoints
BASE_URL = "https://gtfsapi.translink.ca/v3"
TRIP_UPDATES_URL = f"{BASE_URL}/gtfsrealtime"
POSITION_URL = f"{BASE_URL}/gtfsposition"


def fetch_trip_updates():
    """Fetch trip updates (delay information) from TransLink API."""
    if not TRANSLINK_API_KEY:
        raise ValueError("TRANSLINK_API_KEY environment variable is not set")

    url = f"{TRIP_UPDATES_URL}?apikey={TRANSLINK_API_KEY}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def parse_trip_updates(feed):
    """Parse GTFS-RT feed and extract delay information."""
    records = []
    timestamp = datetime.now(timezone.utc)

    for entity in feed.entity:
        if not entity.HasField('trip_update'):
            continue

        trip_update = entity.trip_update
        trip_id = trip_update.trip.trip_id if trip_update.trip.HasField('trip_id') else None
        route_id = trip_update.trip.route_id if trip_update.trip.HasField('route_id') else None
        vehicle_id = trip_update.vehicle.id if trip_update.HasField('vehicle') and trip_update.vehicle.HasField('id') else None

        for stop_time_update in trip_update.stop_time_update:
            stop_id = stop_time_update.stop_id if stop_time_update.HasField('stop_id') else None

            # Get arrival delay (in seconds)
            arrival_delay = None
            if stop_time_update.HasField('arrival'):
                arrival_delay = stop_time_update.arrival.delay if stop_time_update.arrival.HasField('delay') else None

            # Get departure delay (in seconds)
            departure_delay = None
            if stop_time_update.HasField('departure'):
                departure_delay = stop_time_update.departure.delay if stop_time_update.departure.HasField('delay') else None

            # Use arrival delay if available, otherwise departure delay
            delay_seconds = arrival_delay if arrival_delay is not None else departure_delay

            if delay_seconds is not None and stop_id:
                records.append({
                    'route_id': route_id,
                    'stop_id': stop_id,
                    'trip_id': trip_id,
                    'delay_seconds': delay_seconds,
                    'vehicle_id': vehicle_id,
                    'recorded_at': timestamp
                })

    return records


def save_to_database(records):
    """Save records to CockroachDB using batch insert."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is not set")

    import psycopg2
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            # Prepare data as list of tuples
            data = [
                (r['route_id'], r['stop_id'], r['trip_id'], r['delay_seconds'], r['vehicle_id'], r['recorded_at'])
                for r in records
            ]
            # Batch insert for much better performance
            execute_values(
                cur,
                """
                INSERT INTO bus_delays (route_id, stop_id, trip_id, delay_seconds, vehicle_id, recorded_at)
                VALUES %s
                """,
                data,
                page_size=1000
            )
        conn.commit()
    finally:
        conn.close()


def main():
    """Main entry point."""
    print(f"[{datetime.now().isoformat()}] Starting data collection...")

    # Fetch and parse trip updates
    feed = fetch_trip_updates()
    records = parse_trip_updates(feed)
    print(f"Parsed {len(records)} delay records from GTFS-RT feed")

    if not records:
        print("No delay records found. Exiting.")
        return

    # Save to database (only if DATABASE_URL is set)
    if DATABASE_URL:
        save_to_database(records)
        print(f"Successfully saved {len(records)} records to database")
    else:
        print("DATABASE_URL not set. Skipping database save.")
        # Print sample records for debugging
        print("\nSample records:")
        for record in records[:5]:
            print(f"  Route {record['route_id']}, Stop {record['stop_id']}: {record['delay_seconds']}s delay")


if __name__ == "__main__":
    main()
