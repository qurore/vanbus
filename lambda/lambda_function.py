"""
TransLink Bus Delay Data Collector - AWS Lambda
Fetches GTFS-RT data and stores in CockroachDB
"""
import os
import json
import ssl
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from google.transit import gtfs_realtime_pb2
import pg8000


def fetch_gtfs_rt(api_key: str) -> bytes:
    """Fetch GTFS-RT data from TransLink API"""
    url = f"https://gtfsapi.translink.ca/v3/gtfsrealtime?apikey={api_key}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.content


def parse_gtfs_rt(data: bytes, collected_at: datetime) -> list:
    """Parse GTFS-RT protobuf data"""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)

    records = []

    # Use feed header timestamp
    recorded_at = datetime.fromtimestamp(feed.header.timestamp, tz=timezone.utc)

    for entity in feed.entity:
        if not entity.HasField('trip_update'):
            continue

        trip_update = entity.trip_update
        trip_id = trip_update.trip.trip_id if trip_update.trip.HasField('trip_id') else None
        route_id = trip_update.trip.route_id if trip_update.trip.HasField('route_id') else None
        vehicle_id = trip_update.vehicle.id if trip_update.HasField('vehicle') and trip_update.vehicle.HasField('id') else None

        for stop_time_update in trip_update.stop_time_update:
            stop_id = stop_time_update.stop_id if stop_time_update.HasField('stop_id') else None

            delay_seconds = None
            if stop_time_update.HasField('arrival') and stop_time_update.arrival.HasField('delay'):
                delay_seconds = stop_time_update.arrival.delay
            elif stop_time_update.HasField('departure') and stop_time_update.departure.HasField('delay'):
                delay_seconds = stop_time_update.departure.delay

            if delay_seconds is not None and stop_id:
                records.append({
                    'route_id': route_id,
                    'stop_id': stop_id,
                    'trip_id': trip_id,
                    'delay_seconds': delay_seconds,
                    'vehicle_id': vehicle_id,
                    'recorded_at': recorded_at,
                    'collected_at': collected_at,
                })

    return records


def parse_database_url(database_url: str):
    """Parse DATABASE_URL into pg8000 connection parameters"""
    parsed = urlparse(database_url)
    return {
        'host': parsed.hostname,
        'port': parsed.port or 26257,
        'user': parsed.username,
        'password': parsed.password,
        'database': parsed.path.lstrip('/').split('?')[0],
    }


def save_to_db(database_url: str, records: list) -> int:
    """Save records to CockroachDB using pg8000"""
    if not records:
        return 0

    params = parse_database_url(database_url)

    # Create SSL context for CockroachDB
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

    try:
        # Batch insert using unnest for better performance
        route_ids = [r['route_id'] for r in records]
        stop_ids = [r['stop_id'] for r in records]
        trip_ids = [r['trip_id'] for r in records]
        delays = [r['delay_seconds'] for r in records]
        vehicle_ids = [r['vehicle_id'] for r in records]
        recorded_at_times = [r['recorded_at'].isoformat() for r in records]
        collected_at_times = [r['collected_at'].isoformat() for r in records]

        cur.execute('''
            INSERT INTO bus_delays (route_id, stop_id, trip_id, delay_seconds, vehicle_id, recorded_at, collected_at)
            SELECT * FROM unnest(
                %s::text[],
                %s::text[],
                %s::text[],
                %s::int[],
                %s::text[],
                %s::timestamptz[],
                %s::timestamptz[]
            )
        ''', (route_ids, stop_ids, trip_ids, delays, vehicle_ids, recorded_at_times, collected_at_times))

        conn.commit()
        return len(records)
    finally:
        cur.close()
        conn.close()


def lambda_handler(event, context):
    """AWS Lambda entry point"""
    start_time = datetime.now(timezone.utc)
    collected_at = start_time

    # Get environment variables
    api_key = os.environ.get('TRANSLINK_API_KEY')
    database_url = os.environ.get('DATABASE_URL')

    if not api_key or not database_url:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Missing environment variables'})
        }

    try:
        # 1. Fetch GTFS-RT data
        data = fetch_gtfs_rt(api_key)

        # 2. Parse protobuf
        records = parse_gtfs_rt(data, collected_at)

        if not records:
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No records found in feed'})
            }

        # 3. Save to database
        saved_count = save_to_db(database_url, records)

        duration_ms = int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'message': f'Saved {saved_count} records in {duration_ms}ms'
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
