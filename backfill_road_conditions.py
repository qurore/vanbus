#!/usr/bin/env python3
"""
Backfill road conditions data from DriveBC historical CSV files.

DriveBC API only provides current ACTIVE events, so historical data
must be imported from CSV files available at:
https://catalogue.data.gov.bc.ca/dataset/bc-road-and-weather-conditions

Note: This script is provided for future use when historical CSV data
becomes available. Currently, the Lambda collector will build up
historical data over time by collecting active events periodically.
"""
import os
import csv
from datetime import datetime, timezone
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')


def parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse datetime string from CSV."""
    if not dt_str:
        return None
    try:
        # Try ISO format first
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except ValueError:
        pass
    try:
        # Try common CSV format
        return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    return None


def import_from_csv(csv_path: str) -> int:
    """Import road conditions from a CSV file."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    collected_at = datetime.now(timezone.utc)
    records = []

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Map CSV columns to database columns
            # Note: Column names may vary depending on the CSV source
            record = (
                row.get('event_id') or row.get('EVENT_ID'),
                row.get('status') or row.get('STATUS'),
                row.get('severity') or row.get('SEVERITY'),
                row.get('event_type') or row.get('EVENT_TYPE'),
                row.get('event_subtype') or row.get('EVENT_SUBTYPE'),
                row.get('headline') or row.get('HEADLINE'),
                row.get('description') or row.get('DESCRIPTION'),
                row.get('road_name') or row.get('ROAD_NAME'),
                row.get('direction') or row.get('DIRECTION'),
                float(row.get('lat') or row.get('LATITUDE') or 0) or None,
                float(row.get('lon') or row.get('LONGITUDE') or 0) or None,
                parse_datetime(row.get('created_at') or row.get('CREATED')),
                parse_datetime(row.get('updated_at') or row.get('UPDATED')),
                collected_at,
            )

            # Skip records without event_id
            if record[0]:
                records.append(record)

    if not records:
        print(f"No valid records found in {csv_path}")
        return 0

    try:
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

        execute_values(cur, sql, records, page_size=1000)
        conn.commit()

        return len(records)
    finally:
        cur.close()
        conn.close()


def main():
    import sys

    if len(sys.argv) < 2:
        print("Usage: python backfill_road_conditions.py <csv_file>")
        print()
        print("Import historical road conditions from a CSV file.")
        print("CSV should have columns: event_id, status, severity, event_type,")
        print("event_subtype, headline, description, road_name, direction,")
        print("lat, lon, created_at, updated_at")
        print()
        print("Historical data can be downloaded from:")
        print("https://catalogue.data.gov.bc.ca/dataset/bc-road-and-weather-conditions")
        return

    csv_path = sys.argv[1]

    if not os.path.exists(csv_path):
        print(f"Error: File not found: {csv_path}")
        return

    print(f"Importing road conditions from {csv_path}...")
    count = import_from_csv(csv_path)
    print(f"Imported {count} records")


if __name__ == "__main__":
    main()
