#!/usr/bin/env python3
"""
Archive old bus delay data to Parquet and optionally delete from database.
"""

import os
import argparse
from datetime import datetime, timezone, timedelta

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')


def export_to_parquet(days_old: int, output_dir: str = "archives"):
    """Export records older than X days to Parquet."""
    os.makedirs(output_dir, exist_ok=True)

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{output_dir}/bus_delays_{timestamp_str}.parquet"

    conn = psycopg2.connect(DATABASE_URL)
    try:
        # Count records to export
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM bus_delays WHERE recorded_at < %s",
                (cutoff_date,)
            )
            count = cur.fetchone()[0]

        if count == 0:
            print(f"No records older than {days_old} days found.")
            return None, 0

        print(f"Exporting {count:,} records older than {days_old} days...")

        # Read data into pandas DataFrame
        query = """
            SELECT id, route_id, stop_id, trip_id, delay_seconds, vehicle_id, recorded_at
            FROM bus_delays
            WHERE recorded_at < %s
            ORDER BY recorded_at
        """
        df = pd.read_sql(query, conn, params=(cutoff_date,))

        # Save as Parquet (compressed)
        df.to_parquet(filename, compression='snappy', index=False)

        file_size_mb = os.path.getsize(filename) / (1024 * 1024)
        print(f"Exported to: {filename} ({file_size_mb:.1f} MB)")
        return filename, count
    finally:
        conn.close()


def delete_old_records(days_old: int):
    """Delete records older than X days from database."""
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM bus_delays WHERE recorded_at < %s",
                (cutoff_date,)
            )
            deleted = cur.rowcount
        conn.commit()
        print(f"Deleted {deleted:,} records from database.")
        return deleted
    finally:
        conn.close()


def get_stats():
    """Get current database statistics."""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bus_delays")
            total = cur.fetchone()[0]

            cur.execute("SELECT MIN(recorded_at), MAX(recorded_at) FROM bus_delays")
            min_date, max_date = cur.fetchone()

            print(f"Total records: {total:,}")
            print(f"Date range: {min_date} to {max_date}")
            return total, min_date, max_date
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Archive old bus delay data")
    parser.add_argument("--days", type=int, default=30, help="Archive records older than X days (default: 30)")
    parser.add_argument("--delete", action="store_true", help="Delete records after export")
    parser.add_argument("--stats", action="store_true", help="Show database statistics only")
    parser.add_argument("--output-dir", default="archives", help="Output directory for Parquet files")

    args = parser.parse_args()

    if args.stats:
        get_stats()
        return

    # Export
    filename, count = export_to_parquet(args.days, args.output_dir)

    if filename and args.delete:
        confirm = input(f"Delete {count:,} records from database? (yes/no): ")
        if confirm.lower() == "yes":
            delete_old_records(args.days)
        else:
            print("Deletion cancelled.")


if __name__ == "__main__":
    main()
