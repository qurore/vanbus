#!/usr/bin/env python3
"""
Export all CockroachDB tables to local CSV files.
"""

import os
import sys
from datetime import datetime

import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
OUTPUT_DIR = "db_export"

TABLES = [
    "bus_delays",
    "weather",
    "road_conditions",
    "routes",
    "trips",
    "calendar",
    "calendar_dates",
    "stop_times",
    "stops",
]


def export_table(conn, table_name: str, output_dir: str):
    """Export a single table to CSV."""
    filepath = os.path.join(output_dir, f"{table_name}.csv")
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        row_count = len(df)
        if row_count == 0:
            print(f"  {table_name}: empty (skipped)")
            return 0
        df.to_csv(filepath, index=False)
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        print(f"  {table_name}: {row_count:,} rows ({size_mb:.1f} MB)")
        return row_count
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        print(f"  {table_name}: table does not exist (skipped)")
        return 0
    except Exception as e:
        conn.rollback()
        print(f"  {table_name}: ERROR - {e}")
        return 0


def main():
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL not set in .env")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Connecting to CockroachDB...")
    conn = psycopg2.connect(DATABASE_URL)

    print(f"Exporting tables to {OUTPUT_DIR}/\n")
    total_rows = 0
    for table in TABLES:
        total_rows += export_table(conn, table, OUTPUT_DIR)

    conn.close()

    print(f"\nDone! Total: {total_rows:,} rows exported to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
