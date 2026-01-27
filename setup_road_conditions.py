#!/usr/bin/env python3
"""
Setup road_conditions table in CockroachDB for DriveBC event data.
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')


def setup_table():
    """Create the road_conditions table if it doesn't exist."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS road_conditions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                event_id STRING NOT NULL,
                status STRING,
                severity STRING,
                event_type STRING,
                event_subtype STRING,
                headline STRING,
                description TEXT,
                road_name STRING,
                direction STRING,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ,
                collected_at TIMESTAMPTZ,
                UNIQUE (event_id, updated_at)
            );
        ''')

        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_road_conditions_event_type
            ON road_conditions (event_type);
        ''')

        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_road_conditions_created_at
            ON road_conditions (created_at);
        ''')

        cur.execute('''
            CREATE INDEX IF NOT EXISTS idx_road_conditions_location
            ON road_conditions (lat, lon);
        ''')

        conn.commit()
        print("road_conditions table created successfully!")

        # Verify table structure
        cur.execute('''
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'road_conditions'
            ORDER BY ordinal_position;
        ''')
        print("\nTable structure:")
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]}")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    setup_table()
