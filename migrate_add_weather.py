#!/usr/bin/env python3
"""Add weather columns to bus_delays table."""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')

MIGRATION_SQL = """
ALTER TABLE bus_delays
ADD COLUMN IF NOT EXISTS temperature_c FLOAT,
ADD COLUMN IF NOT EXISTS humidity_percent INT,
ADD COLUMN IF NOT EXISTS wind_speed_kmh FLOAT,
ADD COLUMN IF NOT EXISTS wind_direction STRING,
ADD COLUMN IF NOT EXISTS pressure_kpa FLOAT,
ADD COLUMN IF NOT EXISTS visibility_km FLOAT;
"""

if __name__ == "__main__":
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        cur.execute(MIGRATION_SQL)
    conn.commit()
    conn.close()
    print("Migration completed: Added weather columns to bus_delays table.")
