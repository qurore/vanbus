#!/usr/bin/env python3
"""Create the bus_delays table in CockroachDB."""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bus_delays (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    route_id STRING,
    stop_id STRING,
    trip_id STRING,
    delay_seconds INT,
    vehicle_id STRING,
    recorded_at TIMESTAMPTZ DEFAULT now(),
    temperature_c FLOAT,
    humidity_percent INT,
    wind_speed_kmh FLOAT,
    wind_direction STRING,
    pressure_kpa FLOAT,
    visibility_km FLOAT,
    INDEX idx_route_id (route_id),
    INDEX idx_stop_id (stop_id),
    INDEX idx_recorded_at (recorded_at)
);
"""

if __name__ == "__main__":
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    conn.close()
    print("Table 'bus_delays' created successfully.")
