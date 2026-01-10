#!/usr/bin/env python3
"""Import stops data from CSV to CockroachDB"""
import os
import csv
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ['DATABASE_URL']
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Create stops table
cur.execute('''
    CREATE TABLE IF NOT EXISTS stops (
        stop_id TEXT PRIMARY KEY,
        stop_code TEXT,
        stop_name TEXT,
        stop_lat DOUBLE PRECISION,
        stop_lon DOUBLE PRECISION,
        zone_id TEXT
    );
''')
conn.commit()
print('stops table created')

# Import data
with open('stops.csv', 'r') as f:
    reader = csv.DictReader(f)
    count = 0
    for row in reader:
        cur.execute('''
            INSERT INTO stops (stop_id, stop_code, stop_name, stop_lat, stop_lon, zone_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (stop_id) DO UPDATE SET
                stop_code = EXCLUDED.stop_code,
                stop_name = EXCLUDED.stop_name,
                stop_lat = EXCLUDED.stop_lat,
                stop_lon = EXCLUDED.stop_lon,
                zone_id = EXCLUDED.zone_id;
        ''', (row['stop_id'], row['stop_code'], row['stop_name'],
              float(row['stop_lat']), float(row['stop_lon']), row['zone_id']))
        count += 1

conn.commit()
print(f'{count} stops imported')

cur.close()
conn.close()
