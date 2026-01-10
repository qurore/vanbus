#!/usr/bin/env python3
"""Import GTFS static data to CockroachDB"""
import os
import csv
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ['DATABASE_URL']
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

def create_tables():
    """Create all GTFS tables"""
    print("Creating tables...")

    # Routes table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS routes (
            route_id TEXT PRIMARY KEY,
            agency_id TEXT,
            route_short_name TEXT,
            route_long_name TEXT,
            route_type INT
        );
    ''')

    # Trips table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS trips (
            trip_id TEXT PRIMARY KEY,
            route_id TEXT,
            service_id TEXT,
            trip_headsign TEXT,
            direction_id INT,
            shape_id TEXT
        );
    ''')

    # Calendar table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS calendar (
            service_id TEXT PRIMARY KEY,
            monday INT,
            tuesday INT,
            wednesday INT,
            thursday INT,
            friday INT,
            saturday INT,
            sunday INT,
            start_date TEXT,
            end_date TEXT
        );
    ''')

    # Calendar dates (exceptions)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS calendar_dates (
            service_id TEXT,
            date TEXT,
            exception_type INT,
            PRIMARY KEY (service_id, date)
        );
    ''')

    # Stop times (scheduled arrivals)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS stop_times (
            trip_id TEXT,
            stop_id TEXT,
            arrival_time TEXT,
            departure_time TEXT,
            stop_sequence INT,
            PRIMARY KEY (trip_id, stop_sequence)
        );
    ''')

    conn.commit()
    print("Tables created.")

def import_routes():
    """Import routes.txt"""
    print("Importing routes...")
    cur.execute("DELETE FROM routes;")

    with open('gtfs_data/routes.txt', 'r') as f:
        reader = csv.DictReader(f)
        data = [(r['route_id'], r['agency_id'], r['route_short_name'],
                 r['route_long_name'], int(r['route_type']) if r['route_type'] else None)
                for r in reader]

    execute_values(cur, '''
        INSERT INTO routes (route_id, agency_id, route_short_name, route_long_name, route_type)
        VALUES %s
    ''', data)
    conn.commit()
    print(f"  {len(data)} routes imported.")

def import_trips():
    """Import trips.txt"""
    print("Importing trips...")
    cur.execute("DELETE FROM trips;")

    with open('gtfs_data/trips.txt', 'r') as f:
        reader = csv.DictReader(f)
        data = [(r['trip_id'], r['route_id'], r['service_id'], r['trip_headsign'],
                 int(r['direction_id']) if r['direction_id'] else None, r['shape_id'])
                for r in reader]

    # Batch insert
    batch_size = 10000
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        execute_values(cur, '''
            INSERT INTO trips (trip_id, route_id, service_id, trip_headsign, direction_id, shape_id)
            VALUES %s
        ''', batch)
        conn.commit()

    print(f"  {len(data)} trips imported.")

def import_calendar():
    """Import calendar.txt"""
    print("Importing calendar...")
    cur.execute("DELETE FROM calendar;")

    with open('gtfs_data/calendar.txt', 'r') as f:
        reader = csv.DictReader(f)
        data = [(r['service_id'], int(r['monday']), int(r['tuesday']), int(r['wednesday']),
                 int(r['thursday']), int(r['friday']), int(r['saturday']), int(r['sunday']),
                 r['start_date'], r['end_date'])
                for r in reader]

    execute_values(cur, '''
        INSERT INTO calendar (service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date)
        VALUES %s
    ''', data)
    conn.commit()
    print(f"  {len(data)} calendar entries imported.")

def import_calendar_dates():
    """Import calendar_dates.txt"""
    print("Importing calendar_dates...")
    cur.execute("DELETE FROM calendar_dates;")

    with open('gtfs_data/calendar_dates.txt', 'r') as f:
        reader = csv.DictReader(f)
        data = [(r['service_id'], r['date'], int(r['exception_type'])) for r in reader]

    execute_values(cur, '''
        INSERT INTO calendar_dates (service_id, date, exception_type)
        VALUES %s
    ''', data)
    conn.commit()
    print(f"  {len(data)} calendar date exceptions imported.")

def import_stop_times():
    """Import stop_times.txt (large file, batch processing)"""
    print("Importing stop_times (this may take a while)...")
    cur.execute("DELETE FROM stop_times;")
    conn.commit()

    batch_size = 50000
    batch = []
    total = 0

    with open('gtfs_data/stop_times.txt', 'r') as f:
        reader = csv.DictReader(f)
        for r in reader:
            batch.append((r['trip_id'], r['stop_id'], r['arrival_time'].strip(),
                         r['departure_time'].strip(), int(r['stop_sequence'])))

            if len(batch) >= batch_size:
                execute_values(cur, '''
                    INSERT INTO stop_times (trip_id, stop_id, arrival_time, departure_time, stop_sequence)
                    VALUES %s
                ''', batch)
                conn.commit()
                total += len(batch)
                print(f"  {total:,} records processed...")
                batch = []

    # Insert remaining
    if batch:
        execute_values(cur, '''
            INSERT INTO stop_times (trip_id, stop_id, arrival_time, departure_time, stop_sequence)
            VALUES %s
        ''', batch)
        conn.commit()
        total += len(batch)

    print(f"  {total:,} stop_times imported.")

if __name__ == '__main__':
    create_tables()
    import_routes()
    import_trips()
    import_calendar()
    import_calendar_dates()
    import_stop_times()

    cur.close()
    conn.close()
    print("\nAll GTFS data imported successfully!")
