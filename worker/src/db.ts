/**
 * CockroachDB connection and operations
 */
import postgres from "postgres";
import type { DelayRecord } from "./gtfs-rt";

export async function saveRecords(
  databaseUrl: string,
  records: DelayRecord[]
): Promise<number> {
  if (records.length === 0) return 0;

  const sql = postgres(databaseUrl, {
    ssl: "require",
    max: 1,
    idle_timeout: 20,
    connect_timeout: 30,
  });

  try {
    // Batch insert using unnest for better performance
    const routeIds = records.map((r) => r.route_id);
    const stopIds = records.map((r) => r.stop_id);
    const tripIds = records.map((r) => r.trip_id);
    const delays = records.map((r) => r.delay_seconds);
    const vehicleIds = records.map((r) => r.vehicle_id);
    const timestamps = records.map((r) => r.recorded_at.toISOString());

    await sql`
      INSERT INTO bus_delays (route_id, stop_id, trip_id, delay_seconds, vehicle_id, recorded_at)
      SELECT * FROM unnest(
        ${routeIds}::text[],
        ${stopIds}::text[],
        ${tripIds}::text[],
        ${delays}::int[],
        ${vehicleIds}::text[],
        ${timestamps}::timestamptz[]
      )
    `;

    return records.length;
  } finally {
    await sql.end();
  }
}
