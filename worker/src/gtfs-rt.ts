/**
 * GTFS-RT Parser for TransLink data
 */
import GtfsRealtimeBindings from "gtfs-realtime-bindings";

export interface DelayRecord {
  route_id: string | null;
  stop_id: string | null;
  trip_id: string | null;
  delay_seconds: number;
  vehicle_id: string | null;
  recorded_at: Date;
}

export function parseGtfsRealtime(buffer: ArrayBuffer): DelayRecord[] {
  const feed = GtfsRealtimeBindings.transit_realtime.FeedMessage.decode(
    new Uint8Array(buffer)
  );

  const records: DelayRecord[] = [];

  // Use feed header timestamp
  const timestamp = feed.header?.timestamp
    ? new Date(Number(feed.header.timestamp) * 1000)
    : new Date();

  for (const entity of feed.entity) {
    if (!entity.tripUpdate) continue;

    const tripUpdate = entity.tripUpdate;
    const tripId = tripUpdate.trip?.tripId || null;
    const routeId = tripUpdate.trip?.routeId || null;
    const vehicleId = tripUpdate.vehicle?.id || null;

    for (const stopTimeUpdate of tripUpdate.stopTimeUpdate || []) {
      const stopId = stopTimeUpdate.stopId || null;

      // Get delay from arrival or departure
      let delaySeconds: number | null = null;

      if (stopTimeUpdate.arrival?.delay !== undefined && stopTimeUpdate.arrival?.delay !== null) {
        delaySeconds = stopTimeUpdate.arrival.delay;
      } else if (stopTimeUpdate.departure?.delay !== undefined && stopTimeUpdate.departure?.delay !== null) {
        delaySeconds = stopTimeUpdate.departure.delay;
      }

      if (delaySeconds !== null && stopId) {
        records.push({
          route_id: routeId,
          stop_id: stopId,
          trip_id: tripId,
          delay_seconds: delaySeconds,
          vehicle_id: vehicleId,
          recorded_at: timestamp,
        });
      }
    }
  }

  return records;
}
