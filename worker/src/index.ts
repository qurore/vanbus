/**
 * TransLink Bus Delay Data Collector
 * Cloudflare Worker with scheduled cron trigger
 */
import { parseGtfsRealtime } from "./gtfs-rt";
import { saveRecords } from "./db";

export interface Env {
  TRANSLINK_API_KEY: string;
  DATABASE_URL: string;
  TRANSLINK_BASE_URL: string;
}

async function collectData(env: Env): Promise<string> {
  const startTime = Date.now();
  const collectedAt = new Date();

  // 1. Fetch GTFS-RT data from TransLink
  const url = `${env.TRANSLINK_BASE_URL}?apikey=${env.TRANSLINK_API_KEY}`;
  const response = await fetch(url, {
    headers: {
      Accept: "application/x-protobuf",
    },
  });

  if (!response.ok) {
    throw new Error(`TransLink API error: ${response.status} ${response.statusText}`);
  }

  const buffer = await response.arrayBuffer();

  // 2. Parse protobuf data
  const records = parseGtfsRealtime(buffer, collectedAt);

  if (records.length === 0) {
    return "No records found in feed";
  }

  // 3. Save to CockroachDB
  const savedCount = await saveRecords(env.DATABASE_URL, records);

  const duration = Date.now() - startTime;
  return `Saved ${savedCount} records in ${duration}ms`;
}

export default {
  // Scheduled handler (cron trigger)
  async scheduled(
    controller: ScheduledController,
    env: Env,
    ctx: ExecutionContext
  ): Promise<void> {
    console.log(`[${new Date().toISOString()}] Cron triggered`);

    try {
      const result = await collectData(env);
      console.log(`[${new Date().toISOString()}] ${result}`);
    } catch (error) {
      console.error(`[${new Date().toISOString()}] Error:`, error);
      throw error;
    }
  },

  // HTTP handler (for manual testing)
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext
  ): Promise<Response> {
    const url = new URL(request.url);

    // Health check
    if (url.pathname === "/health") {
      return new Response("OK", { status: 200 });
    }

    // Manual trigger (for testing)
    if (url.pathname === "/collect") {
      try {
        const result = await collectData(env);
        return new Response(JSON.stringify({ success: true, message: result }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown error";
        return new Response(JSON.stringify({ success: false, error: message }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        });
      }
    }

    return new Response("TransLink Bus Delay Collector\n\nEndpoints:\n- /health\n- /collect", {
      status: 200,
    });
  },
};
