export interface Snapshot {
  uptime_sec: number;
  alerts_by_state: Record<string, number>;
  feed_ever_connected: boolean;
  feed_last_seen_ms_ago: number;
  nats_connected: boolean;
  venue_ticks: Record<string, number>;
  ticks: number;
  ticks_per_sec: number;
  ticks_dropped: number;
  triggers_fired: number;
  triggers_per_sec: number;
  triggers_published: number;
  triggers_publish_dropped: number;
  engine: { live: number; dropped_triggers: number };
}

export interface Trigger {
  alert_id: string;
  symbol: string;
  venue: string;
  tier: string;
  fired_price: string;
  fired_at_unix_nanos: number;
  direction: "ABOVE" | "BELOW";
  target_price: string;
}

/** Server-side 2-minute history from the hello frame (time-ascending). */
export interface History {
  t: number[]; // ticks/s
  f: number[]; // triggers/s
  l: number[]; // engine.live
  v: Record<string, number[]>; // per-venue ticks/s
}

export interface Hello {
  venues: string[];
  tiers: string[];
  symbol_count: number;
  history: History;
}

export const state = {
  hello: null as Hello | null,
  snapshot: null as Snapshot | null,
  triggers: [] as Trigger[], // newest first, capped at 10 (the rail)
  series: {
    ticks: [] as number[],   // ticks/s, seeded from hello.history.t
    fires: [] as number[],   // triggers/s, seeded from hello.history.f
    live: [] as number[],    // engine.live, seeded from hello.history.l
    venues: {} as Record<string, number[]>,
  },
};

const SERIES_CAP = 120;

function capPush(arr: number[], v: number): void {
  arr.push(v);
  if (arr.length > SERIES_CAP) arr.shift();
}

/** Called when the hello frame lands: graphs start full. Runs again on SSE
 * reconnect, so the venue diff base must reset or the first post-reconnect
 * venue sample would spike by the reconnect gap. */
export function seedHistory(h: History): void {
  for (const k in lastVenueTotal) delete lastVenueTotal[k];
  state.series.ticks = [...h.t];
  state.series.fires = [...h.f];
  state.series.live = [...h.l];
  state.series.venues = Object.fromEntries(
    Object.entries(h.v).map(([venue, s]) => [venue, [...s]]),
  );
}

// Venue rates need the cumulative counter diff, kept out of band:
const lastVenueTotal: Record<string, number> = {};
function pushVenueSample(venue: string, total: number): void {
  const arr = (state.series.venues[venue] ??= []);
  const rate = lastVenueTotal[venue] === undefined ? 0 : total - lastVenueTotal[venue];
  lastVenueTotal[venue] = total;
  capPush(arr, rate);
}

/** One 1s snapshot arrives → append the new sample points. */
export function pushSample(s: Snapshot): void {
  capPush(state.series.ticks, s.ticks_per_sec);
  capPush(state.series.fires, s.triggers_per_sec);
  capPush(state.series.live, s.engine.live);
  for (const [venue, total] of Object.entries(s.venue_ticks)) {
    pushVenueSample(venue, total);
  }
}

/** One batched frame (oldest→newest) → prepend to the rail in order. */
export function pushTriggers(batch: Trigger[]): void {
  for (const tr of batch) {
    state.triggers.unshift(tr);
  }
  if (state.triggers.length > 10) state.triggers.length = 10;
}
