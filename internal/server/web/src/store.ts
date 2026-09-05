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
  sys: {
    rss_bytes: number;
    heap_bytes: number;
    host_mem_used_bytes: number;
    host_mem_total_bytes: number;
    host_cpu_percent: number;
    proc_cpu_percent: number;
    cpu_cores: number;
    db_bytes: number;
    disk_free_bytes: number;
    disk_total_bytes: number;
  };
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
  c: number[]; // host cpu %
  m: number[]; // process rss bytes
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
    cpu: [] as number[],     // host cpu %, seeded from hello.history.c
    rss: [] as number[],     // process rss bytes, seeded from hello.history.m
  },
};

const SERIES_CAP = 120;

function capPush(arr: number[], v: number): void {
  arr.push(v);
  if (arr.length > SERIES_CAP) arr.shift();
}

/** Called when the hello frame lands: graphs start full. */
export function seedHistory(h: History): void {
  state.series.ticks = [...h.t];
  state.series.fires = [...h.f];
  state.series.live = [...h.l];
  state.series.cpu = [...h.c];
  state.series.rss = [...h.m];
}

/** One 1s snapshot arrives → append the new sample points. */
export function pushSample(s: Snapshot): void {
  capPush(state.series.ticks, s.ticks_per_sec);
  capPush(state.series.fires, s.triggers_per_sec);
  capPush(state.series.live, s.engine.live);
  if (s.sys) {
    capPush(state.series.cpu, s.sys.host_cpu_percent);
    capPush(state.series.rss, s.sys.rss_bytes);
  }
}

/** One batched frame (oldest→newest) → prepend to the rail in order. */
export function pushTriggers(batch: Trigger[]): void {
  for (const tr of batch) {
    state.triggers.unshift(tr);
  }
  if (state.triggers.length > 10) state.triggers.length = 10;
}
