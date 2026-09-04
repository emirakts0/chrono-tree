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

export interface Hello {
  venues: string[];
  tiers: string[];
  symbol_count: number;
}

export const state = {
  hello: null as Hello | null,
  snapshot: null as Snapshot | null,
  triggers: [] as Trigger[], // newest first
};

/** Rolling ticks/s series for the pulse sparkline (120 samples = 2 min). */
export const tickSeries: number[] = [];

export function pushTrigger(tr: Trigger): void {
  state.triggers.unshift(tr);
  if (state.triggers.length > 200) state.triggers.pop();
}
