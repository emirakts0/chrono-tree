import { state } from "../store";
import { fmt, fmtDuration, sparkline, statusDot, trend, trendArrow, trendDir } from "./cards";

// Small inline-SVG glyphs for the alert book tiles (no icon libraries).
const GLYPHS: Record<string, string> = {
  active: `<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="3.5" fill="none" stroke="currentColor" stroke-width="2"/><circle class="pulse" cx="8" cy="8" r="6.5" fill="none" stroke="currentColor" stroke-width="1"/></svg>`,
  triggered: `<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M9.5 1 3 9.2h3.6L6 15l6.3-8.2H8.7L9.5 1z" fill="currentColor"/></svg>`,
  cancelled: `<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="5.5" fill="none" stroke="currentColor" stroke-width="1.5"/><line x1="3.4" y1="12.6" x2="12.6" y2="3.4" stroke="currentColor" stroke-width="1.5"/></svg>`,
};

export interface Mounts {
  inquiry: HTMLElement;
}

export function mount(root: HTMLElement): Mounts {
  root.innerHTML = `
  <header class="topbar">
    <span class="brand">chrono<span class="brand-dot">▸</span></span>
    <span class="status mono">
      <span id="dot-feed"></span> feed
      <span id="dot-nats"></span> nats
      <span class="sep"></span>
      <span class="mono" id="published">—</span> pub
      · <span class="mono" id="pubdropped">—</span> drop
    </span>
  </header>
  <main class="grid">
    <section class="card area-pulse"><h2>pulse</h2>
      <div class="big mono" id="tickrate">—</div>
      <div class="sub">ticks/s</div>
      <div id="spark"></div>
      <div class="duo"><span class="mono" id="ticks">—</span> ticks
        · <span class="mono" id="ticksdropped">—</span> dropped</div>
    </section>
    <section class="card area-book"><h2>alert book</h2>
      <div class="trio">
        <div class="tile t-active"><span class="glyph">${GLYPHS.active}</span><div class="big mono" id="st-active">—</div><div class="sub">active</div><div class="tilespark" id="livetrendline"></div></div>
        <div class="tile t-triggered"><span class="glyph">${GLYPHS.triggered}</span><div class="big mono" id="st-triggered">—</div><div class="sub">triggered</div><div class="tilespark" id="firesspark"></div></div>
        <div class="tile t-cancelled"><span class="glyph">${GLYPHS.cancelled}</span><div class="big mono" id="st-cancelled">—</div><div class="sub">cancelled</div></div>
      </div>
    </section>
    <section class="card area-venues"><h2>venues</h2><div id="venues" class="rows"></div></section>
    <section class="card area-engine"><h2>engine</h2><div id="engine" class="rows"></div></section>
    <section class="card area-stream rail"><h2>trigger stream</h2><div class="stream" id="stream">
      <div class="empty">waiting for the first trigger…</div></div></section>
    <section class="card area-inquiry" id="inquiry"></section>
  </main>`;
  return { inquiry: document.getElementById("inquiry")! };
}

export function update(): void {
  const s = state.snapshot;
  const set = (id: string, v: string) => {
    const el = document.getElementById(id);
    if (el) el.textContent = v;
  };
  const setDot = (id: string, ok: boolean) => {
    const el = document.getElementById(id);
    if (el) el.innerHTML = statusDot(ok);
  };
  setDot("dot-feed", s ? s.feed_ever_connected && s.feed_last_seen_ms_ago < 10_000 : false);
  setDot("dot-nats", !!s?.nats_connected);
  if (!s) return;

  // Topbar owns transport health (published/dropped) — nowhere else.
  set("published", fmt(s.triggers_published));
  set("pubdropped", fmt(s.triggers_publish_dropped));

  // Pulse owns tick volume.
  set("tickrate", fmt(s.ticks_per_sec));
  set("ticks", fmt(s.ticks));
  set("ticksdropped", fmt(s.ticks_dropped));
  const spark = document.getElementById("spark");
  if (spark) spark.innerHTML = sparkline(state.series.ticks);

  // Alert book owns the state tiles; each populated tile carries its own
  // trend: the triggered tile the fires rate, the active (live-count) tile
  // the engine.live trend.
  set("st-active", String(s.alerts_by_state.active ?? 0));
  set("st-triggered", String(s.alerts_by_state.triggered ?? 0));
  set("st-cancelled", String(s.alerts_by_state.cancelled ?? 0));
  const fspark = document.getElementById("firesspark");
  if (fspark) fspark.innerHTML = trend(state.series.fires);
  const lt = document.getElementById("livetrendline");
  if (lt) lt.innerHTML = trend(state.series.live);

  const venues = document.getElementById("venues");
  if (venues) {
    const entries = Object.entries(s.venue_ticks).sort((a, b) => b[1] - a[1]);
    const max = entries[0]?.[1] || 1;
    // Rate marker: the venue's current ticks/s against the busiest venue's.
    const rate = (v: string): number => (state.series.venues[v] ?? []).slice(-1)[0] ?? 0;
    const maxRate = Math.max(1, ...entries.map(([v]) => rate(v)));
    venues.innerHTML =
      entries
        .map(([v, n]) => {
          const series = state.series.venues[v] ?? [];
          return `<div class="row">
          <span class="vname">${v} <span class="trendmark ${trendDir(series)}">${trendArrow(series)}</span></span>
          <span class="vmeta">${trend(series.slice(-40))}</span>
          <span class="bar"><i style="width:${(100 * n) / max}%"></i><i class="mark" style="left:${(100 * rate(v)) / maxRate}%"></i></span>
          <span class="mono">${fmt(n)}</span>
        </div>`;
        })
        .join("") || `<div class="empty">no ticks yet</div>`;
  }

  // Engine card: engine-internal health only. Uptime lives here now.
  const eng = document.getElementById("engine");
  if (eng) {
    eng.innerHTML = `
      <div class="row"><span>ring drops</span><span class="mono">${fmt(s.engine.dropped_triggers)}</span></div>
      <div class="row"><span>symbols</span><span class="mono">${state.hello ? fmt(state.hello.symbol_count) : "—"}</span></div>
      <div class="row"><span>uptime</span><span class="mono">${fmtDuration(s.uptime_sec)}</span></div>`;
  }

  // The rail re-renders only when its content changed (keyed), so the
  // slide-in does not replay on every snapshot.
  const stream = document.getElementById("stream");
  if (stream) {
    const head = state.triggers[0];
    const key = head ? `${state.triggers.length}:${head.alert_id}:${head.fired_at_unix_nanos}` : "";
    if (key !== stream.dataset.key) {
      stream.dataset.key = key;
      stream.innerHTML =
        state.triggers.length === 0
          ? `<div class="empty">waiting for the first trigger…</div>`
          : state.triggers
              .map(
                (tr) => `<div class="trg">
                <span class="badge ${tr.direction === "ABOVE" ? "up" : "down"}">${tr.direction}</span>
                <span class="sym">${tr.symbol}</span>
                <span class="mono">${tr.fired_price}</span>
                <span class="meta">${tr.venue}/${tr.tier}</span>
                <span class="mono time">${new Date(tr.fired_at_unix_nanos / 1e6).toLocaleTimeString()}</span>
              </div>`,
              )
              .join("");
    }
  }
}
