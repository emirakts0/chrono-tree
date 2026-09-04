import { state, tickSeries } from "../store";
import { fmt, fmtDuration, sparkline, statusDot } from "./cards";

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
      <span class="sep"></span> up <span id="uptime">—</span>
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
    <section class="card area-fires red"><h2>fires</h2>
      <div class="big mono" id="fired">—</div>
      <div class="sub">triggers fired</div>
      <div class="duo"><span class="mono" id="published">—</span> published
        · <span class="mono" id="pubdropped">—</span> dropped</div>
    </section>
    <section class="card area-book"><h2>alert book</h2>
      <div class="trio">
        <div class="tile"><div class="big mono" id="st-active">—</div><div class="sub">active</div></div>
        <div class="tile"><div class="big mono" id="st-triggered">—</div><div class="sub">triggered</div></div>
        <div class="tile"><div class="big mono" id="st-cancelled">—</div><div class="sub">cancelled</div></div>
      </div>
    </section>
    <section class="card area-venues"><h2>venues</h2><div id="venues" class="rows"></div></section>
    <section class="card area-engine"><h2>engine</h2><div class="rows" id="engine"></div></section>
    <section class="card area-stream teal"><h2>trigger stream</h2><div class="stream" id="stream">
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
  // The status dots are markup (a colored span), not text.
  const setDot = (id: string, ok: boolean) => {
    const el = document.getElementById(id);
    if (el) el.innerHTML = statusDot(ok);
  };
  setDot("dot-feed", s ? s.feed_ever_connected && s.feed_last_seen_ms_ago < 10_000 : false);
  setDot("dot-nats", !!s?.nats_connected);
  if (!s) return;
  set("uptime", fmtDuration(s.uptime_sec));
  set("tickrate", fmt(s.ticks_per_sec));
  set("ticks", fmt(s.ticks));
  set("ticksdropped", fmt(s.ticks_dropped));
  set("fired", fmt(s.triggers_fired));
  set("published", fmt(s.triggers_published));
  set("pubdropped", fmt(s.triggers_publish_dropped));
  set("st-active", String(s.alerts_by_state.active ?? 0));
  set("st-triggered", String(s.alerts_by_state.triggered ?? 0));
  set("st-cancelled", String(s.alerts_by_state.cancelled ?? 0));

  const spark = document.getElementById("spark");
  if (spark) spark.innerHTML = sparkline(tickSeries);

  const venues = document.getElementById("venues");
  if (venues) {
    const entries = Object.entries(s.venue_ticks).sort((a, b) => b[1] - a[1]);
    const max = entries[0]?.[1] || 1;
    venues.innerHTML = entries
      .map(([v, n]) => `<div class="row"><span>${v}</span>
        <span class="bar"><i style="width:${(100 * n) / max}%"></i></span>
        <span class="mono">${fmt(n)}</span></div>`)
      .join("") || `<div class="empty">no ticks yet</div>`;
  }
  const eng = document.getElementById("engine");
  if (eng) {
    eng.innerHTML = `<div class="row"><span>live alerts</span><span class="mono">${fmt(s.engine.live)}</span></div>
      <div class="row"><span>ring drops</span><span class="mono">${fmt(s.engine.dropped_triggers)}</span></div>
      <div class="row"><span>symbols</span><span class="mono">${state.hello ? fmt(state.hello.symbol_count) : "—"}</span></div>`;
  }

  // Re-render the stream only when a new trigger arrived; rebuilding it on
  // every 1s snapshot would replay the slide-in animation each second.
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
              .slice(0, 12)
              .map(
                (tr) => `<div class="trg">
                <span class="badge ${tr.direction === "ABOVE" ? "up" : "down"}">${tr.direction}</span>
                <span class="sym">${tr.symbol}</span>
                <span class="mono">${tr.fired_price}</span>
                <span class="meta">${tr.venue}/${tr.tier}</span>
                <span class="mono time">${new Date(tr.fired_at_unix_nanos / 1e6).toLocaleTimeString()}</span>
              </div>`
              )
              .join("");
    }
  }
}
