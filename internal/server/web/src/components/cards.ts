export const fmt = (n: number): string =>
  n >= 1e9 ? `${(n / 1e9).toFixed(1)}B` : n >= 1e6 ? `${(n / 1e6).toFixed(1)}M` : n >= 1e4 ? `${(n / 1e3).toFixed(1)}k` : n.toLocaleString();

export function fmtDuration(sec: number): string {
  const h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60), s = Math.floor(sec % 60);
  return h > 0 ? `${h}h ${m}m` : m > 0 ? `${m}m ${s}s` : `${s}s`;
}

export function statusDot(ok: boolean): string {
  return `<span class="dot ${ok ? "on" : "off"}"></span>`;
}

/** Inline-SVG sparkline, no chart library. */
export function sparkline(series: number[]): string {
  if (series.length < 2) return "";
  const w = 100, h = 28;
  const max = Math.max(...series, 1);
  const pts = series.map((v, i) => `${(i / (series.length - 1)) * w},${h - (v / max) * h}`);
  return `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" role="img" aria-label="ticks per second, last two minutes">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" points="${pts.join(" ")}"/>
  </svg>`;
}
