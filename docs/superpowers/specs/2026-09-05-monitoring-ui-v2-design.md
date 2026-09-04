# Monitoring UI v2 — Design

Date: 2026-09-05
Spec for: iteration on the monitoring dashboard shipped in `feat/monitoring-ui` (merged at 5c0da58)

## Goal

Three improvements to the live dashboard:

1. **Protective trigger buffer** — server-side 10-slot ring, drained once per
   second; a 10,000-alarm burst ships only the last 10, batched.
2. **Service-side history** — 2 minutes of per-second samples for selected
   metrics, delivered on connect so graphs are full at page load.
3. **Visual overhaul** — vertical trigger rail, card interior redesign,
   dedup of information, legible text colors, custom dropdowns, load
   intro animation, responsive breakpoints.

## Decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | Trigger batch frame shape | Separate `triggers` frame on the same 1s tick; `snapshot` stays byte-identical to the `/stats` document |
| 2 | History scope | `ticks_per_sec`, `triggers_per_sec`, `engine.live`, per-venue `ticks/s`; 120 samples (2 min), in the `hello` frame |
| 3 | Backend structure | One server-wide 1s tick goroutine sampling history, building one snapshot, draining the ring, broadcasting both frames through the existing hub; `handleStream` becomes a pure relay |
| 4 | Fires card | `pine-teal` base, `molten-lava` glow + brick-ember details, low-opacity hatch pattern; rate sparkline in `light-green` |
| 5 | Text legibility | `silver` banned from light (linen) surfaces; labels there use coffee-bean at 60–70% opacity; silver reserved for dark surfaces |
| 6 | Dropdowns | Small custom TS dropdown component (button + popover, keyboard + outside-click), replaces native `<select>` in inquiry filters |

## Architecture

### The 1-second engine

`internal/server` gains a single background tick goroutine started by `New`
and stopped by a new `Server.Close()` (idempotent, closes an internal stop
channel). chronod calls it in its shutdown path, after `httpServer.Shutdown`
returns — the goroutine's exit no longer depends on connection lifetimes.
Every second, in order:

1. **Sample history** — append `ticks_per_sec`, `triggers_per_sec`,
   `engine.live`, and per-venue `ticks/s` (delta of cumulative `venue_ticks`
   between samples) into fixed 120-slot arrays.
2. **Build the snapshot** — call the existing `statusSnapshot()` exactly once.
3. **Drain the trigger ring** — take up to 10 accumulated triggers, clear it.

Then broadcast the `snapshot` frame through the hub, followed by a
`triggers` frame **only if the ring was non-empty**. Every connection sees
identical frames; the hub's 64-frame overflow logic now guards every frame
type (at ≤2 frames/sec it will essentially never fire).

`handleStream` becomes a dumb relay: write `hello` (with history) on
connect, then forward hub frames until disconnect. Per-connection tickers
and per-connection snapshot builds are removed.

### Trigger ring

A 10-slot ring in `Server`, mutex-guarded. `HandleTrigger` overwrites the
oldest slot when full. Drain returns the batch oldest→newest and clears.
10,000 alarms in one second → exactly the last 10 ship.

### SSE contract (changed)

- `data: {"type":"hello","venues":[...],"tiers":[...],"symbol_count":N,"history":{"t":[...],"f":[...],"l":[...],"v":{"<VENUE>":[...]}}}` — on connect. `history` keys are short (480 numbers per hello); arrays are time-ascending, right-padded as they fill after boot.
- `data: {"type":"snapshot","snapshot":{...}}` — every 1s, byte-equivalent to `/stats`.
- `data: {"type":"triggers","triggers":[...]}` — right after the snapshot, only when the drained ring was non-empty; each item is the existing `pub.Trigger` JSON body.
- Per-trigger `{"type":"trigger",...}` frames are **removed**.

### What does not change

`pub.SubscribeTriggers`, the `Publisher` interface, `service.Core`, the
inquiry endpoints, `/stats`, `/healthz`, `/metrics`. `engine/` and `price/`
remain frozen (zero diffs vs main). The dashboard stays read-only.

## UI

### Layout

The trigger stream becomes a tall vertical rail on the right, spanning the
top three rows:

```
┌─ pulse ────────┬─ fires ────────┬─ alert book ───┬─ stream ──┐
│  big ticks/s   │  big fired     │  3 state tiles │ │ trigger 1 │
│  sparkline     │  rate spark    │  + live trend  │ │ trigger 2 │
│                │                │                │ │  …(last10)│
├─ venues ───────┴─ engine ───────┴────────────────┤ │          │
│  rows + per-venue mini trends                    │ │          │
├─ alert inquiry (full width) ──────────────────────┴──────────┤
```

### Information dedup — each datum appears exactly once

| Datum | Lives on |
|---|---|
| ticks/s (big + sparkline), ticks total, ticks dropped | pulse |
| triggers fired (big), triggers/s (sparkline only) | fires |
| published / publish-dropped counters | topbar, next to the nats dot |
| alert state tiles (active/triggered/cancelled), engine.live trend | alert book |
| engine ring drops, symbols, uptime | engine (uptime moves here from topbar) |
| feed/nats status dots | topbar |
| per-venue share bar + mini trend | venues |

### Card interiors

- Alert book tiles: SVG glyphs + colored progress fills; thin `engine.live`
  trend line at the bottom.
- Engine rows: keyline dividers, aligned mono values, chip-styled labels.
- Venue rows: rounded bar tracks (share of volume) + light-green marker at
  the current 1s rate from history.
- Trigger rail rows: badge · symbol · price · venue/time, newest on top,
  slide-in animation (as today).

### Visual language

- **Fires card:** `pine-teal` base; `molten-lava` radial glow behind the big
  number; thin `brick-ember` top keyline; `light-green` sparkline (allowed:
  on pine-teal); ~4%-opacity `molten-lava` diagonal hatch
  (`repeating-linear-gradient`) as the soft pattern.
- **Trigger rail:** switches from teal to a `graphite`/onyx dark card with
  `light-green` time markers (so it doesn't compete with the fires card).
- **Legibility rule:** `silver` (#BFB3AD) never on light surfaces — labels
  on linen cards use `coffee-bean` at 60–70% opacity (≈ #6B615B, ~5:1
  contrast). `silver` and `soft-linen` remain for dark surfaces.
- **Dropdowns:** custom component — styled button (current value + chevron),
  popover list, keyboard (arrows/Esc) + click, closes on outside-click;
  reused for state/venue/tier/direction; symbol input styled to match.
  ~80 lines of TS, no libraries.

### Intro animation

On load: full-screen `onyx` overlay, `chrono` wordmark in `soft-linen`
centered, green ▸ pulsing. Over ~1.4s the wordmark drifts up-left to its
topbar position while the overlay fades and the grid blooms in beneath
(staggered card fade/translate). One-shot; `prefers-reduced-motion` skips
straight to the page.

### Responsive

- **>1100px:** 12-column grid with the right rail.
- **≤1100px:** rail drops below the top cards as a horizontal card (still
  last-10).
- **≤720px:** single column; trigger rows compress to badge · symbol ·
  price; inquiry table scrolls horizontally.

## Failure behavior

- A client connecting mid-second waits ≤1s for its first snapshot; `hello`
  (with history) goes out immediately, so the page is never blank.
- Slow clients are dropped by the hub exactly as today (bounded 64-frame
  queues, non-blocking senders).
- The tick goroutine has no external I/O beyond the hub; no extra failure
  modes to handle.

## Testing

- **Ring:** >10 pushes via `HandleTrigger` → drain returns exactly the last
  10, oldest→newest; concurrent push + drain under `-race`.
- **Tick engine** (`testing/synctest`, already used in this package): 1s
  advance → one snapshot broadcast; pre-loaded ring → `triggers` frame
  follows; empty ring → no `triggers` frame; many ticks → history caps at
  120, per-venue deltas correct.
- **Hello:** contains `history` with all four series; snapshot frame remains
  byte-equivalent to `/stats`.
- Existing `TestStreamHelloSnapshotTrigger` rewritten for the batched
  contract; `TestHubOverflowClosesSlowClient` unchanged.
- **Frontend:** `tsc --noEmit` (strict) is a hard gate; the custom dropdown
  is exercised manually in the running app (no JS test harness in this
  repo; adding one is out of scope).
- **Gates per task:** `go build ./... && go vet ./...`, package tests
  `-race`, `git diff --stat main -- engine price` empty, bundle rebuilt via
  `build.sh` and committed (byte-identical on no-op rebuild), integration
  test green.
