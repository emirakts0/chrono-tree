# Monitoring UI Design (bento dashboard, SSE)

Date: 2026-09-04
Status: approved (brainstorming session 2026-09-04)
Out of scope: JetStream persistence, TLS/auth, multi-node, browser automation tests.

## Goal

A live monitoring dashboard for chronod: bento-grid layout, sleek and modern,
served from chronod's existing HTTP listener (`:8080`), streaming system status
over SSE and showing every trigger the moment it fires. Read-only — alert
creation stays with `chronoctl`/gRPC — plus read-only alert inquiry (by id and
basic filtering) to the extent the existing service catalog permits.

## Decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | Where the UI lives | Embedded in chronod (`internal/server`), no new binary |
| 2 | Trigger feed source | Subscribe `chrono.triggers.>` on the publisher's existing nats.go connection (NATS loopback, not a tap inside `deliver`) |
| 3 | Frontend tech | No-build-step vanilla TypeScript; `tsc --noEmit` + `esbuild --bundle --minify --target=es2022`; `dist/app.js` committed, `go:embed`-ed (same pattern as committed proto stubs) |
| 4 | Streaming contract | Single multiplexed SSE endpoint `/api/stream` with typed frames |
| 5 | Interaction scope | Read-only monitoring + alert inquiry (`GET /api/alerts`, `GET /api/alerts/{id}`); no create/cancel from the UI |
| 6 | Visual direction | Bento grid on onyx canvas; palette below; frontend-design skill drives implementation polish |

Rationale for the NATS loopback (decision 2): a direct tap in `deliver` would
recreate the watcher fan-out machinery deleted in the NATS migration. The
loopback consumes the public wire contract, keeps `internal/service` free of
browser concerns, and payloads arrive already enriched. nats.go resubscribes
automatically on reconnect, so outages self-heal.

## Architecture & data flow

```
chronofeed ──gRPC──▶ service.Core ──▶ engine ──Triggers()──▶ pump ──▶ NATS (publish)
                                                                                        │
browser ◀──SSE── internal/server hub ◀── subscribe chrono.triggers.> ───────────────────┘
                (same nats.go connection the publisher owns — bidirectional)
```

Three components:

1. **`pub.NATSPublisher.SubscribeTriggers(fn func(Trigger)) error`** — concrete
   method on `NATSPublisher` (the `Publisher` interface stays
   `Publish/Connected/Close`). Subscribes `chrono.triggers.>`, unmarshals the
   JSON payload, calls `fn` on nats.go's reader goroutine. Unsubscribed/closed
   with the connection at shutdown.

2. **SSE hub in `internal/server`** (new file, ~100 lines): browsers register
   on connect; the `fn` above fans a `trigger` frame out (non-blocking send on
   a bounded per-client queue — 64 frames; overflow closes that client's
   connection, the browser reconnects); a 1s ticker builds a `snapshot` frame
   from `core` + `stats`. Slow browsers are dropped, never block the pump or
   nats.go.

3. **Read-only alert accessors on `service.Core`**: `GetAlert(id)` and
   `ListAlerts(filter)` returning copies under the existing `RLock`. The
   service `Alert` struct additionally records `ValidFrom` and `Expires`
   (present in the upsert request, currently forwarded to the engine and
   dropped) so the detail view is complete.

Wiring: `server.New` gains the hub; `cmd/chronod` calls `SubscribeTriggers`
after boot and feeds the hub. Nothing in the ingest path changes.

## HTTP + SSE contract

### `GET /api/stream` (SSE, one connection per tab)

Frames are `data: {json}\n\n`; `Content-Type: text/event-stream`,
`Cache-Control: no-cache`. The 1s snapshots double as the heartbeat; browsers
reconnect via `EventSource` semantics.

- **`hello`** (on connect): `{"type":"hello","venues":[...],"tiers":[...],"symbol_count":N}`
  — the reference-data vocabulary for rendering.
- **`snapshot`** (every 1s): the full `/stats` view — `uptime_sec`, `ticks`,
  `ticks_per_sec`, `ticks_dropped`, `triggers_fired`, `triggers_per_sec`,
  `triggers_published`, `triggers_publish_dropped`, `alerts_by_state`,
  `feed_ever_connected`, `feed_last_seen_ms_ago`, `nats_connected`,
  `venue_ticks`, `engine:{live,dropped_triggers}`.
- **`trigger`** (on NATS delivery): the `pub.Trigger` body as-is —
  `alert_id`, `symbol`, `venue`, `tier`, `fired_price` (decimal string),
  `direction` (`"ABOVE"|"BELOW"`), `target_price` (decimal string),
  `fired_at_unix_nanos`.

### Alert inquiry

- `GET /api/alerts?state=&symbol=&venue=&tier=&direction=&limit=&offset=`
  — filtered listing sorted `created_at` desc; default limit 50, max 200;
  response `{"total":N,"items":[...]}`.
- `GET /api/alerts/{id}` — full detail (id string form as returned by
  `UpsertAlert`) or 404.
- JSON via `encoding/json/v2`, like the rest of the status surface.

### `GET /`

Serves the embedded SPA from `go:embed web/dist`.

## Frontend design

### Bento layout (desktop; CSS grid, ~12 columns, ~14px gap on onyx canvas; single column below ~900px)

```
┌─────────────────────────────┬──────────────┬──────────────┐
│  PULSE (linen, 2×2)         │ FIRES        │ ALERT BOOK   │
│  tick rate, live sparkline, │ (brick-ember)│ (linen)      │
│  ticks total, dropped       │ fired/publish│ by state:    │
├──────────────┬──────────────┤ counters+rate│ 3 stat tiles │
│ VENUES       │ ENGINE       │ + drops      ├──────────────┤
│ (linen)      │ (linen)      ├──────────────┤              │
│ per-venue    │ live alerts, │              │              │
│ tick bars    │ ring drops   │              │              │
├──────────────┴──────────────┴──────────────┴──────────────┤
│  TRIGGER STREAM (pine-teal, full width, live rows)        │
├───────────────────────────────────────────────────────────┤
│  ALERT INQUIRY (linen, wide: filter bar + table + pager)  │
└───────────────────────────────────────────────────────────┘
```

Header strip above the grid: wordmark, then status cluster (feed dot, NATS
dot, uptime) — silver labels, `light-green` dots, `brick-ember` on failure.

### Palette (CSS custom properties in `:root`)

| Role | Token | Value | Usage |
|------|-------|-------|-------|
| Canvas (~30-35%) | `--onyx` | `#161515` | Page background framing all cards |
| | `--graphite` | `#2E2D29` | Alternative dark surface (inner dark panels) |
| Primary cards (~40-45%) | `--soft-linen` | `#F3EFE7` | Dominant card surface |
| | `--linen` | `#F4EEE5` | Inner panels on linen cards |
| Accent (~15%) | `--brick-ember` | `#D40000` | Fires card, ABOVE badges, failure dots |
| | `--pine-teal` | `#164D44` | Trigger stream card, inquiry primary buttons |
| Typography (~5-7%) | `--coffee-bean` | `#1E1917` | Text, 1px linework on linen |
| | `--silver` | `#BFB3AD` | Labels, metadata, inactive filters |
| | `--molten-lava` | `#770B0C` | Shadows/secondary text on red surfaces |
| Micro (~1-2%) | `--light-green` | `#7FDF74` | Status dots, live-pulse accents |

Contrast rule: `light-green` never sits directly on linen — it appears only
against `coffee-bean` or `pine-teal` chips (dots have a dark chip behind
them).

Sleek/modern mechanics: 16–18px card radii; tabular-figure monospace for all
metrics; soft diffuse shadows; ~150ms transitions; new trigger rows slide in;
sparklines drawn client-side (inline SVG, no chart library) from a
120-second ring buffer of snapshots. No external fonts or CDNs — fully
offline, single binary.

### TS structure (`internal/server/web/`)

- `index.html` — shell, mounts the app.
- `src/app.ts` — entry; `EventSource` connect/reconnect; wiring.
- `src/store.ts` — snapshot ring buffers (120 s), latest state.
- `src/api.ts` — alert inquiry fetches.
- `src/components/*.ts` — DOM builders, one per card.
- `src/style.css` — palette as `:root` custom properties, bento grid.
- `build.sh` — `tsc --noEmit` (type gate) then `esbuild --bundle --minify
  --target=es2022` to `dist/app.js` (CSS injected into the bundle; one embed
  artifact). `dist/app.js` is committed; the TS toolchain is only needed to
  regenerate it, exactly like `buf generate` for proto stubs.

The implementation plan MUST invoke the frontend-design skill for the
visual/CSS/layout work; this spec pins the palette, layout structure, and
behavior, and that skill executes the polish.

## Failure handling

- **Slow browser:** bounded 64-frame queue per client; overflow closes that
  connection; `EventSource` auto-reconnects. Never blocks NATS reader or pump.
- **NATS outage:** nats.go resubscribes on reconnect; `nats_connected:false`
  renders as a red status dot while snapshots keep flowing (feed and stats
  are independent of the subscription).
- **chronod shutdown:** existing `SetShuttingDown` + HTTP shutdown close SSE
  streams; the subscription dies with the connection.
- **No auth:** localhost ops tool, same posture as `/metrics` — documented,
  not gated.

## Testing

- `pub.SubscribeTriggers` round-trip against the embedded broker
  (`pubtest`): publish via `NATSPublisher`, receive via subscription.
- Hub unit tests: fan-out, slow-client close on overflow, clean unsubscribe.
- SSE endpoint via `httptest`: hello/snapshot/trigger frame shapes; snapshot
  cadence.
- `/api/alerts`: filters (state/symbol/venue/tier/direction), pagination
  (`total`, limit clamp), 404 detail; copy-safety under `-race`.
- `service.Core` accessors return copies (mutation isolation).
- Integration test extension: after the rate gate, `GET /` serves HTML with
  the embedded bundle, `GET /api/alerts/{id}` resolves the seeded alert.
- The UI itself ships without browser automation — API contract tests +
  `tsc --noEmit` gate + manual visual pass.
- Standard gates: `go build ./... && go vet ./...`, full `go test ./... -race
  -count=1`, `git diff --stat main -- engine price` empty.
