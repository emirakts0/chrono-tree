# chrono-tree Service Layer — gRPC Daemon, Feed & Demo Client Design

**Date:** 2026-09-04
**Status:** Approved (brainstorming complete)
**Module:** `github.com/emir/chrono-tree` (new packages only)
**Depends on:** `2026-09-02-chrono-tree-design.md` (Phase A, merged), `2026-09-03-dims-8slot-design.md` (merged)

## 1. Purpose

Build the service layer on top of the engine: a real gRPC daemon (`chronod`)
hosting the matching engine with HTTP status endpoints, a crypto market-data
simulator (`chronofeed`) streaming high-volume dummy rates into it, and a demo
client (`chronoctl`) that registers alerts and observes triggers. The engine
and price packages are **frozen, untouched** — the service consumes them as-is.

## 2. Decisions

| Question | Decision |
|---|---|
| Branch base | `main` at 9c7bb51 (dims merged); engine/price unchanged |
| Topology | Three binaries: `chronod`, `chronofeed` (gRPC client of chronod), `chronoctl` |
| RPC framework | Canonical `google.golang.org/grpc`, separate listener from HTTP |
| Trigger delivery | Server-streaming `WatchTriggers` only; fan-out from a single pump |
| Feed protocol | Client-streaming `StreamTicks(stream TickBatch)`, batches ≤ 64 |
| Feed scale | ~500 pairs, configurable rate defaulting to 20k ticks/s, bursty per symbol |
| Market | Crypto: majors with realistic anchors + sub-cent memecoins (8 decimals) |
| Dims | Width 2: `venue` ∈ {ATLAS, NOVA, ZENITH} × `tier` ∈ {TOP, MID} |
| Status surface | JSON endpoints only: `/healthz` `/readyz` `/stats` `/metrics` (Prometheus) |
| Prices on the wire | Decimal strings (`"65000.12"`), exact conversion via `price` package |
| Persistence | None — engine is volatile by design; no WAL |
| Config | Flags only; catalogs are compiled-in Go tables |

## 3. Layout

```
cmd/chronod/       — engine host: gRPC :9090 + HTTP :8080
cmd/chronofeed/    — crypto market simulator, gRPC client
cmd/chronoctl/     — demo client (alert, watch, demo subcommands)
api/proto/chrono/v1/ — service definitions (buf-managed)
api/gen/           — generated Go (protoc-gen-go + protoc-gen-go-grpc)
internal/service/  — gRPC service impl, trigger pump, alert catalog, error mapping
internal/catalog/  — symbol → decimals table, dim vocabulary (shared, compiled-in)
internal/feed/     — market simulator (random walk, spreads, emission shaping)
internal/server/   — HTTP status mux, stats snapshot
internal/stats/    — atomic counters package
```

## 4. gRPC API (`chrono.v1`)

```proto
service AlertService {
  rpc UpsertAlert(UpsertAlertRequest) returns (UpsertAlertResponse);
  rpc CancelAlert(CancelAlertRequest) returns (CancelAlertResponse);
  rpc WatchTriggers(WatchTriggersRequest) returns (stream Trigger);
}
service FeedService {
  rpc StreamTicks(stream TickBatch) returns (FeedStatus);
  rpc GetCatalog(GetCatalogRequest) returns (Catalog);
}
```

- `Trigger` fields: alert ID (UUID string), symbol, fired price (decimal
  string), venue name, tier name, timestamp. Dim values are resolved to names
  via the catalog before fan-out.
- `GetCatalog` returns pairs (with decimals) and the dim vocabulary — the way
  clients learn valid values without out-of-band knowledge.
- `grpc_health_v1` registered; `chronoctl` and `chronofeed` health-check on
  connect. Keepalive: server parameters with `MaxConnectionAge`, standard
  enforcement policy.

### chronod internals

A `service.Core` wraps `engine.Engine` and owns:

- **Dim vocabulary** — string↔uint16 interning built from config:
  `venue: [ATLAS, NOVA, ZENITH]`, `tier: [TOP, MID]`; engine configured with
  `Config.Dims{"venue", "tier"}`.
- **Symbol catalog** — pair → decimals (8 for spot pairs; `int64` at 8
  decimals caps at ~92 billion, and `price.Parse` rejects overflow exactly).
- **Alert catalog** — `map[AlertID]AlertMeta` + state for `/stats` and richer
  trigger payloads.
- **Trigger pump** — one goroutine drains `engine.Triggers()` and fans out to
  connected watchers' buffered channels. A full watcher channel → that watcher
  is disconnected (drop-and-log, never blocks the pump — same philosophy as
  the engine's ring).

## 5. chronofeed — market simulator

- ~500 pairs: majors anchored realistically (BTCUSDT ~65,000, ETHUSDT
  ~3,400, SOLUSDT ~150), long tail of alts and sub-cent memecoins down to
  ~0.00001234 — deliberately exercising the int64/decimals design where
  float64 fails.
- Per tick: geometric random walk, crypto-tuned volatility (σ ≈ 0.05%–0.3%
  per tick, memecoins wilder), bid/ask spread from a venue×tier table (TOP
  tight, MID wide). Venue and tier rotate per tick so all 6 dim combos
  interleave in the tree.
- Emission: target rate configurable (`-rate`, default 20k ticks/s), bursty
  per-symbol heat (a symbol occasionally ticks more, like real feeds), one
  gRPC client-stream per venue, batches ≤ 64. On stream error → reconnect with
  backoff, live-only (no history replay).
- RNG: `math/rand/v2` `ChaCha8`, `-seed` flag for reproducibility.

## 6. chronoctl — demo client

- Subcommands: `watch` (print triggers as they fire), `alert` (register one
  alert: `--pair BTCUSDT --venue ATLAS --tier TOP --dir above --price
  65000.12`), `demo` (the showcase).
- `demo` seeds ~1000 dim-scoped alerts (random pairs/venues/tiers, prices ±2%
  of simulated levels → steady drip) plus deliberate fan-out sets (same
  price, one alert per venue) demonstrating caller-side fan-out. Prints every
  trigger with human-readable dims.

## 7. HTTP status surface & observability

Second listener, plain `net/http` with method-pattern `ServeMux`:

- `GET /healthz` — liveness; 503 while shutting down.
- `GET /readyz` — engine up, catalogs loaded, gRPC serving, feed connected at
  least once and seen within 30s (readiness visibly flips in the demo).
- `GET /stats` — `encoding/json/v2` snapshot: uptime, alerts by state,
  triggers fired total + 1-min rate, ticks received total + rate, per-venue
  tick counts, watcher count, engine queue depths (mutation backlog, trigger
  drops).
- `GET /metrics` — Prometheus via `client_golang`: counters
  `chrono_ticks_total{venue,tier}`, `chrono_triggers_fired_total{symbol,venue,tier}`,
  `chrono_triggers_delivered_total`, `chrono_trigger_drops_total`,
  `chrono_ticks_dropped_total`; gauges `chrono_alerts_active`,
  `chrono_watchers`, `chrono_feed_connected`; histograms
  `chrono_tick_batch_size`, tick→engine latency.
- `internal/stats`: `sync/atomic` counters snapshotted per scrape; label
  metrics updated from single-writer pump goroutines.
- Shutdown order: SIGTERM/SIGINT → gRPC `GracefulStop` (10s bound) → stop
  feed accept → `engine.Close` → HTTP shutdown → exit 0. In-flight triggers
  reach watchers before streams close.
- `/debug/pprof/` exposed, including Go 1.27's `goroutineleak`.

## 8. Validation & error mapping

- Protovalidate rules where declarative (symbol pattern, direction enum,
  price string format); service-level catalog checks otherwise.
- Unknown symbol/venue/tier → `codes.InvalidArgument` naming valid values.
- Price not exactly representable at the symbol's decimals (incl. overflow) →
  `InvalidArgument`, surfacing `price.ErrPrecisionLoss` as "not representable
  at N decimals".
- `StreamTicks` batch with unknown symbol/dims → whole batch rejected
  (`InvalidArgument`) — feed bugs are loud. Malformed tick inside a valid
  batch → tick dropped + `chrono_ticks_dropped_total`, batch proceeds.
- Engine error → gRPC status mapping centralized in `internal/service/errors.go`.
- Logging: `log/slog` JSON handler, request-scoped attrs via gRPC middleware.

## 9. Configuration

Flags only, no config files: `chronod` `-grpc-addr -http-addr -log-level`;
`chronofeed` `-server -rate -seed -venue-streams`; `chronoctl` `-server`.
Symbol catalog and dim vocab are compiled-in Go tables (`internal/catalog`),
shared across binaries; `GetCatalog` is the wire view.

## 10. Modern Go usage (each for a real reason)

The first four are Go 1.27 additions; the last two are the modern baseline.

| Feature | Use |
|---|---|
| stdlib `uuid` | Alert IDs (UUIDv7 → `AlertID[16]byte`, time-ordered) |
| `encoding/json/v2` | `/stats` marshaling |
| `testing/synctest` + `httptest.NewTestServer` | Deterministic readiness/backoff tests, no wall-clock sleeps |
| `math/rand/v2` (`ChaCha8`) | Simulator RNG, seedable |
| `net/http` method patterns | Status mux |
| `log/slog` | Structured logging |

## 11. Testing

- `internal/service`: bufconn gRPC end-to-end (upsert → tick → watcher gets
  trigger, prices as decimal strings); validation table tests; catalog
  round-trip. Property test: random populations + random ticks, gRPC-delivered
  triggers must equal a naive brute-force evaluator (oracle pattern ported to
  the service layer).
- `internal/feed`: determinism under fixed seed; price bands; every tick's
  price string round-trips exactly through `price.Parse/Format`.
- `internal/server`: httptest for endpoint shapes and metric series presence.
- Concurrency: race-detector test with N watchers + parallel upserts + hot
  feed (reduced rate); no lost triggers beyond documented drop semantics.
- `goleak` VerifyTestMain in each binary package, as `engine` does.
- Integration smoke (build tag `integration`): subprocesses `chronod` +
  `chronofeed` + demo loop ~5s; asserts triggers flow and `/stats` reports
  the feed. Not in default `go test ./...`.

## 12. Acceptance gates

- `go build ./...`, `go vet ./...`, full suite green under `-race`, goleak
  clean.
- Feed sustained at `-rate 20000`: `/stats` shows ≥19.5k ticks/s ingested,
  zero unexplained watcher drops.
- Engine and price packages have zero diffs (frozen).

## 13. Out of scope

- Persistence (WAL/snapshot) — engine volatile by design.
- TLS/auth on gRPC or HTTP — localhost demo.
- Config files (YAML/TOML), dashboards (HTML), gRPC-Web/JSON transcoding.
- Fan-out automation (service-side wildcard expansion) — caller's job.
- Rate limiting/auth for feed clients.
