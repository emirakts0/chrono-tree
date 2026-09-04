# chrono-tree NATS Trigger Delivery — Design

**Date:** 2026-09-04
**Status:** Approved (brainstorming complete)
**Module:** `github.com/emir/chrono-tree`
**Depends on:** `2026-09-04-service-design.md` (merged) — this amends its trigger-delivery design
**Frozen:** `engine/` and `price/` remain untouched (zero diffs)

## 1. Purpose

Replace the service's RPC trigger fan-out (`WatchTriggers` server streaming,
watcher hub, buffered channels, eviction) with NATS publishing. chronod's
trigger pump — still the sole `engine.Triggers()` consumer — enriches each
trigger and publishes it to a NATS subject. Clients never connect to chronod
to receive triggers; they subscribe to NATS. chronoctl shrinks to a simple
alert-creation script.

Rationale: the watcher fan-out was the most complex and least realistic part
of the service layer. A message broker is the natural fit — decoupled
delivery, wildcard subscriptions over the engine's dim structure, and
observability via the standard `nats` CLI.

## 2. Decisions

| Question | Decision |
|---|---|
| NATS topology | External NATS server; chronod is a plain client (`-nats-url`, default `nats://localhost:4222`) |
| Delivery semantics | Core NATS publish (at-most-once, fire-and-forget); JetStream later via the `Publisher` seam |
| Subject structure | `chrono.triggers.{VENUE}.{TIER}` (e.g. `chrono.triggers.ATLAS.TOP`) |
| Payload | JSON (`encoding/json/v2`) enriched trigger |
| Headers | `Nats-Msg-Id: <alert_id>` (JetStream dedup hook), `Symbol: <symbol>` |
| chronoctl | `alert` + `seed` subcommands only; no trigger consumption anywhere in the repo |
| NATS down at boot | Fatal — chronod fails to start |
| NATS down later | nats.go reconnects forever; triggers during the outage are dropped-and-counted |
| Pump wiring | Synchronous `nc.Publish` inline in the pump goroutine; no extra queue or goroutine |
| Readiness | `/readyz` stays feed-based; NATS state is a gauge + `/stats` field, not gating |
| Removals | `WatchTriggers` RPC (proto + impl), watcher hub, watcher metrics/stats, `chronoctl watch`, demo watch tail |

## 3. Publisher contract (`internal/pub`)

```go
// Trigger is the enriched, wire-ready trigger (mirrors the old RPC payload).
type Trigger struct {
    AlertID          string `json:"alert_id"`
    Symbol           string `json:"symbol"`
    Venue            string `json:"venue"`
    Tier             string `json:"tier"`
    FiredPrice       string `json:"fired_price"`    // decimal string
    FiredAtUnixNanos int64  `json:"fired_at_unix_nanos"`
    Direction        string `json:"direction"`      // "ABOVE" | "BELOW"
    TargetPrice      string `json:"target_price"`   // decimal string
}

type Publisher interface {
    // Publish marshals tr and publishes to chrono.triggers.{tr.Venue}.{tr.Tier}
    // with Nats-Msg-Id and Symbol headers. Errors on disconnect or buffer
    // overflow; never blocks, never retries.
    Publish(tr Trigger) error
    // Connected reports the underlying connection state (for /stats, gauges).
    Connected() bool
    // Close drains (bounded) and closes the connection.
    Close() error
}
```

Implementations: `NewNATS(url string) (*NATSPublisher, error)` (boot-fails on
unreachable), `NoopPublisher` for tests. The seam exists so a JetStream
publisher can slot in later without touching the pump.

## 4. Pump change

`deliver` keeps its current behavior up to and including enrichment, state
flip to `triggered`, and the `active`-gate skip (non-active triggers are
counted, not shipped). The tail changes from `broadcast(out)` to:

```go
if err := c.pub.Publish(tr); err != nil {
    c.stats.TriggersPublishDropped.Add(1)
    warnOncePerQuietPeriod(err)   // slog, rate-limited — not per-trigger spam
}
c.stats.TriggersPublished.Add(1) // only on success
```

Marshal errors are impossible for this struct in practice but take the same
drop path.

## 5. Proto changes

`chrono.proto`: delete `WatchTriggers`, `WatchTriggersRequest`, and `Trigger`
(its only consumer was the deleted RPC); `internal/pub.Trigger` owns the
wire shape from here on. Everything else is unchanged. Regenerate with buf;
generated code committed.

## 6. chronod wiring

- New flag `-nats-url` (default `nats://localhost:4222`).
- `run()` connects **before** serving: unreachable NATS → startup error
  (fatal). Reconnect: `RetryOnFailedConnect: false`, then nats.go defaults —
  infinite reconnects with jittered backoff (100ms–2s).
- Shutdown order: `SetShuttingDown` → gRPC `GracefulStop` (10s bound) →
  pump stop → `pub.Close()` (drain, 3s bound, so a final burst reaches NATS)
  → `engine.Close` → HTTP shutdown.
- `/readyz` unchanged (feed-based). NATS state is observable, not gating:
  chronod ingests and evaluates fine with NATS briefly down; only delivery
  degrades, visibly.

## 7. chronoctl

- `alert` — unchanged (register one, print ID).
- `seed` — the old `demo` minus the watch tail: N dim-scoped ABOVE alerts at
  reference ±2% (default 1000, `-n`, `-seed`) plus the per-venue BTCUSDT
  fan-out set; prints the seeded count; exits.
- `watch` and `demo` removed. Observation is `nats sub chrono.triggers.>`.

## 8. Observability deltas

- `/stats`: `triggers_published` replaces `triggers_delivered`;
  `triggers_publish_dropped` added; `watchers` and `watcher_drops` removed;
  `nats_connected` bool added.
- Prometheus: `chrono_triggers_published_total` and
  `chrono_triggers_publish_dropped_total` replace
  `chrono_triggers_delivered_total` and `chrono_trigger_drops_total`;
  `chrono_watchers` removed; `chrono_nats_connected` gauge added.
  `chrono_triggers_fired_total` and all tick metrics unchanged.

## 9. Testing

- `internal/pub`: embedded in-process NATS server (`nats-server/v2` test
  server, random port). Subject mapping (`ATLAS.TOP` etc.), payload JSON
  shape, headers, wildcard subscriptions (`*.MID`, `>`), publish-failure
  path (server closed → error → drop counting works).
- `internal/service`: oracle and stress tests ported from the WatchTriggers
  stream to a NATS-subscribing collector — the oracle's brute-force expected
  set is now compared against triggers received over NATS, covering pump →
  publish → broker → subscriber end to end in-process.
- `cmd/chronod`: boot fails fast with an unreachable `-nats-url`; succeeds
  with the embedded server; shutdown completes within bounds.
- `cmd/chronoctl`: `seed` registers alerts via gRPC and exits (bufconn
  test, as today's demo test).
- Integration smoke (`-tags integration`): real `chronod` + `chronofeed`
  binaries + embedded NATS server + `chronoctl seed`; assert triggers arrive
  on the subject, `/stats` shows `nats_connected`, and the feed-rate gate
  (≥19.5k ticks/s) still holds.
- Gates: `go build ./...`, `go vet ./...`, full `-race` suite green, goleak
  clean, engine/price zero diffs.

## 10. Out of scope

- JetStream (durable streams, replay, dedup beyond the header hook).
- TLS/auth on NATS; clustering.
- Replaying or buffering triggers during NATS outages.
- Any consumer binary in this repo — subscribers are external (`nats` CLI or
  user code).
