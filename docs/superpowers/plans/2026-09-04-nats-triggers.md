# NATS Trigger Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `WatchTriggers` RPC fan-out with NATS publishing: the trigger pump publishes enriched triggers to `chrono.triggers.{venue}.{tier}`, chronoctl shrinks to `alert` + `seed`, and all watcher machinery is deleted.

**Architecture:** A new `internal/pub` package owns the wire contract (JSON payload, `Nats-Msg-Id`/`Symbol` headers, subject mapping) behind a `Publisher` interface (the JetStream-later seam). `service.Core` keeps its pump as the sole `Triggers()` consumer but calls `Publisher.Publish` inline instead of broadcasting to watcher channels. chronod connects to external NATS at boot (fail-fast) and drains on shutdown. Tests embed `nats-server/v2` in-process; production binaries only ever use the `nats.go` client.

**Tech Stack:** Go 1.27, `github.com/nats-io/nats.go` (client), `github.com/nats-io/nats-server/v2` (tests only, in-process), existing grpc-go/protovalidate/Prometheus stack.

**Spec:** `docs/superpowers/specs/2026-09-04-nats-triggers-design.md`

## Global Constraints

- **Engine and price packages are FROZEN:** zero diffs to `engine/` and `price/` (spec header). Verify after every task: `git diff --stat main -- engine price` is empty.
- **Subject/payload contract exactly:** subject `chrono.triggers.{VENUE}.{TIER}`; payload JSON (`encoding/json/v2`) of `{alert_id, symbol, venue, tier, fired_price, fired_at_unix_nanos, direction ("ABOVE"|"BELOW"), target_price}` (prices decimal strings); headers `Nats-Msg-Id: <alert_id>` and `Symbol: <symbol>`.
- **Metrics names exactly:** add `chrono_triggers_published_total`, `chrono_triggers_publish_dropped_total`, `chrono_nats_connected`; delete `chrono_triggers_delivered_total`, `chrono_trigger_drops_total`, `chrono_watchers`. All tick metrics and `chrono_triggers_fired_total` unchanged.
- **`/stats` keys exactly:** add `triggers_published`, `triggers_publish_dropped`, `nats_connected`; delete `watchers`, `watcher_drops`, `triggers_delivered`. All other keys unchanged.
- **chronod boot:** `-nats-url` flag, default `nats://localhost:4222`; unreachable NATS at startup is a fatal error. Later outages: nats.go reconnects forever (infinite, jittered backoff); publishes that error are dropped-and-counted — never block, never retry, never spool.
- **`/readyz` stays feed-based.** NATS state is observable (`chrono_nats_connected`, `/stats` `nats_connected`), never gating.
- **`nats-server/v2` is a test-only dependency** (in-process servers via `internal/pub/pubtest`); no production binary imports it.
- Generated proto code is COMMITTED; regenerate with buf only in Task 5 (`export PATH="$HOME/go/bin:$PATH" && buf generate`).
- Every task: `go build ./... && go vet ./...` clean; tests green before commit (`-race`).
- Commit messages: `feat(...)`, `test(...)`, `docs(...)`, `refactor(...)` style used in this repo.

## Deviations from spec (approved by plan self-review)

1. **Integration smoke wires its alert via direct gRPC upsert, not `chronoctl seed`.** The spec's §9 names `chronoctl seed`; the plan uses a deterministic BELOW alert just above BTC's anchor (fires on the first matching tick) so the trigger assertion is immediate rather than dependent on the random walk crossing ±2%. `chronoctl seed` behavior is covered by its own bufconn test (Task 4). Everything else in the integration test matches §9.
2. **`Core.Close()` keeps its current shape** (stop pump → `engine.Close()`); chronod calls `pub.Close()` (drain, 3s bound) immediately after. The spec's §6 ordering (drain between pump stop and engine close) is preserved in effect — the pump is stopped before the drain, and the drain is bounded — without splitting `Core.Close` into two calls.

---

### Task 1: `internal/pub` — publisher package + in-process NATS test server

**Files:**
- Create: `internal/pub/pub.go`
- Create: `internal/pub/pub_test.go`
- Create: `internal/pub/pubtest/pubtest.go`
- Modify: `go.mod`, `go.sum`

**Interfaces:**
- Consumes: nothing in this repo.
- Produces (Tasks 2–5 rely on exactly these):

```go
package pub

type Trigger struct {
    AlertID          string `json:"alert_id"`
    Symbol           string `json:"symbol"`
    Venue            string `json:"venue"`
    Tier             string `json:"tier"`
    FiredPrice       string `json:"fired_price"`
    FiredAtUnixNanos int64  `json:"fired_at_unix_nanos"`
    Direction        string `json:"direction"` // "ABOVE" | "BELOW"
    TargetPrice      string `json:"target_price"`
}
func Subject(venue, tier string) string // "chrono.triggers.ATLAS.TOP"
type Publisher interface {
    Publish(tr Trigger) error
    Connected() bool
    Close() error
}
func NewNATS(url string) (*NATSPublisher, error) // boot-fatal on unreachable
type NATSPublisher struct{ ... }
type Noop struct{} // Publish nil, Connected true, Close nil
```

```go
package pubtest // internal/pub/pubtest
func Start(t *testing.T) string // embedded NATS server on 127.0.0.1:<random>; returns client URL; shuts down on cleanup
```

- [ ] **Step 1: Add dependencies**

```bash
go get github.com/nats-io/nats.go@latest
go get github.com/nats-io/nats-server/v2@latest
go mod tidy
```

Note: `nats-server/v2` is pulled in only by `internal/pub/pubtest`; `go mod tidy` keeps it as a direct require (pubtest is a normal package). That is expected.

- [ ] **Step 2: Write the failing test `internal/pub/pub_test.go`**

```go
package pub

import (
	"testing"
	"time"

	"nats" "github.com/nats-io/nats.go"

	"github.com/emir/chrono-tree/internal/pub/pubtest"
)

func sampleTrigger() Trigger {
	return Trigger{
		AlertID: "0192ced1-4a1e-7abc-8def-0123456789ab", Symbol: "BTCUSDT",
		Venue: "ATLAS", Tier: "TOP",
		FiredPrice: "65001.00", FiredAtUnixNanos: 1_700_000_000_000_000_001,
		Direction: "ABOVE", TargetPrice: "65000.00",
	}
}

func TestSubject(t *testing.T) {
	if got := Subject("ATLAS", "TOP"); got != "chrono.triggers.ATLAS.TOP" {
		t.Fatalf("Subject = %q", got)
	}
}

func TestPublishSubjectPayloadHeaders(t *testing.T) {
	url := pubtest.Start(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	raw := make(chan *nats.Msg, 16)
	if _, err := nc.ChanSubscribe("chrono.triggers.>", raw); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	p, err := NewNATS(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if !p.Connected() {
		t.Fatal("publisher should report connected")
	}
	want := sampleTrigger()
	if err := p.Publish(want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var msg *nats.Msg
	select {
	case msg = <-raw:
	case <-time.After(3 * time.Second):
		t.Fatal("no message received")
	}
	if msg.Subject != "chrono.triggers.ATLAS.TOP" {
		t.Fatalf("subject = %q", msg.Subject)
	}
	if got := msg.Header.Get("Nats-Msg-Id"); got != want.AlertID {
		t.Fatalf("Nats-Msg-Id = %q, want %q", got, want.AlertID)
	}
	if got := msg.Header.Get("Symbol"); got != want.Symbol {
		t.Fatalf("Symbol header = %q, want %q", got, want.Symbol)
	}
	var got Trigger
	if err := json.Unmarshal(msg.Data, &got); err != nil {
		t.Fatalf("payload not JSON: %v", err)
	}
	if got != want {
		t.Fatalf("payload = %+v, want %+v", got, want)
	}
}

func TestPublishWildcardTier(t *testing.T) {
	url := pubtest.Start(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	raw := make(chan *nats.Msg, 16)
	if _, err := nc.ChanSubscribe("chrono.triggers.*.MID", raw); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	p, err := NewNATS(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	top := sampleTrigger() // ATLAS/TOP — must NOT arrive
	if err := p.Publish(top); err != nil {
		t.Fatal(err)
	}
	mid := sampleTrigger()
	mid.Tier = "MID"
	if err := p.Publish(mid); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-raw:
		if msg.Subject != "chrono.triggers.ATLAS.MID" {
			t.Fatalf("subject = %q", msg.Subject)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("MID trigger not received on *.MID")
	}
	select {
	case msg := <-raw:
		t.Fatalf("unexpected extra message on subject %q", msg.Subject)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestNewNATSBootFails(t *testing.T) {
	// Port 1 is never a NATS server; RetryOnFailedConnect is off.
	if _, err := NewNATS("nats://127.0.0.1:1"); err == nil {
		t.Fatal("NewNATS should fail on unreachable server")
	}
}

func TestPublishAfterCloseErrors(t *testing.T) {
	url := pubtest.Start(t)
	p, err := NewNATS(url)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := p.Publish(sampleTrigger()); err == nil {
		t.Fatal("Publish after Close should error (drop-and-count path)")
	}
	if p.Connected() {
		t.Fatal("Connected should be false after Close")
	}
	// Close is safe to call twice (chronod defer + shutdown path).
	_ = p.Close()
}
```

Fix the imports before running: the `nats` import line must read `nats "github.com/nats-io/nats.go"`, and add `"encoding/json/v2"` as `json` (Go 1.27: `import "encoding/json/v2"` binds package name `json`).

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/pub/`
Expected: FAIL — package does not exist.

- [ ] **Step 4: Write `internal/pub/pubtest/pubtest.go`**

```go
// Package pubtest starts an in-process NATS server for tests. It exists so
// every test that needs a broker (unit, service, and integration) embeds
// one instead of requiring an external nats-server binary; production code
// never imports this package or the nats-server dependency.
package pubtest

import (
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// Start launches an embedded NATS server on 127.0.0.1 with a random port
// and returns its client URL. The server shuts down at test cleanup.
func Start(t *testing.T) string {
	t.Helper()
	s, err := natsserver.NewServer(&natsserver.Options{
		Host:   "127.0.0.1",
		Port:   -1, // random free port
		NoLog:  true,
		NoSigs: true,
	})
	if err != nil {
		t.Fatalf("embedded NATS server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded NATS server did not become ready")
	}
	t.Cleanup(s.Shutdown)
	return s.ClientURL()
}
```

- [ ] **Step 5: Write `internal/pub/pub.go`**

```go
// Package pub owns the trigger wire contract for NATS: enriched triggers
// as JSON on chrono.triggers.{venue}.{tier} subjects with Nats-Msg-Id and
// Symbol headers. Core NATS only — at-most-once, fire-and-forget; the
// Publisher interface is the seam a JetStream implementation slots into.
package pub

import (
	"encoding/json/v2"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// Trigger is the enriched, wire-ready trigger (spec §3).
type Trigger struct {
	AlertID          string `json:"alert_id"`
	Symbol           string `json:"symbol"`
	Venue            string `json:"venue"`
	Tier             string `json:"tier"`
	FiredPrice       string `json:"fired_price"` // decimal string
	FiredAtUnixNanos int64  `json:"fired_at_unix_nanos"`
	Direction        string `json:"direction"` // "ABOVE" | "BELOW"
	TargetPrice      string `json:"target_price"` // decimal string
}

// Subject maps a dim combo to its NATS subject.
func Subject(venue, tier string) string {
	return fmt.Sprintf("chrono.triggers.%s.%s", venue, tier)
}

// Publisher is the delivery seam. Publish must never block and never
// retry: an error means the caller drops the trigger and counts it.
// Connected is for observability only, never gating.
type Publisher interface {
	Publish(tr Trigger) error
	Connected() bool
	Close() error
}

// drainBound caps how long Close waits for the connection's pending
// publishes to flush before cutting it (spec §6).
const drainBound = 3 * time.Second

// NATSPublisher publishes over one core NATS connection.
type NATSPublisher struct {
	nc *nats.Conn
}

// NewNATS connects to url and fails if the server is unreachable at call
// time (chronod treats that as a fatal boot error). Later disconnects are
// handled by nats.go's reconnect: infinite attempts, 100ms base wait with
// up to 2s jitter. While disconnected, nats.go buffers pending publishes
// (default 8MB); Publish errors once the buffer overflows or the
// connection is closed — those errors are the drop-and-count path.
func NewNATS(url string) (*NATSPublisher, error) {
	nc, err := nats.Connect(url,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(100*time.Millisecond),
		nats.ReconnectJitter(2*time.Second, 2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect %s: %w", url, err)
	}
	return &NATSPublisher{nc: nc}, nil
}

// Publish marshals tr and publishes it on Subject(tr.Venue, tr.Tier) with
// the Nats-Msg-Id (JetStream dedup hook, free today) and Symbol headers.
func (p *NATSPublisher) Publish(tr Trigger) error {
	payload, err := json.Marshal(tr)
	if err != nil {
		return fmt.Errorf("marshal trigger: %w", err)
	}
	msg := nats.NewMsg(Subject(tr.Venue, tr.Tier))
	msg.Data = payload
	msg.Header.Set("Nats-Msg-Id", tr.AlertID)
	msg.Header.Set("Symbol", tr.Symbol)
	return p.nc.PublishMsg(msg)
}

// Connected reports the live connection state.
func (p *NATSPublisher) Connected() bool {
	return p.nc.Status() == nats.CONNECTED
}

// Close drains (flushes pending publishes, bounded by drainBound) and
// closes the connection. Safe to call more than once.
func (p *NATSPublisher) Close() error {
	done := make(chan error, 1)
	go func() { done <- p.nc.Drain() }()
	select {
	case err := <-done:
		p.nc.Close()
		return err
	case <-time.After(drainBound):
		p.nc.Close()
		return fmt.Errorf("nats drain did not finish within %s", drainBound)
	}
}

// Noop is the zero-dependency publisher for tests and call sites that
// have not wired NATS yet. Connected reports true: to /stats and the
// gauge, a Noop publisher presents as a healthy sink.
type Noop struct{}

func (Noop) Publish(Trigger) error { return nil }
func (Noop) Connected() bool       { return true }
func (Noop) Close() error          { return nil }
```

- [ ] **Step 6: Run tests**

Run: `go test ./internal/pub/ -race -count=1`
Expected: PASS (all five tests).

- [ ] **Step 7: Commit**

```bash
git add internal/pub/ go.mod go.sum
git commit -m "feat(pub): NATS trigger publisher with subject/header contract"
```

---

### Task 2: Core rewiring — pump publishes, watcher machinery deleted

The heart of the change: `service.Core`'s pump tail becomes `Publisher.Publish`; the watcher hub, `WatchTriggers`, and eviction go away; stats/metrics/`/stats` fields swap; the service tests (unit, oracle, stress) move to the publisher path. This task also makes the one-line mechanical `NewCore` call-site updates everywhere else so the whole module builds green.

**Files:**
- Modify: `internal/service/core.go` (remove watcher machinery; publisher injection; deliver tail)
- Modify: `internal/stats/stats.go` (field swap) + `internal/stats/stats_test.go`
- Modify: `internal/server/prom.go` (full-file replacement below)
- Modify: `internal/server/server.go` (statusView + gauge registration)
- Modify: `internal/server/server_test.go` (keys and metric names)
- Modify: `internal/service/testenv_test.go` (two envs: recording + NATS)
- Delete: `internal/service/watch_test.go` (replaced by `pub_test.go` below)
- Create: `internal/service/pub_test.go`
- Modify: `internal/service/oracle_test.go`, `internal/service/stress_test.go` (NATS collector)
- Modify (mechanical, one line each — add `pub.Noop{}` argument): `cmd/chronod/main.go`, `cmd/chronod/main_test.go`, `cmd/chronofeed/main_test.go`, `cmd/chronoctl/demo_test.go`

**Interfaces:**
- Consumes: Task 1's `pub.Publisher`, `pub.Trigger`, `pub.Noop`, `pub.NewNATS`, `pub.Subject`.
- Produces (Tasks 3–5 rely on):

```go
// internal/service
func NewCore(cfg engine.Config, cat *catalog.Catalog, m Metrics, st *stats.Stats, p pub.Publisher) *Core
func (c *Core) NATSConnected() bool // pub.Connected()
// Metrics interface changes: TriggerDelivered() → TriggerPublished();
// WatcherDrop() → TriggerPublishDropped(); Watchers(n int) REMOVED.
// DELETED: WatchTriggers, WatcherCount, watcher, newWatcher, broadcast,
// fromEngineDirection, hubMu, watchers field.
```

```go
// internal/stats — TriggersDelivered/WatcherDrops replaced by:
TriggersPublished     atomic.Uint64
TriggersPublishDropped atomic.Uint64
// Snapshot JSON: "triggers_published", "triggers_publish_dropped"
```

- [ ] **Step 1: Update `internal/stats/stats.go`**

Replace the `Stats` struct's counter block and `Snapshot`:

```go
type Stats struct {
	started time.Time

	Ticks                atomic.Uint64
	TicksDropped         atomic.Uint64
	TriggersFired        atomic.Uint64
	TriggersPublished    atomic.Uint64
	TriggersPublishDropped atomic.Uint64

	TickRate Rate
	FireRate Rate
}
```

```go
// Snapshot is the /stats JSON shape (json/v2 marshals the tags).
type Snapshot struct {
	UptimeSec              float64 `json:"uptime_sec"`
	Ticks                  uint64  `json:"ticks"`
	TicksPerSec            float64 `json:"ticks_per_sec"`
	TicksDropped           uint64  `json:"ticks_dropped"`
	TriggersFired          uint64  `json:"triggers_fired"`
	TriggersPerSec         float64 `json:"triggers_per_sec"`
	TriggersPublished      uint64  `json:"triggers_published"`
	TriggersPublishDropped uint64  `json:"triggers_publish_dropped"`
}

func (s *Stats) Snapshot(now time.Time) Snapshot {
	return Snapshot{
		UptimeSec:              now.Sub(s.started).Seconds(),
		Ticks:                  s.Ticks.Load(),
		TicksPerSec:            s.TickRate.PerSecond(now),
		TicksDropped:           s.TicksDropped.Load(),
		TriggersFired:          s.TriggersFired.Load(),
		TriggersPerSec:         s.FireRate.PerSecond(now),
		TriggersPublished:      s.TriggersPublished.Load(),
		TriggersPublishDropped: s.TriggersPublishDropped.Load(),
	}
}
```

Update `internal/stats/stats_test.go`'s `TestSnapshotFields` to match:

```go
func TestSnapshotFields(t *testing.T) {
	now := time.Now()
	s := New(now)
	s.Ticks.Add(3)
	s.TicksDropped.Add(1)
	s.TriggersFired.Add(2)
	s.TriggersPublished.Add(2)
	s.TriggersPublishDropped.Add(1)
	snap := s.Snapshot(now.Add(2 * time.Second))
	if snap.Ticks != 3 || snap.TicksDropped != 1 || snap.TriggersFired != 2 ||
		snap.TriggersPublished != 2 || snap.TriggersPublishDropped != 1 {
		t.Fatalf("snapshot counters wrong: %+v", snap)
	}
	if math.Abs(snap.UptimeSec-2) > 0.01 {
		t.Fatalf("uptime = %v, want 2", snap.UptimeSec)
	}
}
```

- [ ] **Step 2: Rewrite `internal/service/core.go`**

Changes, top to bottom:

1. Package doc: replace "trigger fan-out" with "trigger publishing to NATS".
2. Imports: add `"log/slog"` and `"github.com/emir/chrono-tree/internal/pub"`.
3. `Metrics` interface and `NoopMetrics`:

```go
// Metrics is the observability hook; implementations must be safe for
// concurrent use. NoopMetrics covers tests.
type Metrics interface {
	Tick(venue, tier string)
	TickDropped()
	TickBatch(n int)
	TickLatency(d time.Duration)
	TriggerFired(symbol, venue, tier string)
	TriggerPublished()
	TriggerPublishDropped()
	AlertsActive(n int)
	FeedConnected(b bool)
}

// NoopMetrics discards everything.
type NoopMetrics struct{}

func (NoopMetrics) Tick(string, string)                 {}
func (NoopMetrics) TickDropped()                        {}
func (NoopMetrics) TickBatch(int)                       {}
func (NoopMetrics) TickLatency(time.Duration)           {}
func (NoopMetrics) TriggerFired(string, string, string) {}
func (NoopMetrics) TriggerPublished()                   {}
func (NoopMetrics) TriggerPublishDropped()              {}
func (NoopMetrics) AlertsActive(int)                    {}
func (NoopMetrics) FeedConnected(bool)                  {}
```

4. `Core` struct: DELETE `hubMu sync.RWMutex` / `watchers map[*watcher]struct{}`; ADD:

```go
	pub       pub.Publisher
	pubHealthy atomic.Bool // log publish failures on transition only
```

5. DELETE the `watcher` type and `newWatcher` entirely.
6. `NewCore` gains the publisher parameter and drops the watchers map init:

```go
// NewCore builds the engine with the catalog's dim vocabulary. The
// publisher receives every enriched trigger; nil is not allowed — pass
// pub.Noop{} when there is nothing to publish to.
func NewCore(cfg engine.Config, cat *catalog.Catalog, m Metrics, st *stats.Stats, p pub.Publisher) *Core {
	cfg.Dims = cat.Dims() // the catalog owns the vocabulary
	c := &Core{
		Cat:     cat,
		eng:     engine.New(cfg),
		metrics: m,
		stats:   st,
		pub:     p,
		now:     func() time.Time { return time.Now() },
		alerts:  make(map[engine.AlertID]*Alert),
	}
	c.venueTicks = make(map[string]*atomic.Uint64, len(cat.DimValues(catalog.DimVenue)))
	for _, v := range cat.DimValues(catalog.DimVenue) {
		c.venueTicks[v] = &atomic.Uint64{}
	}
	c.pumpCtx, c.pumpCancel = context.WithCancel(context.Background())
	c.pumpDone = make(chan struct{})
	go c.pump()
	return c
}
```

7. DELETE: `WatcherCount`, `broadcast`, `WatchTriggers`, `fromEngineDirection`.
8. Replace `deliver` and add the direction helper and accessor:

```go
// pump drains the engine's trigger ring and publishes enriched triggers
// to NATS. It is the ONLY Triggers() consumer.
```

(update the pump's doc comment as above; its body is unchanged)

```go
// deliver enriches one engine trigger from the service catalog and
// publishes it. The catalog entry survives the engine's own fire-time
// refs cleanup — that removal race is why the service catalog exists.
func (c *Core) deliver(tr *engine.Trigger, now time.Time) {
	c.mu.RLock()
	a := c.alerts[tr.ID]
	var state AlertState
	if a != nil {
		state = a.State
	}
	c.mu.RUnlock()
	c.stats.TriggersFired.Add(1)
	c.stats.FireRate.Add(1, now)
	if a == nil || state != StateActive {
		// Fired for an alert we no longer consider active (cancelled
		// concurrently, or a terminal replacement race). Count, don't ship.
		return
	}
	c.mu.Lock()
	if a.State != StateActive {
		c.mu.Unlock()
		return // cancelled/replaced between the read above and here
	}
	a.State = StateTriggered
	c.mu.Unlock()
	out := pub.Trigger{
		AlertID:          alertIDString(tr.ID),
		Symbol:           a.Symbol,
		Venue:            a.Venue,
		Tier:             a.Tier,
		FiredPrice:       price.Format(int64(tr.Price), a.Decimals),
		FiredAtUnixNanos: tr.TS,
		Direction:        directionOf(a.Direction),
		TargetPrice:      price.Format(int64(a.TargetPrice), a.Decimals),
	}
	c.metrics.TriggerFired(a.Symbol, a.Venue, a.Tier)
	if err := c.pub.Publish(out); err != nil {
		// Drop-and-count: the pump never blocks, never retries (spec §4).
		c.stats.TriggersPublishDropped.Add(1)
		c.metrics.TriggerPublishDropped()
		if c.pubHealthy.CompareAndSwap(true, false) {
			slog.Warn("trigger publish failing; dropping until NATS recovers", "err", err)
		}
		return
	}
	if !c.pubHealthy.Swap(true) {
		slog.Info("trigger publishing recovered")
	}
	c.stats.TriggersPublished.Add(1)
	c.metrics.TriggerPublished()
}

func directionOf(d engine.Direction) string {
	if d == engine.DirLTE {
		return "BELOW"
	}
	return "ABOVE"
}

// NATSConnected reports the publisher's connection state for /stats and
// the chrono_nats_connected gauge. Observability only — never readiness.
func (c *Core) NATSConnected() bool { return c.pub.Connected() }
```

9. `Core.Close` is UNCHANGED (pump stop → engine close; chronod drains the publisher right after — see plan deviation 2).

- [ ] **Step 3: Replace `internal/server/prom.go` in full**

```go
package server

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/emir/chrono-tree/internal/service"
)

// PromMetrics implements service.Metrics on a Prometheus registry.
type PromMetrics struct {
	ticks          *prometheus.CounterVec
	fired          *prometheus.CounterVec
	published      prometheus.Counter
	publishDropped prometheus.Counter
	ticksDropped   prometheus.Counter
	batch          prometheus.Histogram
	latency        prometheus.Histogram
	active         prometheus.Gauge
	feed           prometheus.Gauge
}

// NewPromMetrics registers and returns the metric set. The names are
// pinned by the spec.
func NewPromMetrics(reg prometheus.Registerer) *PromMetrics {
	m := &PromMetrics{
		ticks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chrono_ticks_total", Help: "Ticks accepted by the engine, by venue and tier.",
		}, []string{"venue", "tier"}),
		fired: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chrono_triggers_fired_total", Help: "Alerts fired by the engine, by symbol/venue/tier.",
		}, []string{"symbol", "venue", "tier"}),
		published: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chrono_triggers_published_total", Help: "Triggers published to NATS.",
		}),
		publishDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chrono_triggers_publish_dropped_total", Help: "Triggers dropped because the NATS publish failed.",
		}),
		ticksDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chrono_ticks_dropped_total", Help: "Ticks dropped for unrepresentable prices.",
		}),
		batch: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "chrono_tick_batch_size", Help: "Ticks per StreamTicks batch.",
			Buckets: []float64{1, 4, 8, 16, 32, 48, 64, 96, 128},
		}),
		latency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "chrono_tick_latency_seconds", Help: "Tick timestamp-to-ingest latency.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 13), // 100µs .. ~410ms
		}),
		active: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chrono_alerts_active", Help: "Alerts currently active service-side.",
		}),
		feed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chrono_feed_connected", Help: "1 once a feed has ever connected.",
		}),
	}
	reg.MustRegister(m.ticks, m.fired, m.published, m.publishDropped, m.ticksDropped,
		m.batch, m.latency, m.active, m.feed)
	return m
}

func (m *PromMetrics) Tick(venue, tier string)     { m.ticks.WithLabelValues(venue, tier).Inc() }
func (m *PromMetrics) TickDropped()                { m.ticksDropped.Inc() }
func (m *PromMetrics) TickBatch(n int)             { m.batch.Observe(float64(n)) }
func (m *PromMetrics) TickLatency(d time.Duration) { m.latency.Observe(d.Seconds()) }
func (m *PromMetrics) TriggerFired(symbol, venue, tier string) {
	m.fired.WithLabelValues(symbol, venue, tier).Inc()
}
func (m *PromMetrics) TriggerPublished()        { m.published.Inc() }
func (m *PromMetrics) TriggerPublishDropped()   { m.publishDropped.Inc() }
func (m *PromMetrics) AlertsActive(n int)       { m.active.Set(float64(n)) }
func (m *PromMetrics) FeedConnected(b bool) {
	if b {
		m.feed.Set(1)
	}
}

var _ service.Metrics = (*PromMetrics)(nil)
```

(`chrono_nats_connected` is NOT here — it is a `GaugeFunc` over `Core.NATSConnected()` registered in `server.New`, so it never needs an interface method or a writer goroutine.)

- [ ] **Step 4: Update `internal/server/server.go`**

1. `New` registers the NATS gauge:

```go
func New(core *service.Core, st *stats.Stats, reg *prometheus.Registry) *Server {
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "chrono_nats_connected", Help: "1 while the NATS publisher connection is up.",
	}, func() float64 {
		if core.NATSConnected() {
			return 1
		}
		return 0
	}))
	return &Server{core: core, stats: st, reg: reg}
}
```

2. `statusView` replacement:

```go
// statusView is the /stats JSON document.
type statusView struct {
	UptimeSec              float64           `json:"uptime_sec"`
	AlertsByState          map[string]int    `json:"alerts_by_state"`
	FeedEverConnected      bool              `json:"feed_ever_connected"`
	FeedLastSeenMsAgo      int64             `json:"feed_last_seen_ms_ago"` // -1 = never
	NATSConnected          bool              `json:"nats_connected"`
	VenueTicks             map[string]uint64 `json:"venue_ticks"`
	Ticks                  uint64            `json:"ticks"`
	TicksPerSec            float64           `json:"ticks_per_sec"`
	TicksDropped           uint64            `json:"ticks_dropped"`
	TriggersFired          uint64            `json:"triggers_fired"`
	TriggersPerSec         float64           `json:"triggers_per_sec"`
	TriggersPublished      uint64            `json:"triggers_published"`
	TriggersPublishDropped uint64            `json:"triggers_publish_dropped"`
	Engine                 engineView        `json:"engine"`
}
```

3. `handleStats` replacement:

```go
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	snap := s.stats.Snapshot(now)
	es := s.core.Engine().Stats()
	view := statusView{
		UptimeSec:              snap.UptimeSec,
		AlertsByState:          s.core.AlertsByState(),
		FeedEverConnected:      s.core.FeedEverConnected(),
		FeedLastSeenMsAgo:      -1,
		NATSConnected:          s.core.NATSConnected(),
		VenueTicks:             s.core.VenueTicks(),
		Ticks:                  snap.Ticks,
		TicksPerSec:            snap.TicksPerSec,
		TicksDropped:           snap.TicksDropped,
		TriggersFired:          snap.TriggersFired,
		TriggersPerSec:         snap.TriggersPerSec,
		TriggersPublished:      snap.TriggersPublished,
		TriggersPublishDropped: snap.TriggersPublishDropped,
		Engine:                 engineView{Live: es.Live, DroppedTriggers: es.DroppedTriggers},
	}
	if last := s.core.FeedLastSeen(); !last.IsZero() {
		view.FeedLastSeenMsAgo = time.Since(last).Milliseconds()
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.MarshalWrite(w, view); err != nil {
		// Headers are sent; nothing to do but log-shape the error.
		_ = err
	}
}
```

- [ ] **Step 5: Update `internal/server/server_test.go`**

- `newServer`/`TestMetricsEndpoint`'s `service.NewCore(...)` calls gain `pub.Noop{}` (import `"github.com/emir/chrono-tree/internal/pub"`).
- `TestStatsJSON` key list becomes: `{"uptime_sec", "alerts_by_state", "feed_ever_connected", "venue_ticks", "ticks", "ticks_per_sec", "triggers_fired", "triggers_published", "triggers_publish_dropped", "nats_connected", "engine"}`.
- `TestMetricsEndpoint`: replace `pm.TriggerDelivered()` with `pm.TriggerPublished()`; the names slice becomes `{"chrono_ticks_total", "chrono_triggers_fired_total", "chrono_triggers_published_total", "chrono_triggers_publish_dropped_total", "chrono_ticks_dropped_total", "chrono_alerts_active", "chrono_nats_connected", "chrono_feed_connected", "chrono_tick_batch_size", "chrono_tick_latency_seconds"}` and add `pm.TriggerPublished()` before scraping so the counter series exists.

- [ ] **Step 6: Rewrite `internal/service/testenv_test.go`**

```go
package service

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"nats" "github.com/nats-io/nats.go"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/pub"
	"github.com/emir/chrono-tree/internal/pub/pubtest"
	"github.com/emir/chrono-tree/internal/stats"
)

type testEnv struct {
	core *Core
	cc   *grpc.ClientConn
	srv  *grpc.Server
	rec  *recordingPub // non-nil for the fast unit env
	nats string        // non-empty for the NATS-backed env
}

// recordingPub captures published triggers for unit tests, and can be
// flipped to failing to exercise the drop-and-count path.
type recordingPub struct {
	mu   sync.Mutex
	pubs []pub.Trigger
	fail bool
}

func (r *recordingPub) Publish(tr pub.Trigger) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return errors.New("publisher down")
	}
	r.pubs = append(r.pubs, tr)
	return nil
}
func (r *recordingPub) Connected() bool { return true }
func (r *recordingPub) Close() error    { return nil }
func (r *recordingPub) triggers() []pub.Trigger {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]pub.Trigger(nil), r.pubs...)
}

func newEnv(t *testing.T) *testEnv {
	t.Helper()
	rec := &recordingPub{}
	return newEnvWithPub(t, rec, "")
}

// newNATSEnv builds the env against a real in-process NATS server and a
// real NATSPublisher — the full pump → publish → broker path.
func newNATSEnv(t *testing.T) *testEnv {
	t.Helper()
	url := pubtest.Start(t)
	p, err := pub.NewNATS(url)
	if err != nil {
		t.Fatalf("pub.NewNATS: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return newEnvWithPub(t, p, url)
}

func newEnvWithPub(t *testing.T, p pub.Publisher, natsURL string) *testEnv {
	t.Helper()
	cat := catalog.Default()
	core := NewCore(engine.DefaultConfig(), cat, NoopMetrics{}, stats.New(time.Now()), p)
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(NewValidateInterceptor()))
	chronov1.RegisterAlertServiceServer(srv, core)
	chronov1.RegisterFeedServiceServer(srv, core)
	go func() { _ = srv.Serve(lis) }()
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() {
		_ = cc.Close()
		srv.Stop()
		core.Close()
	})
	var rec *recordingPub
	if r, ok := p.(*recordingPub); ok {
		rec = r
	}
	return &testEnv{core: core, cc: cc, srv: srv, rec: rec, nats: natsURL}
}

// natsClient connects a subscriber to the env's embedded NATS server
// (NATS-backed envs only) and unsubscribes+closes at cleanup.
func (e *testEnv) natsClient(t *testing.T) *nats.Conn {
	t.Helper()
	if e.nats == "" {
		t.Fatal("natsClient requires newNATSEnv")
	}
	nc, err := nats.Connect(e.nats)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	return nc
}

func (e *testEnv) alerts() chronov1.AlertServiceClient { return chronov1.NewAlertServiceClient(e.cc) }
func (e *testEnv) feed() chronov1.FeedServiceClient    { return chronov1.NewFeedServiceClient(e.cc) }
```

- [ ] **Step 7: Delete `internal/service/watch_test.go`; create `internal/service/pub_test.go`**

```go
package service

import (
	"context"
	"encoding/json/v2"
	"testing"
	"time"

	"nats" "github.com/nats-io/nats.go"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/pub"
)

func TestPublishesEnrichedTrigger(t *testing.T) {
	e := newNATSEnv(t)
	nc := e.natsClient(t)
	raw := make(chan *nats.Msg, 16)
	if _, err := nc.ChanSubscribe("chrono.triggers.>", raw); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	resp, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
		Symbol: "BTCUSDT", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction: chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "65000.00", Venue: "ATLAS", Tier: "TOP",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65001.00", "ATLAS", "TOP"),
	}}); err != nil {
		t.Fatal(err)
	}

	var msg *nats.Msg
	select {
	case msg = <-raw:
	case <-time.After(3 * time.Second):
		t.Fatal("no trigger published")
	}
	if msg.Subject != pub.Subject("ATLAS", "TOP") {
		t.Fatalf("subject = %q", msg.Subject)
	}
	var tr pub.Trigger
	if err := json.Unmarshal(msg.Data, &tr); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if tr.AlertID != resp.GetAlertId() || tr.Symbol != "BTCUSDT" ||
		tr.Venue != "ATLAS" || tr.Tier != "TOP" {
		t.Fatalf("trigger dims/id wrong: %+v", tr)
	}
	if tr.FiredPrice != "65001.00" || tr.TargetPrice != "65000.00" {
		t.Fatalf("prices wrong: %+v", tr)
	}
	if tr.Direction != "ABOVE" || tr.FiredAtUnixNanos == 0 {
		t.Fatalf("direction/timestamp wrong: %+v", tr)
	}
	if got := msg.Header.Get("Nats-Msg-Id"); got != resp.GetAlertId() {
		t.Fatalf("Nats-Msg-Id = %q", got)
	}
	if got := e.core.AlertsByState()["triggered"]; got != 1 {
		t.Fatalf("triggered state count = %d, want 1", got)
	}
	if e.core.stats.TriggersPublished.Load() != 1 {
		t.Fatal("publish not counted")
	}
}

func TestPublishDropWhenPublisherFails(t *testing.T) {
	e := newEnv(t)
	e.rec.fail = true
	if _, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
		Symbol: "BTCUSDT", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction: chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "65000.00", Venue: "ATLAS", Tier: "TOP",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65001.00", "ATLAS", "TOP"),
	}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && e.core.stats.TriggersPublishDropped.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	if got := e.core.stats.TriggersPublishDropped.Load(); got != 1 {
		t.Fatalf("TriggersPublishDropped = %d, want 1", got)
	}
	if got := e.core.stats.TriggersPublished.Load(); got != 0 {
		t.Fatalf("TriggersPublished = %d, want 0", got)
	}
	if len(e.rec.triggers()) != 0 {
		t.Fatal("failing publisher should not record triggers")
	}
	// The alert still flipped to triggered: a delivery failure must not
	// rewind alert state.
	if got := e.core.AlertsByState()["triggered"]; got != 1 {
		t.Fatalf("triggered state count = %d, want 1", got)
	}
}
```

- [ ] **Step 8: Port the oracle to the NATS path (`internal/service/oracle_test.go`)**

Replace the watcher block (the `wstream` setup, `gotIDs` goroutine, and the WatcherCount wait — lines from `// Watch in the background` through the `for e.core.WatcherCount() == 0 ...` loop) with:

```go
	// Subscribe in the background, collecting trigger IDs from the wire —
	// the same path real consumers use.
	nc := e.natsClient(t)
	raw := make(chan *nats.Msg, 4096)
	if _, err := nc.ChanSubscribe("chrono.triggers.>", raw); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	gotIDs := make(chan string, 4096)
	go func() {
		for m := range raw {
			var tr pub.Trigger
			if err := json.Unmarshal(m.Data, &tr); err == nil {
				gotIDs <- tr.AlertID
			}
		}
		close(gotIDs)
	}()
```

Add imports `"encoding/json/v2"` and `"github.com/emir/chrono-tree/internal/pub"`, change the env to `e := newNATSEnv(t)`, and delete the now-unused `context` import only if nothing else uses it (`context.Background()` still appears in upserts — it stays). Everything else (seeding, tick loop, brute-force comparison, stray drain) is unchanged.

- [ ] **Step 9: Port the stress test (`internal/service/stress_test.go`)**

Replace the 4-watcher setup block (from `var delivered = make([]uint64, 4)` through the `wwg` goroutines) with a single NATS subscription and counter; rename the test; delete `TestWatcherEviction` entirely; delete the `sum` helper if now unused (it is — remove it):

```go
// TestStressFeedAndPublish: parallel upserts and a 5k/s tick stream for
// ~1.5s against a real in-process NATS server. Under -race this
// exercises the pump, the publisher, and catalog mutation together.
// Assertions: triggers fired > 0, received over NATS > 0, zero dropped
// publishes at this modest rate.
func TestStressFeedAndPublish(t *testing.T) {
	e := newNATSEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nc := e.natsClient(t)
	var received atomic.Uint64
	if _, err := nc.Subscribe("chrono.triggers.>", func(*nats.Msg) { received.Add(1) }); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
```

…and at the end replace the old assertions with:

```go
	if e.core.stats.TriggersFired.Load() == 0 {
		t.Fatal("nothing fired under stress")
	}
	if received.Load() == 0 {
		t.Fatal("no triggers received over NATS")
	}
	if got := e.core.stats.TriggersPublishDropped.Load(); got != 0 {
		t.Fatalf("dropped publishes at 5k ticks/s: %d", got)
	}
```

Remove `var delivered`/`wwg` references in the body (`wwg.Wait()` line goes), keep `wg.Wait()`, `<-feedDone`, `cancel()`. Imports: add `"sync/atomic"` and the `nats` import; remove `"sync"` only if `WaitGroup` remains used (it does — `wg` — so `sync` stays).

- [ ] **Step 10: Mechanical NewCore call-site updates (module must build green)**

Each site gets `pub.Noop{}` as the new last argument (import `"github.com/emir/chrono-tree/internal/pub"`):

- `cmd/chronod/main.go:55` — `service.NewCore(engine.DefaultConfig(), catalog.Default(), pm, st, pub.Noop{})` (Task 3 replaces this with the real publisher).
- `cmd/chronod/main_test.go`, `cmd/chronofeed/main_test.go`, `cmd/chronoctl/demo_test.go` — same one-arg addition.

- [ ] **Step 11: Run the full module**

```bash
go build ./... && go vet ./...
go test ./internal/stats/ ./internal/service/ ./internal/server/ -race -count=1
go test ./... -race -count=1
git diff --stat main -- engine price   # must be empty
```

Expected: all green.

- [ ] **Step 12: Commit**

```bash
git add internal/stats/ internal/service/ internal/server/ cmd/chronod/ cmd/chronofeed/ cmd/chronoctl/
git commit -m "refactor(service): pump publishes triggers to NATS; watcher fan-out removed"
```

(Explicit paths — never `git add -A` here; the worktree has untracked scratch directories like `.idea/` that must not be committed.)

---

### Task 3: `cmd/chronod` — `-nats-url` flag, fail-fast boot, bounded drain

**Files:**
- Modify: `cmd/chronod/main.go`
- Modify: `cmd/chronod/main_test.go`

**Interfaces:**
- Consumes: `pub.NewNATS`, `service.NewCore(..., pub.Publisher)`, `Core.NATSConnected` (via server gauge).
- Produces: `run(ctx context.Context, grpcAddr, httpAddr, natsURL string) error` — connects to NATS before serving; boot fails if unreachable; drains at shutdown.

- [ ] **Step 1: Update `cmd/chronod/main_test.go`**

```go
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/emir/chrono-tree/internal/pub/pubtest"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestRunNATSUnavailableIsFatal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Port 1 never answers; run must return promptly with an error.
	done := make(chan error, 1)
	go func() { done <- run(ctx, freeAddr(t), freeAddr(t), "nats://127.0.0.1:1") }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("run should fail without NATS")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not fail fast without NATS")
	}
}

func TestRunServesHTTP(t *testing.T) {
	natsURL := pubtest.Start(t)
	grpcAddr, httpAddr := freeAddr(t), freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, grpcAddr, httpAddr, natsURL) }()

	deadline := time.Now().Add(10 * time.Second)
	ok := false
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://%s/healthz", httpAddr))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				ok = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ok {
		t.Fatal("healthz never became ready")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not shut down within 15s")
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}
```

(This replaces the file's previous `run(ctx, grpcAddr, httpAddr)` test; the old test's body is folded into `TestRunServesHTTP` above.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/chronod/`
Expected: FAIL — `run` takes 3 parameters.

- [ ] **Step 3: Update `cmd/chronod/main.go`**

1. Flag:

```go
	natsURL := flag.String("nats-url", "nats://localhost:4222", "NATS server URL (trigger publishing)")
```

2. `main` passes it: `run(ctx, *grpcAddr, *httpAddr, *natsURL)`.
3. `run` connects first — before any listener — and swaps the Task-2 `pub.Noop{}` for the real publisher:

```go
func run(ctx context.Context, grpcAddr, httpAddr, natsURL string) error {
	// NATS is a boot dependency: unreachable broker is a fatal error.
	// Later outages reconnect forever; publishes during them drop-and-count.
	publisher, err := pub.NewNATS(natsURL)
	if err != nil {
		return fmt.Errorf("nats: %w", err)
	}
	defer func() { _ = publisher.Close() }() // backstop; the shutdown path drains first

	now := time.Now()
	reg := prometheus.NewRegistry()
	pm := server.NewPromMetrics(reg)
	st := stats.New(now)
	core := service.NewCore(engine.DefaultConfig(), catalog.Default(), pm, st, publisher)
	defer core.Close()
	// ... rest unchanged through the shutdown sequence ...
```

4. In the shutdown sequence, after `core.Close()` and before the HTTP shutdown, add the bounded drain:

```go
	core.Close() // stops the pump, then the engine (idempotent; the defer is a backstop)
	if err := publisher.Close(); err != nil { // bounded 3s drain inside
		slog.Warn("nats drain", "err", err)
	}
```

5. Import `"github.com/emir/chrono-tree/internal/pub"`; the earlier `grpc listen`/`http listen` bodies and everything else are unchanged.

- [ ] **Step 4: Run tests**

Run: `go test ./cmd/chronod/ -race -count=1 -timeout 60s`
Expected: PASS, goleak clean.

- [ ] **Step 5: Commit**

```bash
git add cmd/chronod/
git commit -m "feat(chronod): -nats-url flag with fail-fast boot and bounded drain"
```

---

### Task 4: `cmd/chronoctl` — `alert` + `seed`

**Files:**
- Modify: `cmd/chronoctl/main.go`
- Modify: `cmd/chronoctl/demo_test.go` → rename to `cmd/chronoctl/seed_test.go`

**Interfaces:**
- Consumes: existing `chronov1` clients, `catalog.Default()`, `price.Parse/Format`, `feed.SeedBytes`.
- Produces: subcommands `alert` (unchanged) and `seed`; `runSeed(ctx context.Context, c Clients, out io.Writer, seed uint64, nAlerts int) error` — seeds and RETURNS (no watch tail).

- [ ] **Step 1: Rewrite the test as `cmd/chronoctl/seed_test.go` (delete `demo_test.go`)**

```go
package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/pub"
	"github.com/emir/chrono-tree/internal/service"
	"github.com/emir/chrono-tree/internal/stats"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestRunSeedRegistersAlerts(t *testing.T) {
	now := time.Now()
	core := service.NewCore(engine.DefaultConfig(), catalog.Default(), service.NoopMetrics{}, stats.New(now), pub.Noop{})
	t.Cleanup(core.Close)
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	chronov1.RegisterAlertServiceServer(srv, core)
	chronov1.RegisterFeedServiceServer(srv, core)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := Clients{Alerts: chronov1.NewAlertServiceClient(conn), Feed: chronov1.NewFeedServiceClient(conn)}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out bytes.Buffer
	if err := runSeed(ctx, c, &out, 7, 30); err != nil {
		t.Fatalf("runSeed: %v", err)
	}
	if got := core.AlertCount(); got != 33 { // 30 random + 3-venue BTCUSDT fan-out
		t.Fatalf("alert count = %d, want 33", got)
	}
	if !strings.Contains(out.String(), "seeded 33") {
		t.Fatalf("output missing seed count: %q", out.String())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/chronoctl/`
Expected: FAIL — `runSeed` undefined.

- [ ] **Step 3: Update `cmd/chronoctl/main.go`**

1. DELETE `printTrigger` and `runWatch` entirely.
2. Rename the `demo` flag set to `seed` semantics (keep `-n` and `-seed` flags):

```go
	seedCmd := flag.NewFlagSet("seed", flag.ExitOnError)
	seedN := seedCmd.Int("n", 1000, "alerts to seed")
	seedSeed := seedCmd.Uint64("seed", 1, "RNG seed")
```

3. Usage and dispatch:

```go
	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: chronoctl [flags] <alert|seed> ...")
		os.Exit(2)
	}
```

```go
	case "seed":
		if err := seedCmd.Parse(flag.Args()[1:]); err != nil {
			os.Exit(2)
		}
		if err := runSeed(ctx, c, os.Stdout, *seedSeed, *seedN); err != nil {
			slog.Error("seed", "err", err)
			os.Exit(1)
		}
```

(The `case "watch"` and `case "demo"` blocks are deleted. Drop the `"io"`, `"time"` imports if now unused — `time` was only used by `printTrigger`; `io` only by the watch functions.)

4. Replace `runDemo` with `runSeed` (same seeding body, no watch tail):

```go
// runSeed registers nAlerts dim-scoped ABOVE alerts at reference ±2%
// across random pairs/venues/tiers, plus one deliberate per-venue
// fan-out set on BTCUSDT, then returns. Triggers are observed on NATS
// (`nats sub chrono.triggers.>`), never through this client.
func runSeed(ctx context.Context, c Clients, out io.Writer, seed uint64, nAlerts int) error {
	cat := catalog.Default()
	syms := cat.Symbols()
	rng := rand.New(rand.NewChaCha8(*feed.SeedBytes(seed)))

	seedOne := func(sym catalog.Symbol, venue, tier string) error {
		ref, err := price.Parse(sym.Reference, sym.Decimals)
		if err != nil {
			return err
		}
		f := 0.98 + 0.04*rng.Float64() // ±2% of reference
		target := int64(float64(ref)*f + 0.5)
		_, err = c.Alerts.UpsertAlert(ctx, &chronov1.UpsertAlertRequest{
			Symbol: sym.Name, PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
			Direction: chronov1.Direction_DIRECTION_ABOVE,
			TargetPrice: price.Format(target, sym.Decimals),
			Venue:       venue, Tier: tier,
		})
		return err
	}

	venues, tiers := cat.DimValues(catalog.DimVenue), cat.DimValues(catalog.DimTier)
	for i := 0; i < nAlerts; i++ {
		sym := syms[rng.IntN(len(syms))]
		if err := seedOne(sym, venues[rng.IntN(len(venues))], tiers[rng.IntN(len(tiers))]); err != nil {
			return err
		}
	}
	// Fan-out set: same target on BTCUSDT ask, one alert per venue — the
	// "any venue" pattern is the caller's fan-out, one alert per value.
	var btc catalog.Symbol
	for _, s := range syms {
		if s.Name == "BTCUSDT" {
			btc = s
		}
	}
	for _, v := range venues {
		if err := seedOne(btc, v, catalog.Tiers[0]); err != nil {
			return err
		}
	}
	fmt.Fprintf(out, "seeded %d alerts (incl. %d-venue BTCUSDT fan-out)\n", nAlerts+len(venues), len(venues))
	return nil
}
```

(`io` stays — `out io.Writer`. The only changes from `runDemo` are the doc comment, the dropped `return runWatch(ctx, c, out)` tail, and the final print losing "; watching".)

- [ ] **Step 4: Run tests**

Run: `go test ./cmd/chronoctl/ -race -count=1 -timeout 60s`
Expected: PASS, goleak clean.

- [ ] **Step 5: Commit**

```bash
git add cmd/chronoctl/
git commit -m "refactor(chronoctl): alert + seed only; trigger watching moves to NATS"
```

---

### Task 5: proto regeneration, integration smoke, README, gates

**Files:**
- Modify: `api/proto/chrono/v1/chrono.proto`
- Regenerate: `api/gen/chrono/v1/chrono.pb.go`, `api/gen/chrono/v1/chrono_grpc.pb.go`
- Modify: `tests/integration_test.go`
- Modify: `README.md`

**Interfaces:**
- Consumes: everything from Tasks 1–4.
- Produces: the final API surface (no `WatchTriggers`, no `Trigger` message) and the acceptance evidence.

- [ ] **Step 1: Edit `api/proto/chrono/v1/chrono.proto`**

Delete, verbatim:
- the `WatchTriggers` rpc line and its three comment lines from `service AlertService` (lines 32–34),
- `message WatchTriggersRequest {}` (line 68) and its preceding blank line,
- the `message Trigger { ... }` block (lines 70–79) and its preceding blank line.

The `AlertService` service block becomes:

```protobuf
service AlertService {
  // UpsertAlert inserts an alert or atomically replaces the one with the
  // same id. The server generates a UUIDv7 id when alert_id is empty.
  // The alert is guaranteed visible to Match before the response returns.
  rpc UpsertAlert(UpsertAlertRequest) returns (UpsertAlertResponse);
  rpc CancelAlert(CancelAlertRequest) returns (CancelAlertResponse);
}
```

- [ ] **Step 2: Regenerate**

```bash
export PATH="$HOME/go/bin:$PATH"
buf generate
```

Verify no stray references remain:

```bash
grep -rn "WatchTriggers\|WatchTriggersRequest" --include="*.go" . | grep -v "^./docs"
grep -rn "chronov1\.Trigger" --include="*.go" .
go build ./... && go vet ./...
```

Expected: both greps empty; build and vet clean. (`api/gen/chrono/v1/doc_test.go` pins only enums and Register funcs — unaffected.)

- [ ] **Step 3: Rewrite the trigger leg of `tests/integration_test.go`**

Imports: add `"github.com/emir/chrono-tree/internal/pub"`, `"github.com/emir/chrono-tree/internal/pub/pubtest"`, `nats "github.com/nats-io/nats.go"`; drop nothing else (the grpc client stays for the upsert).

Changes:

1. Start NATS first and pass it to the daemon:

```go
	natsURL := pubtest.Start(t)
	// ...
	daemon := exec.CommandContext(ctx, "go", "run", "./cmd/chronod",
		"-grpc-addr", grpcAddr, "-http-addr", httpAddr, "-nats-url", natsURL)
```

2. Replace the whole WatchTriggers block (from `// Wire one alert near BTC's anchor` through the `select { case <-triggered: ... }`) with:

```go
	// Subscribe before wiring the alert, then register a BELOW alert just
	// above BTC's anchor: the first ATLAS/TOP BTCUSDT ask at ~65000
	// satisfies it, so the trigger fires within moments of the feed's
	// first matching tick.
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	sub, err := nc.SubscribeSync("chrono.triggers.>")
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	alerts := chronov1.NewAlertServiceClient(conn)
	if _, err := alerts.UpsertAlert(ctx, &chronov1.UpsertAlertRequest{
		Symbol: "BTCUSDT", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction:   chronov1.Direction_DIRECTION_BELOW,
		TargetPrice: "65100.00", Venue: "ATLAS", Tier: "TOP",
	}); err != nil {
		t.Fatal(err)
	}

	msg, err := sub.NextMsg(25 * time.Second)
	if err != nil {
		t.Fatalf("no trigger published to NATS: %v", err)
	}
	if msg.Subject != pub.Subject("ATLAS", "TOP") {
		t.Fatalf("subject = %q", msg.Subject)
	}
	var tr pub.Trigger
	if err := json.Unmarshal(msg.Data, &tr); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if tr.Symbol != "BTCUSDT" || tr.Venue != "ATLAS" || tr.Tier != "TOP" {
		t.Fatalf("trigger dims wrong: %+v", tr)
	}
```

Add `"encoding/json/v2"` as `json` to the imports (replacing `"encoding/json"`).

3. In the rate-gate loop, also assert NATS health once before polling:

```go
	// /stats must show NATS connected.
	sresp, err := http.Get("http://" + httpAddr + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	var health map[string]any
	if err := json.NewDecoder(sresp.Body).Decode(&health); err != nil {
		sresp.Body.Close()
		t.Fatal(err)
	}
	sresp.Body.Close()
	if v, _ := health["nats_connected"].(bool); !v {
		t.Fatalf("nats_connected = %v, want true", health["nats_connected"])
	}
```

(Insert this immediately before the `var rate float64` poll block; the polling loop itself is unchanged.)

- [ ] **Step 4: README update**

In `README.md`'s Services section:

- `chronod` bullet: replace the trigger-delivery sentences with: “Fired triggers are published to NATS on `chrono.triggers.{venue}.{tier}` (JSON payloads, `Nats-Msg-Id`/`Symbol` headers); chronod refuses to start without NATS and drops-and-counts publishes during outages.”
- `chronoctl` bullet becomes: “`chronoctl` — alert creation: `alert` (register one alert, print its ID), `seed` (register N dim-scoped alerts plus a per-venue BTCUSDT fan-out set).”
- Replace the demo command block:

```sh
nats-server &                    # or: docker run -p 4222:4222 nats
go run ./cmd/chronod &
go run ./cmd/chronofeed -rate 20000 &
go run ./cmd/chronoctl seed -n 1000
nats sub 'chrono.triggers.>'     # watch triggers fire
curl -s localhost:8080/stats | jq
```

- Replace the paragraph mentioning `WatchTriggers`/watcher eviction with: “Triggers are delivered through NATS — subscribe with wildcards like `chrono.triggers.ATLAS.*` or `chrono.triggers.>`. A publish that fails (NATS briefly down) is dropped and counted (`triggers_publish_dropped`), never blocking the pump — the same drop-and-log philosophy as the engine's own ring.”
- In the Packages section (if it lists the watcher/pump), update the `internal/service` line to say “trigger pump publishing to NATS” and add an `internal/pub` line: “NATS publisher: subject mapping, JSON payload, headers (`Noop` for tests).”

- [ ] **Step 5: Run everything (the gates)**

```bash
go build ./... && go vet ./...
go test ./... -race -count=1
go test ./internal/service -run 'Oracle|Stress|Publish' -race -count=1 -v
go test -tags integration ./tests -run Integration -v -timeout 180s
git diff --stat main -- engine price   # must be empty
```

Expected: all green, including the unchanged ≥19.5k ticks/s rate gate inside the integration test. If the rate gate fails on a loaded machine, re-run once on idle; if it still fails, record the measured number in the task report — do not silently relax the assertion.

- [ ] **Step 6: Commit**

```bash
git add api/ tests/ README.md
git commit -m "feat(api)!: drop WatchTriggers; NATS is the trigger delivery path"
```

---

## Post-plan notes for the executor

- If `nats.go`/`nats-server` versions resolve with API mismatches (e.g. `nats.NewMsg` header API), stop and report BLOCKED with the compile errors — do not fork the wire contract.
- `gofmt` the changed `internal/stats/stats.go` field block after pasting (alignment of the longer `TriggersPublishDropped` name).
- The engine and price packages must show ZERO diffs at the end of every task.
- Go 1.27: `encoding/json/v2` imports bind as `json` — do not add an alias.
