# Burst Absorption Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate burst-trigger loss and cut sustained-firing CPU at the service layer: a `-ring` flag (default 1M slots) and a two-stage pipelined trigger pump (4096-wide enrich stage overlapping a dedicated flip stage).

**Architecture:** All changes live in the service layer — `internal/service/core.go` splits `deliverBatch` into `enrichBatch` (drain → BatchGet → publish, returns fired slice) and `flipper` (MarkTriggeredBatch + gauges), connected by a capacity-2 channel; `cmd/chronod/main.go` gains the `-ring` flag applied to `engine.Config.RingSize`. `engine/` is frozen and its signature surface untouched (`RingSize` is already a public `Config` field). Proof is empirical: re-run three campaign scenarios and compare against `docs/perf/2026-09-06-campaign/`.

**Tech Stack:** Go 1.27, standard `go test` (bufconn gRPC env in `internal/service`), the existing perf harness (`scripts/perf/run.sh`).

**Spec:** `docs/superpowers/specs/2026-09-06-burst-absorption-design.md`

## Global Constraints

- `engine/` is frozen: no engine file changes; `Config.RingSize` is set from outside only.
- `NewCore`'s signature is unchanged.
- Publish order must stay single-threaded (one enrich goroutine); flip order is free (idempotent).
- Error semantics preserved exactly: BatchGet failure → count fires, records stay active; flip failure → log "records stay active until restart", continue.
- `Close()` must not return until the flipper has flushed every in-flight batch (store writes complete) — and still before `eng.Close()`.
- Every commit leaves the tree green: `go vet ./... && go test ./... -count=1`.
- Campaign re-runs (Task 3) require a quiet box, strictly serial, same as the original campaign.

---

### Task 1: Two-stage pump pipeline

**Files:**
- Modify: `internal/service/core.go` (struct fields ~line 97-99, `NewCore` ~line 137-139, `Close` ~line 205-209, `pump`/`deliverBatch` ~lines 541-627)
- Test: `internal/service/pump_test.go` (new file)

**Interfaces:**
- Consumes: existing `c.eng.Triggers().PopBatch`, `c.store.BatchGet`, `c.store.MarkTriggeredBatch`, `alertstore.Fired`.
- Produces: `func (c *Core) enrichBatch(batch []engine.Trigger) []alertstore.Fired` and `func (c *Core) flipper()`; `Core` gains `flipCh chan []alertstore.Fired` and `flipDone chan struct{}`; `deliverBatch` is deleted. `Close` waits on `flipDone`.

- [ ] **Step 1: Write the failing tests**

Create `internal/service/pump_test.go`:

```go
package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
)

// upsertCluster seeds n ABOVE alerts on symbol at targets 10.00..10.(n-1),
// all fired by an ask at 20.00.
func upsertCluster(t *testing.T, e *testEnv, symbol string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
			Symbol: symbol, PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
			Direction:   chronov1.Direction_DIRECTION_ABOVE,
			TargetPrice: fmt.Sprintf("10.%02d", i), Venue: "ATLAS", Tier: "TOP",
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// waitFlips polls store truth until n records sit in triggered.
func waitFlips(t *testing.T, e *testEnv, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, tr, _, err := e.store.Counts(); err == nil && int(tr) == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("store flips did not reach %d within 3s", n)
}

// TestPumpPipelineFlushesOnClose: Close must not return until the flipper
// has written every in-flight flip — a shutdown mid-pipeline loses nothing.
func TestPumpPipelineFlushesOnClose(t *testing.T) {
	e := newEnv(t)
	const n = 30
	upsertCluster(t, e, "FLUSH", n)
	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("FLUSH", "9.99", "20.00", "ATLAS", "TOP"),
	}}); err != nil {
		t.Fatal(err)
	}
	waitTriggers(t, e, n) // enriched + published; flips may still be in flight
	e.core.Close()        // cleanup closes again — idempotent by contract
	a, tr, cn, err := e.store.Counts()
	if err != nil {
		t.Fatal(err)
	}
	if tr != n || a != 0 || cn != 0 {
		t.Fatalf("after close: active=%d triggered=%d cancelled=%d, want 0/%d/0", a, tr, cn, n)
	}
	if got := e.core.AlertsByState()[StateTriggered]; got != n {
		t.Fatalf("triggered gauge = %d, want %d (gauges must match store truth)", got, n)
	}
}

// TestPumpGaugesTrackStore: gauges update on the flip stage; after the
// pipeline quiesces across multiple fire waves they equal store truth.
func TestPumpGaugesTrackStore(t *testing.T) {
	e := newEnv(t)
	const n = 10
	upsertCluster(t, e, "GAUGE", n)
	// Wave 1: ask 10.02 fires targets 10.00-10.02 (3); wave 2: ask 10.09
	// fires the rest (7) — two flip batches.
	for _, ask := range []string{"10.02", "10.09"} {
		if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
			tick("GAUGE", "9.99", ask, "ATLAS", "TOP"),
		}}); err != nil {
			t.Fatal(err)
		}
	}
	waitTriggers(t, e, n)
	waitFlips(t, e, n)
	a, tr, _, err := e.store.Counts()
	if err != nil {
		t.Fatal(err)
	}
	got := e.core.AlertsByState()
	if got[StateTriggered] != int(tr) || got[StateActive] != int(a) {
		t.Fatalf("gauges (%v) != store (active=%d triggered=%d)", got, a, tr)
	}
	if tr != n {
		t.Fatalf("store triggered = %d, want %d", tr, n)
	}
}
```

Note: `tick` and `runTicks` come from `feed_test.go` (same package); `e.store`, `e.core`, `e.alerts()` from `testenv_test.go`.

Spec deviation, documented: the spec's Testing section lists a flip-failure test ("records stay active, warning logs"). `Core.store` is a concrete `*alertstore.Store`, not an interface — deterministic fault injection would require a store seam refactor, which is out of scope (YAGNI). The flip-failure branch is preserved verbatim in `flipper` (Step 3); the task reviewer must verify the branch and its log line match the old `deliverBatch` exactly. The flush-on-close test covers the branch's observable complement (flips that DO land are never lost).

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/service/ -run 'TestPumpPipelineFlushesOnClose|TestPumpGaugesTrackStore' -count=1`
Expected: both FAIL — `TestPumpPipelineFlushesOnClose` sees `triggered < 30` after Close only if the flip lagged; more reliably, compile is clean but the flush assertion may pass by luck on the old synchronous code. **If a test passes against the OLD code, that is acceptable for these two** (they pin the contract, not just the bug); the required red proof is Step 3's structural change keeping them green. Record actual outcomes in the report.

- [ ] **Step 3: Implement the pipeline**

In `internal/service/core.go`:

1. Struct fields (after `pumpDone chan struct{}`, ~line 99) add:

```go
	flipCh   chan []alertstore.Fired // enrich → flipper; capacity 2 = backpressure
	flipDone chan struct{}
```

2. `NewCore` (~lines 137-139) becomes:

```go
	c.pumpCtx, c.pumpCancel = context.WithCancel(context.Background())
	c.pumpDone = make(chan struct{})
	c.flipCh = make(chan []alertstore.Fired, 2)
	c.flipDone = make(chan struct{})
	go c.pump()
	go c.flipper()
```

3. `Close` (~lines 205-209) becomes — the flipper flushes before the engine stops:

```go
// Close stops the pump first (it is the only Triggers() consumer, and the
// engine contract requires submitters to stop before Close), waits for the
// flipper to write every in-flight batch, then stops the engine.
func (c *Core) Close() {
	c.pumpCancel()
	<-c.pumpDone
	<-c.flipDone
	c.eng.Close()
}
```

4. Replace `pump` and `deliverBatch` entirely with:

```go
// pumpBatch is the ring drain width: one BatchGet read tx per 4096
// triggers. The old 64-wide batches made store-transaction overhead the
// pump's dominant cost (~1.7 ms/trigger, ~4.4k flips/s — 2026-09-06
// campaign). 4096 keeps a read tx well under a second of work while
// amortizing per-tx cost across three orders of magnitude more triggers.
const pumpBatch = 4096

// pump drains the engine's trigger ring, enriches and publishes each
// batch (enrichBatch), and hands the flips to the flipper goroutine over
// a capacity-2 channel: enriching batch N+1 overlaps flipping batch N.
// It is the ONLY Triggers() consumer. Publish order is this goroutine's
// order; flip order is free (writes are idempotent).
func (c *Core) pump() {
	defer close(c.pumpDone)
	buf := make([]engine.Trigger, pumpBatch)
	for {
		n := c.eng.Triggers().PopBatch(buf)
		if n == 0 {
			select {
			case <-c.pumpCtx.Done():
				close(c.flipCh)
				return
			case <-time.After(500 * time.Microsecond):
			}
			continue
		}
		if fired := c.enrichBatch(buf[:n]); len(fired) > 0 {
			c.flipCh <- fired
		}
	}
}

// flipper is the pipeline's write stage: one MarkTriggeredBatch per
// enriched batch, then the active/triggered gauges — they track store
// truth, which lands here. Runs until the pump closes the channel and
// every in-flight batch is written.
func (c *Core) flipper() {
	defer close(c.flipDone)
	for fired := range c.flipCh {
		flipped, err := c.store.MarkTriggeredBatch(fired)
		if err != nil {
			// Same contract as the synchronous era: records stay active
			// and re-arm via replay at the next restart.
			slog.Warn("mark triggered failed; records stay active until restart", "err", err)
			continue
		}
		c.active.Add(-int64(flipped))
		c.triggered.Add(int64(flipped))
	}
}

// enrichBatch is the pipeline's read stage: one read tx resolves every
// alert in a drained batch, each active alert's trigger is published,
// and the published IDs return for the flipper to write. A store read
// failure counts the batch as fired and leaves the records active — the
// engine has already dropped its refs at fire time, so they re-enter via
// replay at the next restart: visible duplication beats silent loss.
func (c *Core) enrichBatch(batch []engine.Trigger) []alertstore.Fired {
	ids := make([]engine.AlertID, len(batch))
	for i := range batch {
		ids[i] = batch[i].ID
	}
	recs, err := c.store.BatchGet(ids)
	now := c.now()
	if err != nil {
		for range batch {
			c.stats.TriggersFired.Add(1)
			c.stats.FireRate.Add(1, now)
		}
		slog.Warn("trigger enrichment failed; batch lost, records stay active until restart", "err", err)
		return nil
	}
	fired := make([]alertstore.Fired, 0, len(batch))
	for i := range batch {
		tr := &batch[i]
		c.stats.TriggersFired.Add(1)
		c.stats.FireRate.Add(1, now)
		a, ok := recs[tr.ID]
		if !ok || a.State != alertstore.StateActive {
			continue // unknown or no-longer-active: count, don't ship
		}
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
			continue
		}
		if !c.pubHealthy.Swap(true) {
			slog.Info("trigger publishing recovered")
		}
		c.stats.TriggersPublished.Add(1)
		c.metrics.TriggerPublished()
		fired = append(fired, alertstore.Fired{ID: a.ID, Price: engine.Price(tr.Price), At: tr.TS})
	}
	return fired
}
```

The body of `enrichBatch`'s loop is byte-identical to the old `deliverBatch` loop — the only change is `return fired` instead of the trailing flip block (that block moved into `flipper` verbatim).

- [ ] **Step 4: Run the full service suite**

Run: `go vet ./internal/service/ && go test ./internal/service/ -race -count=1`
Expected: PASS — the two new tests, plus every existing feed/stress/oracle/replay test (external pump contract unchanged). The stress test is the key concurrency witness for the new goroutine.

- [ ] **Step 5: Full suite + commit**

Run: `go vet ./... && go test ./... -count=1` — PASS.

```bash
git add internal/service/core.go internal/service/pump_test.go
git commit -m "perf(service): pipelined trigger pump — 4096-wide enrich overlapping flip"
```

---

### Task 2: chronod `-ring` flag

**Files:**
- Modify: `cmd/chronod/main.go` (flags ~line 39-43, `run` signature ~line 51, core construction ~line 87)

**Interfaces:**
- Consumes: Task 1's core (no signature change); `engine.Config.RingSize`.
- Produces: `-ring` int flag, default `1048576`, applied before `NewCore`.

- [ ] **Step 1: Add the flag and wire it**

In `main()`, after the `dbPath` flag (line 42):

```go
	ring := flag.Int("ring", 1<<20, "trigger ring capacity, rounded to a power of two (bytes: 32 B/slot ≈ 32 MiB at default); size for the largest simultaneous burst")
```

Change the `run` call and signature to carry it (currently `run(ctx, *grpcAddr, *httpAddr, *natsURL, *dbPath)` → add `ringSize int` as the last parameter), and in `run`'s body replace:

```go
	core := service.NewCore(engine.DefaultConfig(), catalog.Empty(), pm, st, publisher, store)
```

with:

```go
	cfg := engine.DefaultConfig()
	cfg.RingSize = ringSize // operational knob: burst absorption ceiling
	core := service.NewCore(cfg, catalog.Empty(), pm, st, publisher, store)
```

- [ ] **Step 2: Verify**

Run: `go vet ./... && go test ./... -count=1`
Expected: PASS — nothing behavioral changed at the default (engine rounds 1<<20 to itself).

Boot check both the default and an explicit value (no unit tests exist for main; this is the test):

```bash
go build -o "$CLAUDE_JOB_DIR/tmp/chronod-ring" ./cmd/chronod
"$CLAUDE_JOB_DIR/tmp/chronod-ring" -nats-url "" -ring 131072 -db "$CLAUDE_JOB_DIR/tmp/ring.bbolt" -grpc-addr :19099 -http-addr :18089 &
CPID=$!
sleep 2
curl -sf localhost:18089/stats | jq '.uptime_sec'
kill $CPID; wait $CPID 2>/dev/null
```
Expected: boots clean, `/stats` answers; a second boot without `-ring` behaves the same.

- [ ] **Step 3: Commit**

```bash
git add cmd/chronod/main.go
git commit -m "feat(chronod): -ring flag sizes the trigger ring (default 1M slots)"
```

---

### Task 3: Post-optimization campaign proof

**Files:**
- Create: `docs/perf/2026-09-06-campaign-postopt/{service-trickle,service-burst-100k,service-burst-500k}/` (via the harness)
- Modify: `docs/perf/2026-09-06-campaign/REPORT.md` (append comparison section)

**Interfaces:**
- Consumes: `scripts/perf/run.sh`, the finished Tasks 1-2 binaries.
- Produces: before/after table in REPORT.md; `analysis.md` per postopt run dir.

- [ ] **Step 1: Pre-flight**

Verify the box is quiet (`free -g` ≥ 6 GB available, load < 1 on 12 cores) and no stray chronod/benchfeed processes. Runs are STRICTLY SERIAL; no builds/tests while a scenario executes.

- [ ] **Step 2: Run the three scenarios**

```bash
for scn in trickle burst-100k burst-500k; do
  bash scripts/perf/run.sh service "$scn" docs/perf/2026-09-06-campaign-postopt || echo "service/$scn INVALID"
done
```
Each ~7-10 min. All three must end `VALID` — gates unchanged; the burst conservation gate (`fired + ring_dropped == cluster`) is now expected to pass with `ring_dropped = 0`.

- [ ] **Step 3: Write per-run analyses**

For each postopt run dir, write `analysis.md` (headline table + delta vs the same scenario in `docs/perf/2026-09-06-campaign/`):

| before (campaign) | metric | expected after |
|---|---|---|
| trickle: 83.3% core, 0 drops | CPU % of one core | substantially lower |
| burst-100k: 65,600 delivered / 34,400 lost / 15.1 s | delivered / ring_dropped / drain | 100,000 / 0 / seconds |
| burst-500k: 65,664 delivered / 434,336 lost / 14.9 s | delivered / ring_dropped / drain | 500,000 / 0 / low seconds |

Report actuals whatever they are — these are targets from the campaign baseline, not gates (the validity gates alone decide VALID).

- [ ] **Step 4: Append the comparison to REPORT.md**

Add a section `## After the burst-absorption fix (postopt)` to `docs/perf/2026-09-06-campaign/REPORT.md`: a three-row before/after table (CPU, RSS peak, delivered, ring-dropped, drain), one paragraph on whether targets were met, and a pointer to the postopt directory. Note the ring default change (65,536 → 1,048,576) and the pump pipeline as the two levers.

- [ ] **Step 5: Full suite + commit**

Run: `go vet ./... && go test ./... -count=1` — PASS.

```bash
git add docs/perf/2026-09-06-campaign-postopt/ docs/perf/2026-09-06-campaign/REPORT.md
git commit -m "docs(perf): postopt campaign — burst absorption before/after"
```
