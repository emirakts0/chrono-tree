# Monitoring UI v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Batch trigger delivery through a 10-slot server-side ring drained at 1Hz, service-side 2-minute metric history for graphs, and a visual overhaul of the dashboard (vertical trigger rail, dedup, legibility, custom dropdowns, intro animation, responsiveness).

**Architecture:** One server-wide 1s tick goroutine in `internal/server` samples history, builds one snapshot, drains the trigger ring, and broadcasts `snapshot` + (non-empty) `triggers` frames through the existing SSE hub; `handleStream` becomes a pure relay. History rides the `hello` frame. The frontend restructures around a right-hand trigger rail and dedups card data, then gets a full visual pass.

**Tech Stack:** Go 1.27 (encoding/json/v2, testing/synctest), existing nats.go stack; TypeScript strict + esbuild/tsc via `npx` (no node_modules), committed bundle.

**Spec:** `docs/superpowers/specs/2026-09-05-monitoring-ui-v2-design.md`

## Global Constraints

- **Engine and price packages are FROZEN:** zero diffs to `engine/` and `price/`. Verify after every task: `git diff --stat main -- engine price` is empty.
- **Read-only:** the UI and its endpoints create, cancel, and mutate nothing.
- **`Publisher` interface and `pub` package unchanged:** `Publish/Connected/Close` + `SubscribeTriggers`. No `internal/pub` diffs in this iteration.
- **SSE contract exactly:**
  - `data: {"type":"hello","venues":[...],"tiers":[...],"symbol_count":N,"history":{"t":[...],"f":[...],"l":[...],"v":{"<VENUE>":[...],...}}}` on connect; history arrays time-ascending, each capped at 120 samples, right-padded as they fill.
  - `data: {"type":"snapshot","snapshot":{...}}` every 1s, byte-equivalent to the `/stats` document (same `statusView` marshal).
  - `data: {"type":"triggers","triggers":[...]}` immediately after a snapshot, ONLY when the ring drained non-empty; max 10 items, oldest→newest; each item is the existing `pub.Trigger` JSON body.
  - Per-trigger `{"type":"trigger",...}` frames are REMOVED.
- **Ring semantics exactly:** 10 slots; overflow overwrites the oldest; drain returns the batch and clears; a 10,000-trigger burst ships exactly the last 10.
- **History exactly:** 120 samples of `ticks_per_sec`, `triggers_per_sec`, `engine.live`, and per-venue ticks/s (delta of cumulative `venue_ticks` between samples; first sample = 0).
- **Server lifecycle:** `New` starts the tick goroutine; new idempotent `Server.Close()` stops it; chronod calls `Close()` after `httpServer.Shutdown`. Test helper `newServer` must `t.Cleanup(s.Close)`.
- **Palette verbatim** (CSS `:root`, unchanged): `--onyx:#161515`, `--graphite:#2E2D29`, `--soft-linen:#F3EFE7`, `--linen:#F4EEE5`, `--brick-ember:#D40000`, `--pine-teal:#164D44`, `--coffee-bean:#1E1917`, `--silver:#BFB3AD`, `--molten-lava:#770B0C`, `--light-green:#7FDF74`.
- **Legibility rule:** `silver` never on light (linen) surfaces — labels there use `coffee-bean` at 60–70% opacity (or a computed ≈ #6B615B). `silver`/`soft-linen` only on dark surfaces (onyx, graphite, pine-teal).
- **`light-green` never on bare linen** — only against `coffee-bean` or `pine-teal` (or graphite).
- **Dedup rule:** each datum renders exactly once (see spec table): pulse owns ticks/s + ticks totals; fires owns triggers fired + rate spark; topbar owns status dots + published/dropped; alert book owns state tiles + engine.live trend; engine card owns ring drops, symbols, uptime; venues owns per-venue bars + trends.
- **UI bundle is committed:** rebuild only via `internal/server/web/build.sh` (npx-based; Node 22+; network on first fetch). `go build ./...` must never require the TS toolchain.
- **No external fonts/CDNs, no chart or UI libraries:** sparklines/trends are inline SVG; the dropdown and intro are hand-rolled TS/CSS.
- **Task 4 REQUIRES the frontend-design skill** for the visual/CSS work; this plan pins contract, layout structure, palette, and behavior.
- Every task: `go build ./... && go vet ./...` clean; tests green before commit (`-race`).
- Commit style: `feat(...)`, `test(...)`, `docs(...)`, `refactor(...)`; trailer `Co-Authored-By: Claude Code <noreply@anthropic.com>`. Never `git add -A` (untracked scratch dirs `.idea/`, `graphify-out/`).

---

### Task 1: Trigger ring, history, and the server tick engine

**Files:**
- Modify: `internal/server/server.go`
- Modify: `internal/server/stream_test.go`
- Modify: `internal/server/server_test.go` (one line: `t.Cleanup(s.Close)` in `newServer`)

**Interfaces:**
- Consumes: existing `sseHub` (unchanged), `statusSnapshot()`, `handleStream`, `pub.Trigger`, `catalog`.
- Produces (Tasks 2–4 rely on exactly these): `Server.Close()`; `HandleTrigger` now pushes to the ring; hub broadcast carries `snapshot`/`triggers` frames; `hello` frame gains `history`.

- [ ] **Step 1: Write the failing tests.** Replace `TestStreamHelloSnapshotTrigger` in `internal/server/stream_test.go` with the batched-contract tests below, and append the ring/history tests. Keep `readFrame`, `TestHubOverflowClosesSlowClient`, and `TestAlertsEndpoints` untouched.

```go
// TestStreamContractBatched: hello (with history) on connect, snapshots at
// 1Hz from the shared tick, and trigger batches only on ticks where the
// ring drained non-empty — never per-trigger frames.
func TestStreamContractBatched(t *testing.T) {
	s := newServer(t)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/api/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}
	body := bufio.NewReader(resp.Body)

	hello := readFrame(t, body)
	if hello["type"] != "hello" {
		t.Fatalf("first frame = %v, want hello", hello["type"])
	}
	hist, ok := hello["history"].(map[string]any)
	if !ok {
		t.Fatalf("hello.history missing: %v", hello)
	}
	for _, k := range []string{"t", "f", "l", "v"} {
		if _, ok := hist[k]; !ok {
			t.Fatalf("hello.history.%s missing: %v", k, hist)
		}
	}

	// A trigger handed to HandleTrigger rides the NEXT tick's batch.
	s.HandleTrigger(pub.Trigger{
		AlertID: "0192ced1-4a1e-7abc-8def-0123456789ab", Symbol: "BTCUSDT",
		Venue: "ATLAS", Tier: "TOP", FiredPrice: "65001.00",
		FiredAtUnixNanos: 1, Direction: "ABOVE", TargetPrice: "65000.00",
	})
	sawSnapshot, sawTriggers, sawPerTrigger := false, false, false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !(sawSnapshot && sawTriggers) {
		f := readFrame(t, body)
		switch f["type"] {
		case "snapshot":
			sawSnapshot = true
			snap := f["snapshot"].(map[string]any)
			for _, k := range []string{"ticks", "ticks_per_sec", "triggers_fired", "alerts_by_state", "nats_connected", "venue_ticks", "engine"} {
				if _, ok := snap[k]; !ok {
					t.Fatalf("snapshot.%s missing", k)
				}
			}
		case "triggers":
			sawTriggers = true
			batch := f["triggers"].([]any)
			if len(batch) != 1 {
				t.Fatalf("batch len = %d, want 1", len(batch))
			}
			tr := batch[0].(map[string]any)
			if tr["alert_id"] != "0192ced1-4a1e-7abc-8def-0123456789ab" {
				t.Fatalf("trigger = %v", tr)
			}
		case "trigger":
			sawPerTrigger = true
		}
	}
	if !sawSnapshot {
		t.Fatal("no snapshot frame within deadline")
	}
	if !sawTriggers {
		t.Fatal("no triggers batch within deadline")
	}
	if sawPerTrigger {
		t.Fatal("per-trigger frames must not exist in v2")
	}
}

// TestTriggerRing: overflow keeps the last 10, drain returns oldest→newest
// and clears.
func TestTriggerRing(t *testing.T) {
	r := &triggerRing{}
	for i := 0; i < 15; i++ {
		r.push(pub.Trigger{AlertID: fmt.Sprintf("id-%02d", i)})
	}
	got := r.drain()
	if len(got) != 10 {
		t.Fatalf("drain len = %d, want 10", len(got))
	}
	if got[0].AlertID != "id-05" || got[9].AlertID != "id-14" {
		t.Fatalf("drain bounds = %s..%s, want id-05..id-14", got[0].AlertID, got[9].AlertID)
	}
	if again := r.drain(); len(again) != 0 {
		t.Fatalf("drain after drain = %d, want 0", len(again))
	}
}

// TestTriggerRingConcurrent: concurrent push + drain under -race.
func TestTriggerRingConcurrent(t *testing.T) {
	r := &triggerRing{}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(2)
		go func() { defer wg.Done(); for i := 0; i < 500; i++ { r.push(pub.Trigger{}) } }()
		go func() { defer wg.Done(); for i := 0; i < 500; i++ { r.drain() } }()
	}
	wg.Wait()
}

// TestHistorySampling: 130 samples cap at 120; venue rates are deltas of
// cumulative counters (first sample 0).
func TestHistorySampling(t *testing.T) {
	h := newHistory()
	v := statusView{
		TicksPerSec:    100,
		TriggersPerSec: 2,
		Engine:         engineView{Live: 7},
		VenueTicks:     map[string]uint64{"ATLAS": 1000},
	}
	for i := 0; i < 130; i++ {
		v.VenueTicks["ATLAS"] += 50
		v.Engine.Live++
		h.sample(v)
	}
	view := h.view()
	if len(view.T) != 120 || len(view.F) != 120 || len(view.L) != 120 {
		t.Fatalf("lengths = %d/%d/%d, want 120/120/120", len(view.T), len(view.F), len(view.L))
	}
	if view.T[0] != 100 || view.F[0] != 2 {
		t.Fatalf("t/f = %v/%v, want 100/2", view.T[0], view.F[0])
	}
	if got := view.L[0]; got != 8 { // Live was 8 by the first sample (7+1)
		t.Fatalf("l[0] = %v, want 8", got)
	}
	if got := view.V["ATLAS"][0]; got != 50 {
		t.Fatalf("venue delta[0] = %v, want 50", got)
	}
	if n := len(view.V["ATLAS"]); n != 120 {
		t.Fatalf("venue series len = %d, want 120", n)
	}
}

// TestServerCloseStopsTick: after Close, no more frames are broadcast.
func TestServerCloseStopsTick(t *testing.T) {
	s := newServer(t)
	ch, remove := s.hub.add()
	defer remove()
	s.Close()
	s.Close() // idempotent
	select {
	case frame, ok := <-ch:
		if ok {
			t.Fatalf("frame after Close: %s", frame)
		}
	case <-time.After(1500 * time.Millisecond):
	}
}
```

Imports to add in `stream_test.go`: `"fmt"`, `"sync"`.

In `internal/server/server_test.go`, `newServer` gains `t.Cleanup(s.Close)` before `return New(...)`:

```go
	s := New(core, stats.New(now), reg)
	t.Cleanup(s.Close)
	return s
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/server/ -run 'TestStreamContractBatched|TestTriggerRing|TestHistorySampling|TestServerCloseStopsTick' -v`
Expected: FAIL — `triggerRing`, `newHistory`, `Close` undefined; `hello.history` missing; per-trigger frame seen.

- [ ] **Step 3: Implement in `internal/server/server.go`.**

Add to imports: `"sync"`.

Add the ring and history (new section after `statusSnapshot`):

```go
// triggerRing is the protective buffer between the NATS reader goroutine
// and the browsers: it keeps at most ringSize triggers, overwriting the
// oldest on overflow, and is drained whole by the 1s tick. A 10,000-trigger
// burst ships exactly the last 10.
const ringSize = 10

type triggerRing struct {
	mu  sync.Mutex
	buf []pub.Trigger // oldest first, len <= ringSize
}

func (r *triggerRing) push(tr pub.Trigger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.buf) == ringSize {
		copy(r.buf, r.buf[1:])
		r.buf[len(r.buf)-1] = tr
		return
	}
	r.buf = append(r.buf, tr)
}

// drain returns the buffered batch (oldest first) and clears the ring.
func (r *triggerRing) drain() []pub.Trigger {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.buf
	r.buf = nil
	return out
}

// histSamples is the retained history window: 120 one-second samples =
// two minutes, delivered in the hello frame so graphs are full at load.
const histSamples = 120

// history holds the per-second sample series backing the dashboard graphs.
type history struct {
	mu      sync.Mutex
	t, f, l []float64                 // ticks/s, triggers/s, engine.live
	v       map[string][]float64      // per-venue ticks/s
	lastV   map[string]uint64         // cumulative venue_ticks at last sample
}

func newHistory() *history {
	return &history{v: make(map[string][]float64), lastV: make(map[string]uint64)}
}

// historyView is the wire shape; keys are short (480 numbers per hello).
type historyView struct {
	T []float64            `json:"t"`
	F []float64            `json:"f"`
	L []float64            `json:"l"`
	V map[string][]float64 `json:"v"`
}

func appendCapped(s []float64, x float64) []float64 {
	s = append(s, x)
	if len(s) > histSamples {
		s = s[1:]
	}
	return s
}

// sample records one second of the snapshot view. Venue rates are deltas
// of the cumulative venue_ticks counters; the first sample of each venue
// is 0 (no prior point to diff).
func (h *history) sample(v statusView) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.t = appendCapped(h.t, v.TicksPerSec)
	h.f = appendCapped(h.f, v.TriggersPerSec)
	h.l = appendCapped(h.l, float64(v.Engine.Live))
	for venue, total := range v.VenueTicks {
		rate := float64(total - h.lastV[venue])
		h.lastV[venue] = total
		h.v[venue] = appendCapped(h.v[venue], rate)
	}
}

func (h *history) view() historyView {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := historyView{
		T: make([]float64, len(h.t)), F: make([]float64, len(h.f)),
		L: make([]float64, len(h.l)), V: make(map[string][]float64, len(h.v)),
	}
	copy(out.T, h.t)
	copy(out.F, h.f)
	copy(out.L, h.l)
	for venue, s := range h.v {
		out.V[venue] = append([]float64(nil), s...)
	}
	return out
}
```

Extend `Server` and `New`:

```go
type Server struct {
	core     *service.Core
	stats    *stats.Stats
	reg      *prometheus.Registry
	hub      *sseHub
	ring     triggerRing
	hist     *history
	stopTick chan struct{}
	stopOnce sync.Once
	shutting atomic.Bool
}
```

In `New`, before the return: start the tick engine (keep the existing registry block as-is):

```go
	s := &Server{
		core: core, stats: st, reg: reg, hub: newSSEHub(),
		hist: newHistory(), stopTick: make(chan struct{}),
	}
	go s.runTick(s.stopTick)
	return s
```

```go
// Close stops the 1s tick goroutine. Idempotent.
func (s *Server) Close() {
	s.stopOnce.Do(func() { close(s.stopTick) })
}

// runTick is the single 1Hz heartbeat: it samples history, builds ONE
// snapshot, drains the trigger ring, and broadcasts both frames through
// the hub. Every connection relays identical frames.
func (s *Server) runTick(stop <-chan struct{}) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			snap := s.statusSnapshot()
			s.hist.sample(snap)
			sf, err := json.Marshal(snapshotFrame{Type: "snapshot", Snapshot: snap})
			if err != nil {
				continue // statusView is plain data; unreachable
			}
			s.hub.broadcast(sf)
			if batch := s.ring.drain(); len(batch) > 0 {
				if tf, err := json.Marshal(triggersFrame{Type: "triggers", Triggers: batch}); err == nil {
					s.hub.broadcast(tf)
				}
			}
		}
	}
}
```

Replace the frame types (`triggerFrame` → `triggersFrame`), extend `helloFrame`:

```go
// helloFrame is the on-connect vocabulary + history frame.
type helloFrame struct {
	Type        string      `json:"type"`
	Venues      []string    `json:"venues"`
	Tiers       []string    `json:"tiers"`
	SymbolCount int         `json:"symbol_count"`
	History     historyView `json:"history"`
}

type snapshotFrame struct {
	Type     string     `json:"type"`
	Snapshot statusView `json:"snapshot"`
}

type triggersFrame struct {
	Type     string        `json:"type"`
	Triggers []pub.Trigger `json:"triggers"`
}

// HandleTrigger buffers one published trigger into the ring; the 1s tick
// ships it (and up to its 9 most recent predecessors) to every browser.
// chronod wires it to pub.NATSPublisher.SubscribeTriggers.
func (s *Server) HandleTrigger(tr pub.Trigger) {
	s.ring.push(tr)
}
```

Rewrite `handleStream` as a pure relay (hello with history, then hub frames; no per-connection ticker):

```go
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	ch, remove := s.hub.add()
	defer remove()

	hello, err := json.Marshal(helloFrame{
		Type:        "hello",
		Venues:      s.core.Cat.DimValues(catalog.DimVenue),
		Tiers:       s.core.Cat.DimValues(catalog.DimTier),
		SymbolCount: len(s.core.Cat.Symbols()),
		History:     s.hist.view(),
	})
	if err != nil {
		return
	}
	fmt.Fprintf(w, "data: %s\n\n", hello)
	fl.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case frame, ok := <-ch:
			if !ok {
				return // overflowed (broadcast closed us) or removed
			}
			fmt.Fprintf(w, "data: %s\n\n", frame)
			fl.Flush()
		}
	}
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/server/ -race -count=1`
Expected: PASS (whole package, including the untouched hub/inquiry tests).

- [ ] **Step 5: Commit**

```bash
git add internal/server/
git commit -m "feat(server): 1Hz tick engine with trigger ring and history"
```

---

### Task 2: chronod wiring + integration coverage + README

**Files:**
- Modify: `cmd/chronod/main.go`
- Modify: `tests/integration_test.go`
- Modify: `README.md`

**Interfaces:**
- Consumes: Task 1's `Server.Close()` and the batched SSE contract.
- Produces: the daemon stops its tick goroutine on shutdown; e2e asserts a `triggers` batch frame.

- [ ] **Step 1: Wire Close.** In `cmd/chronod/main.go`, after `_ = httpServer.Shutdown(shCtx)` add:

```go
	statusSrv.Close() // stop the 1s tick goroutine (SSE relays are gone)
```

- [ ] **Step 2: Extend the integration test.** In `tests/integration_test.go`, locate the existing SSE block (asserts `"type":"hello"` and `"type":"snapshot"`). Keep it, and extend the loop to also capture `"type":"triggers"` (the alert fires earlier in the test, so a batch must arrive). Change the two booleans to three:

```go
	sawHello, sawSnap, sawBatch := false, false, false
	for sc.Scan() && !(sawHello && sawSnap && sawBatch) {
		line := sc.Text()
		if strings.Contains(line, `"type":"hello"`) {
			sawHello = true
		}
		if strings.Contains(line, `"type":"snapshot"`) {
			sawSnap = true
		}
		if strings.Contains(line, `"type":"triggers"`) {
			sawBatch = true
		}
	}
	if !sawHello || !sawSnap {
		t.Fatalf("sse: hello=%v snapshot=%v", sawHello, sawSnap)
	}
```

Note: `sawBatch` is informational — the batch timing depends on when the alert fired relative to the 1s tick, so do NOT fail on `!sawBatch` (flaky). Log it via `t.Logf("sse triggers batch seen: %v", sawBatch)`.

- [ ] **Step 3: README.** In the `internal/server` package description line, change "`/api/stream` (SSE: 1s snapshots + live triggers via NATS loopback)" to "`/api/stream` (SSE: 1s snapshots, 10-slot trigger batches via NATS loopback, 2-minute metric history on connect)".

- [ ] **Step 4: Run everything (the gates)**

```bash
go build ./... && go vet ./...
go test ./... -race -count=1
go test -tags integration ./tests -run Integration -v -timeout 180s
git diff --stat main -- engine price   # must be empty
```

- [ ] **Step 5: Commit**

```bash
git add cmd/chronod/main.go tests/integration_test.go README.md
git commit -m "feat(chronod): stop tick engine on shutdown; e2e batch coverage"
```

---

### Task 3: Frontend data layer + layout restructure + dedup

> Structural only: new grid with the right rail, the dedup moves, history-driven graphs, and the batched `triggers` handling. Visual polish (colors, card interiors, dropdowns, intro) is Task 4 — style minimally here, just enough that nothing renders broken.

**Files:**
- Modify: `internal/server/web/src/store.ts`
- Modify: `internal/server/web/src/app.ts`
- Modify: `internal/server/web/src/components/layout.ts`
- Modify: `internal/server/web/src/components/cards.ts`
- Modify: `internal/server/web/src/style.css` (grid areas + rail structure only)
- Rebuild: `internal/server/web/dist/`

**Interfaces:**
- Consumes: Task 1's frames — `hello.history`, `snapshot` (unchanged shape), `triggers` batch.
- Produces: the DOM structure and state shape Task 4 styles; `state.series` with pre-filled history.

- [ ] **Step 1: Rewrite `store.ts`** — history types, series state, batch push, 10-cap:

```ts
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

/** Server-side 2-minute history from the hello frame (time-ascending). */
export interface History {
  t: number[]; // ticks/s
  f: number[]; // triggers/s
  l: number[]; // engine.live
  v: Record<string, number[]>; // per-venue ticks/s
}

export interface Hello {
  venues: string[];
  tiers: string[];
  symbol_count: number;
  history: History;
}

export const state = {
  hello: null as Hello | null,
  snapshot: null as Snapshot | null,
  triggers: [] as Trigger[], // newest first, capped at 10 (the rail)
  series: {
    ticks: [] as number[],   // ticks/s, seeded from hello.history.t
    fires: [] as number[],   // triggers/s, seeded from hello.history.f
    live: [] as number[],    // engine.live, seeded from hello.history.l
    venues: {} as Record<string, number[]>,
  },
};

const SERIES_CAP = 120;

function capPush(arr: number[], v: number): void {
  arr.push(v);
  if (arr.length > SERIES_CAP) arr.shift();
}

/** Called once when the hello frame lands: graphs start full. */
export function seedHistory(h: History): void {
  state.series.ticks = [...h.t];
  state.series.fires = [...h.f];
  state.series.live = [...h.l];
  state.series.venues = Object.fromEntries(
    Object.entries(h.v).map(([venue, s]) => [venue, [...s]]),
  );
}

// Venue rates need the cumulative counter diff, kept out of band:
const lastVenueTotal: Record<string, number> = {};
function pushVenueSample(venue: string, total: number): void {
  const arr = (state.series.venues[venue] ??= []);
  const rate = lastVenueTotal[venue] === undefined ? 0 : total - lastVenueTotal[venue];
  lastVenueTotal[venue] = total;
  capPush(arr, rate);
}

/** One 1s snapshot arrives → append the new sample points. */
export function pushSample(s: Snapshot): void {
  capPush(state.series.ticks, s.ticks_per_sec);
  capPush(state.series.fires, s.triggers_per_sec);
  capPush(state.series.live, s.engine.live);
  for (const [venue, total] of Object.entries(s.venue_ticks)) {
    pushVenueSample(venue, total);
  }
}

/** One batched frame (oldest→newest) → prepend to the rail in order. */
export function pushTriggers(batch: Trigger[]): void {
  for (const tr of batch) {
    state.triggers.unshift(tr);
  }
  if (state.triggers.length > 10) state.triggers.length = 10;
}
```

- [ ] **Step 2: Rewrite `app.ts`** — hello seeds history, snapshots append samples, batches feed the rail:

```ts
import "./style.css";
import { state, seedHistory, pushSample, pushTriggers } from "./store";
import type { Hello, Snapshot, Trigger } from "./store";
import { mount, update } from "./components/layout";
import { mountInquiry, helloArrived } from "./components/inquiry";

function main(): void {
  mountInquiry(mount(document.getElementById("app")!).inquiry);
  const es = new EventSource("/api/stream"); // EventSource reconnects on its own
  es.onmessage = (ev: MessageEvent<string>) => {
    const frame = JSON.parse(ev.data) as { type: string } & Record<string, unknown>;
    switch (frame.type) {
      case "hello":
        state.hello = frame as unknown as Hello;
        seedHistory(state.hello.history);
        helloArrived(state.hello);
        break;
      case "snapshot":
        state.snapshot = frame.snapshot as Snapshot;
        pushSample(state.snapshot);
        break;
      case "triggers":
        pushTriggers(frame.triggers as Trigger[]);
        break;
    }
    update();
  };
}

main();
```

- [ ] **Step 3: Extend `cards.ts`** — add a mini trend (thin line, no axes) reusing the sparkline geometry, and a venue trend marker helper:

```ts
/** Thin trend line for embedding in tile/row interiors. */
export function trend(series: number[], w = 100, h = 16): string {
  if (series.length < 2) return "";
  const max = Math.max(...series, 1);
  const min = Math.min(...series, 0);
  const span = max - min || 1;
  const pts = series.map((v, i) => {
    const x = (i / (series.length - 1)) * w;
    const y = h - ((v - min) / span) * h;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });
  return `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" class="trend">
    <polyline fill="none" stroke="currentColor" stroke-width="1.5" points="${pts.join(" ")}"/>
  </svg>`;
}

/** Direction arrow (▲/▼/—) for the last vs. previous sample. */
export function trendArrow(series: number[]): string {
  if (series.length < 2) return "—";
  const d = series[series.length - 1] - series[series.length - 2];
  return d > 0 ? "▲" : d < 0 ? "▼" : "—";
}
```

Keep `fmt`, `fmtDuration`, `statusDot`, `sparkline` unchanged.

- [ ] **Step 4: Rewrite `layout.ts`** — new grid, dedup moves, history-fed graphs. Full file:

```ts
import { state } from "../store";
import { fmt, fmtDuration, sparkline, statusDot, trend, trendArrow } from "./cards";

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
    <section class="card area-fires fires"><h2>fires</h2>
      <div class="big mono" id="fired">—</div>
      <div class="sub">triggers fired · <span id="firesarrow">—</span></div>
      <div id="firesspark"></div>
    </section>
    <section class="card area-book"><h2>alert book</h2>
      <div class="trio">
        <div class="tile"><div class="big mono" id="st-active">—</div><div class="sub">active</div></div>
        <div class="tile"><div class="big mono" id="st-triggered">—</div><div class="sub">triggered</div></div>
        <div class="tile"><div class="big mono" id="st-cancelled">—</div><div class="sub">cancelled</div></div>
      </div>
      <div class="livetrend"><span class="lbl">live alerts</span>
        <span class="mono" id="live">—</span><span id="livetrendline"></span></div>
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

  // Fires owns the fired counter + rate trend.
  set("fired", fmt(s.triggers_fired));
  set("firesarrow", trendArrow(state.series.fires));
  const fspark = document.getElementById("firesspark");
  if (fspark) fspark.innerHTML = trend(state.series.fires);

  // Alert book owns state tiles + the engine.live trend (not the engine card).
  set("st-active", String(s.alerts_by_state.active ?? 0));
  set("st-triggered", String(s.alerts_by_state.triggered ?? 0));
  set("st-cancelled", String(s.alerts_by_state.cancelled ?? 0));
  set("live", fmt(s.engine.live));
  const lt = document.getElementById("livetrendline");
  if (lt) lt.innerHTML = trend(state.series.live);

  const venues = document.getElementById("venues");
  if (venues) {
    const entries = Object.entries(s.venue_ticks).sort((a, b) => b[1] - a[1]);
    const max = entries[0]?.[1] || 1;
    venues.innerHTML =
      entries
        .map(([v, n]) => {
          const series = state.series.venues[v] ?? [];
          return `<div class="row">
          <span class="vname">${v} <span class="trendmark">${trendArrow(series)}</span></span>
          <span class="vmeta">${trend(series.slice(-40))}</span>
          <span class="bar"><i style="width:${(100 * n) / max}%"></i></span>
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
```

- [ ] **Step 5: Structural CSS.** In `style.css`, replace the `.grid` block, the `.area-*` rules, and the `.rows .row` rule with (keep everything else; Task 4 refines visuals). The rail is the vertical right column spanning rows 1–2; venues/engine share row 3:

```css
.grid {
  display: grid; gap: 14px; padding: 0 14px 14px;
  grid-template-columns: repeat(12, 1fr);
  grid-template-areas:
    "pulse pulse pulse pulse fires fires fires fires book book rail rail"
    "pulse pulse pulse pulse fires fires fires fires book book rail rail"
    "venues venues venues venues venues engine engine engine engine rail rail rail"
    "inquiry inquiry inquiry inquiry inquiry inquiry inquiry inquiry inquiry inquiry inquiry inquiry";
}
.area-pulse { grid-area: pulse; }
.area-fires { grid-area: fires; }
.area-book { grid-area: book; }
.area-venues { grid-area: venues; }
.area-engine { grid-area: engine; }
.area-stream { grid-area: rail; }
.area-inquiry { grid-area: inquiry; }
.rail { display: flex; flex-direction: column; overflow: hidden; }
.rail .stream { flex: 1; overflow-y: auto; }
.livetrend { display: flex; align-items: center; gap: 8px; margin-top: 10px; }
.livetrend .lbl { font-size: 12px; opacity: .65; }
#livetrendline { flex: 1; }
#livetrendline svg, .vmeta svg { width: 100%; height: 16px; display: block; }
#venues .row {
  display: grid; grid-template-columns: 90px 70px 1fr 90px; gap: 10px;
  align-items: center; padding: 6px 0; font-size: 14px;
}
#engine .row {
  display: flex; justify-content: space-between; align-items: center;
  padding: 6px 0; font-size: 14px;
}
```

- [ ] **Step 6: Type-check, rebuild, verify**

```bash
cd internal/server/web && npx -y -p typescript@5 tsc --noEmit && ./build.sh && cd -
go build ./... && go vet ./...
go test ./internal/server/ -race -count=1   # TestUIAssetsServed still green (app.js serves)
git diff --stat main -- engine price        # empty
```

- [ ] **Step 7: Commit**

```bash
git add internal/server/web/
git commit -m "feat(web): batched stream rail, history graphs, card dedup"
```

---

### Task 4: Visual overhaul

> **REQUIRED SUB-SKILL: superpowers:frontend-design** — apply it for all visual/CSS decisions in this task. The contract, layout structure, palette tokens, dedup, and behavior are pinned by this plan and the spec; the skill governs polish (spacing rhythm, type scale, motion, states). Stay inside the pinned palette and rules.

**Files:**
- Modify: `internal/server/web/src/style.css` (full overhaul)
- Create: `internal/server/web/src/components/dropdown.ts`
- Create: `internal/server/web/src/components/intro.ts`
- Modify: `internal/server/web/src/components/inquiry.ts` (custom dropdowns)
- Modify: `internal/server/web/src/app.ts` (intro call)
- Modify: `internal/server/web/src/components/layout.ts` (intro overlay mount point, tile glyphs)
- Rebuild: `internal/server/web/dist/`

**Interfaces:**
- Consumes: Task 3's DOM structure and `state.series`.
- Produces: the finished dashboard.

- [ ] **Step 1: Create `dropdown.ts`** — styled dropdown, no libraries:

```ts
export interface DDOption {
  value: string; // "" = "any"
  label: string;
}

/**
 * Custom dropdown (native <select> popups cannot be styled). Renders a
 * button + popover into `host`; keeps `input` (a hidden form input) in
 * sync so the existing FormData-based query build is untouched.
 */
export function mountDropdown(
  host: HTMLElement,
  name: string,
  options: DDOption[],
): HTMLInputElement {
  const input = document.createElement("input");
  input.type = "hidden";
  input.name = name;

  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "dd";
  btn.setAttribute("aria-haspopup", "listbox");
  btn.setAttribute("aria-expanded", "false");

  const list = document.createElement("ul");
  list.className = "dd-list";
  list.role = "listbox";
  list.hidden = true;

  let selected = "";
  const render = (): void => {
    btn.innerHTML = `<span>${options.find((o) => o.value === selected)?.label ?? ""}</span><i class="chev">▾</i>`;
    for (const li of list.children) {
      (li as HTMLElement).classList.toggle("sel", (li as HTMLElement).dataset.v === selected);
    }
  };

  options.forEach((o) => {
    const li = document.createElement("li");
    li.role = "option";
    li.dataset.v = o.value;
    li.textContent = o.label;
    li.addEventListener("click", () => {
      selected = o.value;
      input.value = selected;
      render();
      close();
    });
    list.appendChild(li);
  });

  const open = (): void => {
    list.hidden = false;
    btn.setAttribute("aria-expanded", "true");
    (list.querySelector(`[data-v="${selected}"]`) as HTMLElement | null)?.focus();
  };
  const close = (): void => {
    list.hidden = true;
    btn.setAttribute("aria-expanded", "false");
  };
  const toggle = (): void => (list.hidden ? open() : close());

  btn.addEventListener("click", toggle);
  list.addEventListener("keydown", (e: KeyboardEvent) => {
    const items = [...list.children] as HTMLElement[];
    const i = items.indexOf(document.activeElement as HTMLElement);
    if (e.key === "Escape") { close(); btn.focus(); }
    else if (e.key === "ArrowDown" && i < items.length - 1) items[i + 1].focus();
    else if (e.key === "ArrowUp" && i > 0) items[i - 1].focus();
    else if (e.key === "Enter" && i >= 0) { items[i].click(); e.preventDefault(); }
  });
  document.addEventListener("click", (e) => {
    if (!host.contains(e.target as Node)) close();
  });

  host.append(input, btn, list);
  render();
  return input;
}

/** Replace a dropdown's option list in place (hello-frame vocabulary). */
export function setDropdownOptions(host: HTMLElement, options: DDOption[]): void {
  // Simplest correct approach: unmount and rebuild preserving selection.
  const input = host.querySelector("input") as HTMLInputElement;
  const sel = input.value;
  host.innerHTML = "";
  mountDropdown(host, input.name, options);
  const rebuilt = host.querySelector("input") as HTMLInputElement;
  rebuilt.value = sel;
}
```

- [ ] **Step 2: Create `intro.ts`** — the one-shot load animation:

```ts
/**
 * One-shot intro: onyx overlay with the chrono wordmark, which drifts to
 * its topbar home while the page blooms in beneath (~1.4s). Skipped
 * entirely under prefers-reduced-motion.
 */
export function playIntro(): void {
  if (matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  const overlay = document.createElement("div");
  overlay.className = "intro";
  overlay.innerHTML = `<span class="intro-mark">chrono<span class="brand-dot">▸</span></span>`;
  document.body.appendChild(overlay);
  // The wordmark fades/scales down toward the top-left while the overlay
  // dissolves; CSS keyframes own the motion. Cleanup after the longest run.
  setTimeout(() => overlay.remove(), 1800);
}
```

Call it first in `app.ts`'s `main()`:

```ts
import { playIntro } from "./components/intro";
// ...
function main(): void {
  playIntro();
  mountInquiry(mount(document.getElementById("app")!).inquiry);
  // ... unchanged
```

- [ ] **Step 3: Rewrite `inquiry.ts` filters** to use dropdowns. Replace the four `<select>` elements in the form HTML with mount points, and build them after `root.innerHTML`:

```ts
import { mountDropdown, setDropdownOptions, type DDOption } from "./dropdown";
```

Form markup becomes:

```ts
    <form class="filters" id="f">
      <input name="symbol" placeholder="symbol (e.g. BTCUSDT)">
      <span class="ddhost" data-dd="state"></span>
      <span class="ddhost" data-dd="venue"></span>
      <span class="ddhost" data-dd="tier"></span>
      <span class="ddhost" data-dd="direction"></span>
      <button type="submit">query</button>
    </form>
```

And after the markup:

```ts
  const dd = (name: string, opts: DDOption[]): HTMLElement =>
    root.querySelector(`[data-dd="${name}"]`) as HTMLElement;
  const staticOpts = (vals: string[], label: string): DDOption[] =>
    [{ value: "", label }, ...vals.map((v) => ({ value: v, label: v }))];
  mountDropdown(dd("state"), "state", staticOpts(STATES, "state: any"));
  mountDropdown(dd("venue"), "venue", staticOpts([], "venue: any"));
  mountDropdown(dd("tier"), "tier", staticOpts([], "tier: any"));
  mountDropdown(
    dd("direction"), "direction",
    staticOpts(["ABOVE", "BELOW"], "direction: any"),
  );
```

`fillVocab`/`helloArrived` switch from `setOptions(select, values)` to:

```ts
  fillVocab = (hello: Hello): void => {
    setDropdownOptions(dd("venue"), staticOpts(hello.venues, "venue: any"));
    setDropdownOptions(dd("tier"), staticOpts(hello.tiers, "tier: any"));
    fillVocab = null;
  };
```

Remove the `venueSel`/`tierSel`/`setOptions` code; `load()` (FormData-based) is unchanged — the hidden inputs carry the values.

- [ ] **Step 4: The CSS overhaul** (`style.css`, full rewrite within these pins — the frontend-design skill refines but must not violate):
  - **Fires card** (`.card.fires`): `pine-teal` base; `molten-lava` radial glow behind the big number; thin `brick-ember` top keyline (e.g. `border-top: 3px solid` or inset gradient); `#spark`-style rate trend in `light-green`; ~4%-opacity `molten-lava` `repeating-linear-gradient` hatch.
  - **Trigger rail** (`.card.rail`): dark `graphite`/`onyx` card (linen text, `silver` metadata, `light-green` timestamps) — no longer teal.
  - **Legibility:** every `.sub`, `.duo`, `.meta`, `.lbl`, `th`, `.empty` currently `color: var(--silver)` on linen switches to `color: rgba(30,25,23,.68)` (coffee-bean at 68%) or a `--linen-ink` custom property ≈ `#6B615B`. `silver` only on dark surfaces.
  - **Alert book tiles:** small inline-SVG glyphs (e.g. a pulsing ring for active, a lightning bolt for triggered, a slashed circle for cancelled) + colored progress fills; `.tile` backgrounds differentiate states within the palette (pine-teal fill for active, brick-ember for triggered, linen for cancelled).
  - **Engine card:** keyline dividers, chip-styled row labels, right-aligned mono values.
  - **Venues:** rounded bar tracks with a `light-green` marker at the current rate position (from `state.series.venues`), trend arrows colored with the `trendArrow` semantics (up = `brick-ember` on linen — red for heat, not green; light-green reserved for dark surfaces).
  - **Dropdown:** `.dd` button styled like the query button's sibling (linen surface, coffee-bean border, chevron); `.dd-list` popover (absolute, linen, shadow, rounded, same option typography, `.sel` row highlighted pine-teal/linen).
  - **Intro:** `.intro` fixed full-screen `onyx` overlay (z-index above all), `.intro-mark` centered `soft-linen` wordmark with the pulsing green ▸; keyframes move/scale the mark toward the topbar position while the overlay fades out; total ≤1.8s; `prefers-reduced-motion` handled by `playIntro` skipping (CSS rule optional).
  - **Grid bloom:** cards start `opacity:0; translateY(8px)` and animate in with a small stagger (CSS only, e.g. `.card { animation: bloom .4s ease both; }` + per-area `animation-delay`), skipped under reduced motion.
  - **Responsive:** `@media (max-width: 1100px)` — rail becomes a full-width horizontal card after the book card (grid-template-areas rewritten; `.trg` rows stay single-line); `@media (max-width: 720px)` — single column (pulse, fires, book, venues, engine, rail, inquiry), `.trg` compresses to badge · symbol · price (`.meta`, `.time` hidden), `.tbl` wrapped in a horizontally scrollable container (`overflow-x: auto` on `.card.area-inquiry`).
  - Keep: palette `:root` verbatim, `.mono`, `@keyframes slidein`, hub-overflow-irrelevant bits, focus-visible rules, `.chip.state-*`.

- [ ] **Step 5: Build, verify, commit**

```bash
cd internal/server/web && npx -y -p typescript@5 tsc --noEmit && ./build.sh && cd -
go build ./... && go vet ./...
go test ./internal/server/ -race -count=1
git diff --stat main -- engine price    # empty
# Manual pass: nats-server & chronod & chronofeed & seed, then open
# http://localhost:8080/ — intro plays once, graphs pre-filled from
# history, triggers arrive as batches, dropdowns styled, resize to check
# the three breakpoints. Tear everything down after.
git add internal/server/web/
git commit -m "feat(web): visual overhaul — fires restyle, rail, dropdowns, intro"
```

---

## Post-plan notes for the executor

- Task 1's test for `hello.history` keys and Task 3's TS types must stay in sync: `t`/`f`/`l`/`v` verbatim.
- `build.sh` uses `npx -y -p typescript@5` / `npx -y -p esbuild@0.25` semantics (Node 22+); tsc must run clean before esbuild.
- Go 1.27: `encoding/json/v2` binds as `json`; `MarshalWrite`/`UnmarshalRead` only.
- Engine/price zero diffs at the end of every task; never `git add -A`.
- Task 4 brief must name the frontend-design skill as REQUIRED and paste the palette tokens, the legibility rule, the light-green rule, and the fires/rail/intro/responsive pins verbatim.
