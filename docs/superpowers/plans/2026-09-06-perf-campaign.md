# Performance Campaign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A committed, repeatable performance harness that measures RAM/CPU of the engine and service layers across an OFAT scenario spine (1M alerts; rate 20k/100k/200k tps; 500/1000/2000 symbols; trickle firing; 100k/500k bursts), with pprof attribution, validity gates, and a committed first-run report.

**Architecture:** A shared generator package (`internal/bench`) produces deterministic symbol universes, alert layouts, and paced tick streams. Two drivers consume it: `scripts/benchengine` (in-process engine, writes runtime/pprof files) and `scripts/benchfeed` (seeds bbolt, streams to a real chronod). `scripts/perf/run.sh` sequences scenarios, fetches chronod's HTTP pprof profiles, polls `/stats` (which already exposes `sys.rss_bytes`, `sys.proc_cpu_percent`, `sys.db_bytes`), and `scripts/perfcollect` merges everything into a gated `summary.json`. Engine is frozen — everything only imports it.

**Tech Stack:** Go 1.27 standard library (`runtime/pprof`, `encoding/json/v2`), bash + curl + jq for orchestration. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-09-06-perf-campaign-design.md` (amended same-day: burst gate is the conservation law `fired + ring_dropped == k`; the no-NATS mode is a noop publisher, not a failing one — chronod treats NATS as a boot dependency).

## Global Constraints

- `engine/` is frozen: no engine file changes; drivers only import it.
- Every scenario's alert total is 1M (bursts: K cluster alerts inside the 1M budget, rest parked).
- Determinism: same `-seed`, `-symbols`, `-alerts` → byte-identical universes, layouts, walks.
- Zero drops is a gate: all generated ticks are well-formed (`nameOK` passes: names ≤ 20 bytes, prices at pinned scale).
- Never block on NATS: chronod runs with `-nats-url ""` (noop publisher — publish succeeds instantly, store flips still happen).
- Every commit leaves the tree green: `go vet ./... && go test ./... -count=1` (the smoke test is allowed ~60s).
- The dirty `internal/server/web/` working-tree files are never staged by these tasks.
- Alert/tick dims: venue value is always 0 ("ATLAS"); tier alternates 0 ("TOP") / 1 ("MID"); GapCluster alerts are ALL tier 0 so one gap tick fires the whole cluster.
- Burst drain timeout is 120s — a drain that exceeds it reports its actual value and marks the run INVALID (`drain_timeout`), per spec "the measurement IS the number".

---

### Task 1: `internal/bench` — symbol universe, market walk, pacing

**Files:**
- Create: `internal/bench/bench.go`
- Test: `internal/bench/bench_test.go`

**Interfaces:**
- Produces (used by Tasks 2, 4, 6):

```go
type Symbol struct{ Name string; Decimals uint8; Ref int64 } // Ref in base units at Decimals
func Symbols(n int) []Symbol
const Venue = "ATLAS"; const TierTop = "TOP"; const TierMid = "MID"
func Tier(i int) string

type Quote struct{ SymIdx, TierIdx int; Bid, Ask int64 } // base units
type Market struct{ /* ... */ }
func NewMarket(syms []Symbol, seed uint64, holdFirst bool) *Market
func (m *Market) Step() Quote

type Emitter struct{ /* ... */ }
func (e *Emitter) Take(interval time.Duration) int

func ReadRSS(pid int) uint64          // VmRSS bytes from /proc/<pid>/status; 0 on error
func ReadPeakRSS(pid int) uint64      // VmHWM likewise
func ReadCPUSeconds(pid int) float64  // (utime+stime) from /proc/<pid>/stat; USER_HZ=100
```

- [ ] **Step 1: Write the failing tests**

Create `internal/bench/bench_test.go`:

```go
package bench

import (
	"os"
	"testing"
	"time"
)

func TestSymbolsDeterministic(t *testing.T) {
	a, b := Symbols(500), Symbols(500)
	if len(a) != 500 {
		t.Fatalf("len = %d, want 500", len(a))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("symbol %d differs between runs", i)
		}
		if a[i].Ref < 1 {
			t.Fatalf("%s: Ref = %d, want >= 1", a[i].Name, a[i].Ref)
		}
		if len(a[i].Name) > 20 {
			t.Fatalf("%s exceeds the 20-byte tick-name bound", a[i].Name)
		}
	}
}

func TestSymbolsDecimalsMix(t *testing.T) {
	// The universe must exercise all three precision classes (2/4/8).
	syms := Symbols(2000)
	seen := map[uint8]int{}
	for _, s := range syms {
		seen[s.Decimals]++
	}
	for _, dec := range []uint8{2, 4, 8} {
		if seen[dec] == 0 {
			t.Fatalf("decimals %d never appears: %v", dec, seen)
		}
	}
}

func TestMarketRoundRobinAndHold(t *testing.T) {
	syms := Symbols(4)
	m := NewMarket(syms, 1, false)
	for i := 0; i < 8; i++ {
		q := m.Step()
		if q.SymIdx != i%4 {
			t.Fatalf("step %d: SymIdx = %d, want %d", i, q.SymIdx, i%4)
		}
		if q.Bid >= q.Ask {
			t.Fatalf("step %d: bid %d >= ask %d", i, q.Bid, q.Ask)
		}
	}
	h := NewMarket(syms, 1, true)
	q := h.Step()
	if q.SymIdx != 0 {
		t.Fatal("first step is not symbol 0")
	}
	if q.Bid != syms[0].Ref { // held at reference, zero noise
		t.Fatalf("held symbol bid = %d, want ref %d", q.Bid, syms[0].Ref)
	}
}

func TestMarketDeterministic(t *testing.T) {
	syms := Symbols(32)
	a, b := NewMarket(syms, 7, false), NewMarket(syms, 7, false)
	for i := 0; i < 1000; i++ {
		qa, qb := a.Step(), b.Step()
		if qa != qb {
			t.Fatalf("step %d diverged: %+v vs %+v", i, qa, qb)
		}
	}
}

func TestEmitterBudget(t *testing.T) {
	e := &Emitter{target: 20000}
	var n int
	for i := 0; i < 100; i++ { // 100 × 10ms = 1s
		n += e.Take(10 * time.Millisecond)
	}
	if n < 19800 || n > 20200 {
		t.Fatalf("emitter gave %d over 1s at 20k/s, want ~20000", n)
	}
}

func TestProcReads(t *testing.T) {
	pid := os.Getpid()
	if ReadRSS(pid) == 0 && ReadPeakRSS(pid) == 0 {
		t.Fatal("self has neither RSS nor peak")
	}
	if ReadCPUSeconds(pid) <= 0 {
		t.Fatal("self CPU seconds <= 0")
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/bench/ -count=1`
Expected: FAIL — `undefined: Symbols` (package builds, symbols missing).

- [ ] **Step 3: Implement**

Create `internal/bench/bench.go`:

```go
// Package bench generates deterministic performance-campaign workloads:
// symbol universes, alert layouts, and paced mean-reverting tick streams.
// Both drivers (scripts/benchengine, scripts/benchfeed) build on it so the
// engine and service layers measure identical scenarios.
package bench

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"time"
)

// Symbol is one generated tradable pair. Ref is the walk anchor in base
// units at Decimals precision.
type Symbol struct {
	Name     string
	Decimals uint8
	Ref      int64
}

// Symbols builds the deterministic universe of n pairs. Magnitudes are
// log-uniform in [0.00001, 10000) hashed from the name — catalog
// synthAnchor's recipe — with the catalog's precision rule (>=1000 → 2,
// >=1 → 4, else 8), so all three classes appear at any n >= ~30.
func Symbols(n int) []Symbol {
	syms := make([]Symbol, n)
	lo, hi := math.Log(0.00001), math.Log(10000)
	for i := range syms {
		name := fmt.Sprintf("BSYM%04d", i)
		h := fnv.New64a()
		_, _ = h.Write([]byte(name))
		u := h.Sum64()
		x := math.Exp(float64(u%10_000)/10_000*(hi-lo) + lo)
		dec := uint8(8)
		switch {
		case x >= 1000:
			dec = 2
		case x >= 1:
			dec = 4
		}
		syms[i] = Symbol{Name: name, Decimals: dec, Ref: int64(math.Round(x * math.Pow10(int(dec))))}
	}
	return syms
}

// Dim vocabulary: venue value is always 0; tier alternates 0/1. These are
// the engine uint16 values the drivers put in Dims, and the names the
// feeder puts on the wire.
const (
	VenueName = "ATLAS"
	TierTop   = "TOP"
	TierMid   = "MID"
)

func Tier(i int) string {
	if i%2 == 0 {
		return TierTop
	}
	return TierMid
}

// reversion pulls each tick 0.2% of the way back toward the reference —
// chronofeed's walk constant, so the stationary band is ~16σ around Ref.
const reversion = 0.002

func sigmaFor(dec uint8) float64 {
	switch dec {
	case 2:
		return 0.0005
	case 4:
		return 0.001
	default:
		return 0.003
	}
}

// Quote is one advanced symbol's prices, base units at its decimals.
type Quote struct {
	SymIdx  int
	TierIdx int
	Bid     int64
	Ask     int64
}

// Market is a per-run mean-reverting walk over the universe, round-robin
// like chronofeed so every symbol streams at an equal rate. holdFirst pins
// symbol 0 at its reference: the GapCluster soak symbol, whose cluster
// must not fire before the gap tick.
type Market struct {
	syms      []Symbol
	base, ref []int64
	sigma     []float64
	rng       *rand.Rand
	symI      int
	holdFirst bool
}

func NewMarket(syms []Symbol, seed uint64, holdFirst bool) *Market {
	m := &Market{
		syms: syms, base: make([]int64, len(syms)), ref: make([]int64, len(syms)),
		sigma: make([]float64, len(syms)), rng: rand.New(rand.NewPCG(seed, seed*2654435761+1)),
		holdFirst: holdFirst,
	}
	for i, s := range syms {
		m.base[i], m.ref[i], m.sigma[i] = s.Ref, s.Ref, sigmaFor(s.Decimals)
	}
	return m
}

// Step advances one symbol and returns its quote.
func (m *Market) Step() Quote {
	i := m.symI % len(m.syms)
	m.symI++
	if m.holdFirst && i == 0 {
		return Quote{SymIdx: 0, TierIdx: 0, Bid: m.ref[0], Ask: m.ref[0] + 1}
	}
	step := reversion*float64(m.ref[i]-m.base[i]) + m.sigma[i]*float64(m.ref[i])*m.rng.NormFloat64()
	b := m.base[i] + int64(step)
	if b < 1 {
		b = 1
	}
	m.base[i] = b
	half := int64(float64(b) * 0.0005) // ~5bp spread, chronofeed's TOP level
	if half < 1 {
		half = 1
	}
	return Quote{SymIdx: i, TierIdx: m.symI % 2, Bid: b - half, Ask: b + half}
}

// Emitter shapes tick emission toward a target rate: accumulate the
// fractional budget, hand out whole ticks per interval (chronofeed's).
type Emitter struct {
	Target, acc float64
}

func (e *Emitter) Take(interval time.Duration) int {
	e.acc += e.Target * interval.Seconds()
	n := int(e.acc)
	e.acc -= float64(n)
	return n
}

// ReadRSS returns VmRSS for pid, in bytes (0 if unreadable).
func ReadRSS(pid int) uint64 { return readStatusKB(pid, "VmRSS") * 1024 }

// ReadPeakRSS returns VmHWM (high-water mark) for pid, in bytes.
func ReadPeakRSS(pid int) uint64 { return readStatusKB(pid, "VmHWM") * 1024 }

func readStatusKB(pid int, key string) uint64 {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, key+":"); ok {
			kb, _ := strconv.ParseUint(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(v), "kB")), 10, 64)
			return kb
		}
	}
	return 0
}

// ReadCPUSeconds returns utime+stime for pid. Linux USER_HZ is 100 on
// every mainstream build; the campaigns compare runs on one host, so the
// constant is fine.
func ReadCPUSeconds(pid int) float64 {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0
	}
	// Fields 14/15 (1-based) follow "comm)" — find the last ')'.
	s := string(b)
	if i := strings.LastIndexByte(s, ')'); i >= 0 {
		s = s[i+2:]
	}
	f := strings.Fields(s)
	if len(f) < 13 {
		return 0
	}
	utime, _ := strconv.ParseFloat(f[11], 64)
	stime, _ := strconv.ParseFloat(f[12], 64)
	return (utime + stime) / 100
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `go test ./internal/bench/ -count=1`
Expected: PASS (all seven tests).

- [ ] **Step 5: Commit**

```bash
git add internal/bench/
git commit -m "feat(bench): deterministic symbol universe, market walk, pacing"
```

---

### Task 2: `internal/bench` — alert layouts + engine oracle

**Files:**
- Modify: `internal/bench/bench.go` (append layouts and conversions)
- Test: `internal/bench/bench_test.go` (append oracle tests)

**Interfaces:**
- Consumes: Task 1's `Symbol`/`Symbols`/`Quote`/`Market`; `engine.New/DefaultConfig/Upsert/Sync/Match/Triggers`, `alertstore.Alert`.
- Produces (used by Tasks 3, 4, 6):

```go
type Spec struct {
	ID        engine.AlertID
	SymIdx    int
	Symbol    string
	Decimals  uint8
	PriceType engine.PriceType
	Direction engine.Direction
	Target    int64 // base units
	TierIdx   int   // 0/1; venue value is always 0
}
func MkID(i uint64) engine.AlertID
func Parked(n int, syms []Symbol) []Spec          // zero-fire ingest control
func Trickle(n int, syms []Symbol, band float64) []Spec
func GapCluster(total, k int, syms []Symbol) (specs []Spec, gap Quote)
func EngineSpec(s Spec) engine.AlertSpec
func EngineTick(syms []Symbol, q Quote, ts int64) engine.Tick
func StoreAlert(s Spec, now int64) alertstore.Alert
```

- [ ] **Step 1: Write the failing tests**

Append to `internal/bench/bench_test.go` (imports gain `"testing/pkg/browser"`? No — gain `"time"` already present, plus nothing else; the oracle needs no new import):

```go
// drain pops the engine's trigger ring until want triggers arrive or the
// timeout hits; returns how many arrived.
func drain(t *testing.T, e *engine.Engine, want int, timeout time.Duration) int {
	t.Helper()
	buf := make([]engine.Trigger, 64)
	deadline := time.Now().Add(timeout)
	var n int
	for n < want && time.Now().Before(deadline) {
		if k := e.Triggers().PopBatch(buf); k > 0 {
			n += k
			continue
		}
		time.Sleep(500 * time.Microsecond)
	}
	return n
}

func tickOf(syms []Symbol, q Quote, ts int64) engine.Tick { return EngineTick(syms, q, ts) }

func TestParkedNeverFires(t *testing.T) {
	syms := Symbols(50)
	specs := Parked(2000, syms)
	e := engine.New(engine.DefaultConfig())
	defer e.Close()
	for _, s := range specs {
		if err := e.Upsert(EngineSpec(s)); err != nil {
			t.Fatal(err)
		}
	}
	e.Sync()
	m := NewMarket(syms, 1, false)
	now := int64(1 << 40)
	for i := 0; i < 20000; i++ {
		q := m.Step()
		tk := tickOf(syms, q, now+int64(i))
		e.Match(&tk)
	}
	if n := drain(t, e, 1, 50*time.Millisecond); n != 0 {
		t.Fatalf("parked layout fired %d triggers", n)
	}
}

func TestTrickleFiresSome(t *testing.T) {
	syms := Symbols(50)
	specs := Trickle(2000, syms, 0.002)
	e := engine.New(engine.DefaultConfig())
	defer e.Close()
	for _, s := range specs {
		if err := e.Upsert(EngineSpec(s)); err != nil {
			t.Fatal(err)
		}
	}
	e.Sync()
	m := NewMarket(syms, 1, false)
	now := int64(1 << 40)
	for i := 0; i < 20000; i++ {
		q := m.Step()
		tk := tickOf(syms, q, now+int64(i))
		e.Match(&tk)
	}
	n := drain(t, e, 1, 100*time.Millisecond)
	if n == 0 {
		t.Fatal("trickle layout never fired")
	}
	rest := drain(t, e, 2000, 100*time.Millisecond)
	if n+rest >= 2000 {
		t.Fatalf("trickle burned its whole ladder: %d of 2000", n+rest)
	}
}

func TestGapClusterFiresExactlyK(t *testing.T) {
	syms := Symbols(50)
	specs, gap := GapCluster(2000, 400, syms)
	if len(specs) != 2000 {
		t.Fatalf("len(specs) = %d, want 2000", len(specs))
	}
	e := engine.New(engine.DefaultConfig())
	defer e.Close()
	for _, s := range specs {
		if err := e.Upsert(EngineSpec(s)); err != nil {
			t.Fatal(err)
		}
	}
	e.Sync()
	// Soak: held symbol 0 walks nothing, parked rest stays parked.
	m := NewMarket(syms, 1, true)
	now := int64(1 << 40)
	for i := 0; i < 5000; i++ {
		q := m.Step()
		tk := tickOf(syms, q, now+int64(i))
		e.Match(&tk)
	}
	if n := drain(t, e, 1, 50*time.Millisecond); n != 0 {
		t.Fatalf("cluster fired %d during soak", n)
	}
	// Gap tick: fires all 400.
	tk := tickOf(syms, gap, now+99999)
	e.Match(&tk)
	if n := drain(t, e, 400, 5*time.Second); n != 400 {
		t.Fatalf("gap tick fired %d, want exactly 400", n)
	}
}

func TestMkIDUniqueNonZero(t *testing.T) {
	seen := map[engine.AlertID]bool{}
	for i := uint64(0); i < 10000; i++ {
		id := MkID(i)
		if id == (engine.AlertID{}) {
			t.Fatal("zero id")
		}
		if seen[id] {
			t.Fatalf("duplicate id at %d", i)
		}
		seen[id] = true
	}
}

func TestStoreAlertRoundTrip(t *testing.T) {
	syms := Symbols(4)
	specs := Parked(8, syms)
	a := StoreAlert(specs[3], 12345)
	if a.Symbol != specs[3].Symbol || a.Decimals != specs[3].Decimals ||
		a.Venue != VenueName || a.Tier != Tier(specs[3].TierIdx) ||
		int64(a.TargetPrice) != specs[3].Target || a.State != alertstore.StateActive {
		t.Fatalf("round trip mismatch: %+v", a)
	}
}
```

Add `"github.com/emir/chrono-tree/engine"` and `"github.com/emir/chrono-tree/internal/alertstore"` to the test file imports.

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/bench/ -run 'TestParked|TestTrickle|TestGapCluster|TestMkID|TestStoreAlert' -count=1`
Expected: FAIL with `undefined: Parked` / `EngineSpec` / `MkID` etc.

- [ ] **Step 3: Implement**

Append to `internal/bench/bench.go` (imports gain `"encoding/binary"`, `"time"`, engine, alertstore):

```go
// ---- Alert layouts ----

// Spec is a layout-generated alert, transport-agnostic: benchengine turns
// it into engine.AlertSpec, benchfeed into alertstore.Alert.
type Spec struct {
	ID        engine.AlertID
	SymIdx    int
	Symbol    string
	Decimals  uint8
	PriceType engine.PriceType
	Direction engine.Direction
	Target    int64 // base units at Decimals
	TierIdx   int   // 0/1; venue value is always 0
}

// MkID derives a deterministic, non-zero, unique alert id from a layout
// index (UUIDv7-shaped for readability; uniqueness is what matters).
func MkID(i uint64) engine.AlertID {
	var id engine.AlertID
	binary.BigEndian.PutUint64(id[:8], i+1)
	id[6] = (id[6] & 0x0f) | 0x70
	id[8] = (id[8] & 0x3f) | 0x80
	return id
}

// parked builds n zero-fire alerts: GTE rungs at 3× the walk anchor, LTE
// rungs at a quarter of it — the walk never leaves the ±few-% band around
// Ref, so nothing ever crosses. IDs start at offset (GapCluster reuses
// this for its parked tail).
func parked(offset, n int, syms []Symbol) []Spec {
	out := make([]Spec, 0, n)
	for i := 0; i < n; i++ {
		sym := syms[(i+offset)%len(syms)]
		s := Spec{
			ID: MkID(uint64(offset + i)), SymIdx: (i + offset) % len(syms),
			Symbol: sym.Name, Decimals: sym.Decimals,
			PriceType: engine.PriceType(i % 4), TierIdx: i % 2,
		}
		if i%2 == 0 {
			s.Direction, s.Target = engine.DirGTE, sym.Ref*3+int64(i)
		} else {
			s.Direction, s.Target = engine.DirLTE, max(sym.Ref/4, 1)
		}
		out = append(out, s)
	}
	return out
}

// Parked returns the zero-fire ingest-control layout over the whole
// universe.
func Parked(n int, syms []Symbol) []Spec { return parked(0, n, syms) }

// Trickle returns n rungs inside ±band of each symbol's anchor,
// alternating ABOVE/BELOW: the mean-reverting walk crosses them at a
// steady, sparse pace.
func Trickle(n int, syms []Symbol, band float64) []Spec {
	out := make([]Spec, 0, n)
	for i := 0; i < n; i++ {
		sym := syms[i%len(syms)]
		frac := (float64(i) + 0.5) / float64(n)
		s := Spec{
			ID: MkID(uint64(i)), SymIdx: i % len(syms),
			Symbol: sym.Name, Decimals: sym.Decimals,
			PriceType: engine.PriceType(i % 4), TierIdx: i % 2,
		}
		if i%2 == 0 {
			s.Direction = engine.DirGTE
			s.Target = int64(float64(sym.Ref) * (1 + band*frac))
		} else {
			s.Direction = engine.DirLTE
			s.Target = int64(float64(sym.Ref) * (1 - band*frac))
		}
		s.Target = max(s.Target, 1)
		out = append(out, s)
	}
	return out
}

// GapCluster returns total alerts — k GTE rungs stacked one base unit
// apart just above symbol 0's anchor (all tier 0, so one gap tick fires
// them all), the rest the parked tail on symbols 1.. — plus the gap
// quote: a price above every cluster target.
func GapCluster(total, k int, syms []Symbol) ([]Spec, Quote) {
	g0 := syms[0]
	cluster := make([]Spec, k)
	for i := 0; i < k; i++ {
		cluster[i] = Spec{
			ID: MkID(uint64(i)), SymIdx: 0, Symbol: g0.Name, Decimals: g0.Decimals,
			PriceType: engine.PriceType(i % 4), Direction: engine.DirGTE,
			Target: g0.Ref + 1 + int64(i), TierIdx: 0,
		}
	}
	specs := append(cluster, parked(k, total-k, syms[1:])...)
	gap := Quote{
		SymIdx: 0, TierIdx: 0,
		Bid: g0.Ref + int64(k) + 10_000,
		Ask: g0.Ref + int64(k) + 10_001,
	}
	return specs, gap
}

// EngineSpec converts a Spec for direct engine submission. ValidFrom 1:
// valid since the dawn of time; AutoDeactivate so fired entries retire.
func EngineSpec(s Spec) engine.AlertSpec {
	return engine.AlertSpec{
		ID: s.ID, Symbol: s.Symbol, PriceType: s.PriceType, Direction: s.Direction,
		TargetPrice: engine.Price(s.Target), ValidFrom: 1, AutoDeactivate: true,
		Dims: engine.Dims(0, uint16(s.TierIdx)),
	}
}

// EngineTick converts a Quote into an engine tick carrying all four
// price types and matching dims.
func EngineTick(syms []Symbol, q Quote, ts int64) engine.Tick {
	s := syms[q.SymIdx]
	return engine.Tick{
		Symbol: s.Name,
		Bid:    engine.Price(q.Bid), Ask: engine.Price(q.Ask),
		Mid:    engine.Price((q.Bid + q.Ask) / 2), Last: engine.Price(q.Ask),
		Present: engine.TickAllPresent(), TS: ts,
		Dims: engine.Dims(0, uint16(q.TierIdx)),
	}
}

// StoreAlert converts a Spec into a persisted record for seeding the
// service's bbolt store.
func StoreAlert(s Spec, now int64) alertstore.Alert {
	return alertstore.Alert{
		ID: s.ID, Symbol: s.Symbol, Decimals: s.Decimals,
		Venue: VenueName, Tier: Tier(s.TierIdx),
		PriceType: s.PriceType, Direction: s.Direction,
		TargetPrice: engine.Price(s.Target),
		ValidFrom: now, State: alertstore.StateActive, CreatedAt: now,
	}
}
```

Note: `parked(k, total-k, syms[1:])` needs `total-k >= 0` — Task 4's driver validates `cluster < alerts` before calling. Add that guard in the driver, not here.

- [ ] **Step 4: Run the package**

Run: `go test ./internal/bench/ -count=1`
Expected: PASS — Task 1's tests plus the five new ones. The `TestGapClusterFiresExactlyK` oracle is the load-bearing one: if it fails with fires < 400 during the gap, the dims or target math is wrong; do not weaken the test.

- [ ] **Step 5: Commit**

```bash
git add internal/bench/
git commit -m "feat(bench): parked/trickle/gap alert layouts with engine oracle"
```

---

### Task 3: `internal/bench` — Summary type + validity gates

**Files:**
- Modify: `internal/bench/bench.go` (append) — or new file `internal/bench/summary.go` (preferred: one responsibility per file)
- Create: `internal/bench/summary.go`
- Test: `internal/bench/summary_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces (used by Tasks 4, 6, 7):

```go
type Params struct {
	Layer    string `json:"layer"`    // "engine" | "service"
	Scenario string `json:"scenario"` // "baseline", "rate-100k", ...
	Layout   string `json:"layout"`   // "parked" | "trickle" | "gap"
	Alerts   int    `json:"alerts"`
	Symbols  int    `json:"symbols"`
	Cluster  int    `json:"cluster"`
	Rate     int    `json:"rate"`     // target ticks/sec
}

type Summary struct {
	Params          Params  `json:"params"`
	Sent            uint64  `json:"ticks_sent"`
	Accepted        uint64  `json:"ticks_accepted"`
	Dropped         uint64  `json:"ticks_dropped"`
	AchievedRate    float64 `json:"achieved_rate"`
	NSPerTick       float64 `json:"ns_per_tick"`
	Saturated       bool    `json:"saturated"` // feed could not keep up with target (service only)
	RSSSeed         uint64  `json:"rss_seed_bytes"`
	RSSPlateau      uint64  `json:"rss_plateau_bytes"`
	RSSPeak         uint64  `json:"rss_peak_bytes"`
	HeapInuse       uint64  `json:"heap_inuse_bytes"`
	GCCount         uint32  `json:"gc_count"`
	CPUSeconds      float64 `json:"cpu_seconds"`
	CPUPctOneCore   float64 `json:"cpu_pct_one_core"`
	DBBytes         int64   `json:"db_bytes"`
	Fired           uint64  `json:"triggers_fired"`
	Published       uint64  `json:"triggers_published"`
	PublishDropped  uint64  `json:"triggers_publish_dropped"`
	RingDropped     uint64  `json:"ring_dropped"`
	DrainMillis     int64   `json:"drain_ms"` // -1 = not a burst scenario
	WallMillis      int64   `json:"wall_ms"`
	Valid           bool    `json:"valid"`
	InvalidReasons  []string `json:"invalid_reasons,omitempty"`
}

// Validate applies the run-validity gates in place. A run is valid when:
// no drops, firing matches the layout (parked: none; gap: conservation
// fired+ring_dropped == cluster; trickle: some but not all), achieved
// rate >= 95% of target (unless the feed saturated), and RSS/CPU present.
func Validate(s *Summary)
```

- [ ] **Step 1: Write the failing tests**

Create `internal/bench/summary_test.go`:

```go
package bench

import "testing"

// base returns a summary that passes every gate for the parked layout.
func base() Summary {
	return Summary{
		Params:       Params{Layer: "engine", Scenario: "baseline", Layout: "parked", Alerts: 1000, Symbols: 500, Rate: 20000},
		Sent:         100000, Accepted: 100000,
		AchievedRate: 19950, NSPerTick: 50,
		RSSSeed: 1 << 20, RSSPlateau: 2 << 20, RSSPeak: 3 << 20,
		HeapInuse: 1 << 20, GCCount: 5, CPUSeconds: 10, CPUPctOneCore: 250,
		DrainMillis: -1, WallMillis: 5000,
	}
}

func TestValidateParkedPasses(t *testing.T) {
	s := base()
	Validate(&s)
	if !s.Valid {
		t.Fatalf("clean parked run invalid: %v", s.InvalidReasons)
	}
}

func TestValidateDropFails(t *testing.T) {
	s := base()
	s.Dropped = 3
	Validate(&s)
	if s.Valid || len(s.InvalidReasons) != 1 {
		t.Fatalf("drop not gated: valid=%v reasons=%v", s.Valid, s.InvalidReasons)
	}
}

func TestValidateParkedFireFails(t *testing.T) {
	s := base()
	s.Fired = 7
	Validate(&s)
	if s.Valid {
		t.Fatal("parked layout fired but run counted valid")
	}
}

func TestValidateGapConservation(t *testing.T) {
	s := base()
	s.Params.Layout, s.Params.Cluster = "gap", 1000
	s.Fired, s.RingDropped = 990, 10 // 10 fell off the ring: finding, still valid
	Validate(&s)
	if !s.Valid {
		t.Fatalf("conserved burst marked invalid: %v", s.InvalidReasons)
	}
	s.Fired = 500 // 490 + 10 lost: seeding bug
	Validate(&s)
	if s.Valid {
		t.Fatal("unconserved burst counted valid")
	}
}

func TestValidateTrickleWindow(t *testing.T) {
	s := base()
	s.Params.Layout = "trickle"
	s.Fired = 1
	Validate(&s)
	if !s.Valid {
		t.Fatalf("sparse trickle invalid: %v", s.InvalidReasons)
	}
	s.Fired = uint64(s.Params.Alerts)
	Validate(&s)
	if s.Valid {
		t.Fatal("trickle burned the whole ladder and still valid")
	}
}

func TestValidateRateGateAndSaturation(t *testing.T) {
	s := base()
	s.AchievedRate = 10000 // 50% of target
	Validate(&s)
	if s.Valid {
		t.Fatal("half-rate run counted valid")
	}
	s.Saturated = true // the finding IS that the service saturated
	Validate(&s)
	if !s.Valid {
		t.Fatalf("saturated run invalid: %v", s.InvalidReasons)
	}
}

func TestValidateRequiresMeasurements(t *testing.T) {
	s := base()
	s.RSSPeak, s.CPUSeconds = 0, 0
	Validate(&s)
	if s.Valid || len(s.InvalidReasons) != 2 {
		t.Fatalf("missing measurements not gated: %v", s.InvalidReasons)
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/bench/ -run TestValidate -count=1`
Expected: FAIL with `undefined: Validate` / `Params`.

- [ ] **Step 3: Implement**

Create `internal/bench/summary.go`:

```go
package bench

// Params identifies one scenario run; it rides inside the summary so the
// gates (and the report) are self-describing.
type Params struct {
	Layer    string `json:"layer"`
	Scenario string `json:"scenario"`
	Layout   string `json:"layout"`
	Alerts   int    `json:"alerts"`
	Symbols  int    `json:"symbols"`
	Cluster  int    `json:"cluster"`
	Rate     int    `json:"rate"`
}

// Summary is one run's measurements. Drivers (engine) or the collector
// (service) fill it; Validate gates it; the campaign report tabulates it.
type Summary struct {
	Params         Params   `json:"params"`
	Sent           uint64   `json:"ticks_sent"`
	Accepted       uint64   `json:"ticks_accepted"`
	Dropped        uint64   `json:"ticks_dropped"`
	AchievedRate   float64  `json:"achieved_rate"`
	NSPerTick      float64  `json:"ns_per_tick"`
	Saturated      bool     `json:"saturated"`
	RSSSeed        uint64   `json:"rss_seed_bytes"`
	RSSPlateau     uint64   `json:"rss_plateau_bytes"`
	RSSPeak        uint64   `json:"rss_peak_bytes"`
	HeapInuse      uint64   `json:"heap_inuse_bytes"`
	GCCount        uint32   `json:"gc_count"`
	CPUSeconds     float64  `json:"cpu_seconds"`
	CPUPctOneCore  float64  `json:"cpu_pct_one_core"`
	DBBytes        int64    `json:"db_bytes"`
	Fired          uint64   `json:"triggers_fired"`
	Published      uint64   `json:"triggers_published"`
	PublishDropped uint64   `json:"triggers_publish_dropped"`
	RingDropped    uint64   `json:"ring_dropped"`
	DrainMillis    int64    `json:"drain_ms"`
	WallMillis     int64    `json:"wall_ms"`
	Valid          bool     `json:"valid"`
	InvalidReasons []string `json:"invalid_reasons,omitempty"`
}

// Validate applies the run-validity gates in place. Conservation replaces
// naive equality for bursts: fired + ring_dropped == cluster — a ring
// overflow is a finding, not an invalidation, while lost alerts mean the
// layout lied (seeding bug). Saturation likewise: a service that cannot
// keep up at target rate is exactly the measurement, so the 95% rate gate
// only applies when the feed was not back-pressured.
func Validate(s *Summary) {
	var why []string
	if s.Dropped != 0 {
		why = append(why, "tick drops > 0")
	}
	if s.Accepted != s.Sent {
		why = append(why, "accepted != sent")
	}
	switch s.Params.Layout {
	case "parked":
		if s.Fired != 0 {
			why = append(why, "zero-fire layout fired")
		}
	case "gap":
		if got := s.Fired + s.RingDropped; got != uint64(s.Params.Cluster) {
			why = append(why, fmt.Sprintf("burst conservation broken: fired+ring=%d, cluster=%d", got, s.Params.Cluster))
		}
	case "trickle":
		if s.Fired == 0 || s.Fired >= uint64(s.Params.Alerts) {
			why = append(why, fmt.Sprintf("trickle fired %d of %d: outside (0, all)", s.Fired, s.Params.Alerts))
		}
	default:
		why = append(why, "unknown layout "+s.Params.Layout)
	}
	if !s.Saturated && s.AchievedRate < 0.95*float64(s.Params.Rate) {
		why = append(why, fmt.Sprintf("achieved %.0f tps < 95%% of target %d (and not marked saturated)", s.AchievedRate, s.Params.Rate))
	}
	if s.RSSPeak == 0 {
		why = append(why, "no RSS samples")
	}
	if s.CPUSeconds <= 0 {
		why = append(why, "no CPU samples")
	}
	s.InvalidReasons = why
	s.Valid = len(why) == 0
}
```

(`summary.go` imports are `"fmt"` and nothing else beyond the package clause.)

- [ ] **Step 4: Run to verify they pass**

Run: `go test ./internal/bench/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/bench/
git commit -m "feat(bench): run summary and validity gates"
```

---

### Task 4: `scripts/benchengine` — the engine-layer driver

**Files:**
- Create: `scripts/benchengine/main.go`

**Interfaces:**
- Consumes: Tasks 1–3.
- Produces: `<out>/summary.json`, `<out>/cpu.pprof`, `<out>/heap.pprof`, `<out>/goroutine.txt` — run.sh (Task 7) consumes these.

- [ ] **Step 1: Write the driver**

Create `scripts/benchengine/main.go`:

```go
// Command benchengine drives the engine in-process for performance
// scenarios: seeds a layout, paces a tick stream at the target rate,
// times burst drains, and writes pprof profiles plus a gated summary.
// Pure engine — no gRPC, no store.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/bench"
)

func main() {
	layer := "engine"
	scenario := flag.String("scenario", "baseline", "scenario name (rides in the summary)")
	layout := flag.String("layout", "parked", "parked | trickle | gap")
	alerts := flag.Int("alerts", 1_000_000, "total alerts")
	symbols := flag.Int("symbols", 500, "symbol universe size")
	cluster := flag.Int("cluster", 0, "gap layout: cluster size k")
	rate := flag.Float64("rate", 20000, "target ticks/sec")
	duration := flag.Duration("duration", 4*time.Minute, "steady-load phase")
	seed := flag.Uint64("seed", 1, "market RNG seed")
	out := flag.String("out", ".", "results directory")
	flag.Parse()
	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}
	if *layout == "gap" && (*cluster <= 0 || *cluster >= *alerts) {
		log.Fatalf("gap layout needs 0 < cluster(%d) < alerts(%d)", *cluster, *alerts)
	}

	syms := bench.Symbols(*symbols)
	var specs []bench.Spec
	var gap bench.Quote
	switch *layout {
	case "parked":
		specs = bench.Parked(*alerts, syms)
	case "trickle":
		specs = bench.Trickle(*alerts, syms, 0.002)
	case "gap":
		specs, gap = bench.GapCluster(*alerts, *cluster, syms)
	default:
		log.Fatalf("unknown layout %q", *layout)
	}

	eng := engine.New(engine.DefaultConfig())
	defer eng.Close()

	pid := os.Getpid()
	t0 := time.Now()
	for i := range specs {
		if err := eng.Upsert(bench.EngineSpec(specs[i])); err != nil {
			log.Fatalf("upsert %d: %v", i, err)
		}
		if (i+1)%200_000 == 0 {
			log.Printf("seeded %d/%d alerts (%.1fs)", i+1, len(specs), time.Since(t0).Seconds())
		}
	}
	eng.Sync()
	rssSeed := bench.ReadRSS(pid)
	cpuSeed := bench.ReadCPUSeconds(pid)
	log.Printf("seed done: %d alerts in %.1fs, RSS %d MiB",
		len(specs), time.Since(t0).Seconds(), rssSeed>>20)

	// Trigger consumer: the ONLY Triggers() drainer, mirroring the pump.
	var fired atomic.Uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]engine.Trigger, 1024)
		for {
			if n := eng.Triggers().PopBatch(buf); n > 0 {
				fired.Add(uint64(n))
				continue
			}
			select {
			case <-stop:
				return
			case <-time.After(500 * time.Microsecond):
			}
		}
	}()

	cpuF, err := os.Create(*out + "/cpu.pprof")
	if err != nil {
		log.Fatal(err)
	}
	if err := pprof.StartCPUProfile(cpuF); err != nil {
		log.Fatal(err)
	}

	// Steady load: emitter-paced round-robin walk.
	m := bench.NewMarket(syms, *seed, *layout == "gap")
	em := &bench.Emitter{Target: *rate}
	const slice = 10 * time.Millisecond
	var sent uint64
	wallStart := time.Now()
	slices := int(*duration / slice)
	slow := 0 // slices that overran 2×: saturation evidence
	for s := 0; s < slices; s++ {
		want := em.Take(slice)
		sliceStart := time.Now()
		now := time.Now()
		for n := want; n > 0; n-- {
			q := m.Step()
			tk := bench.EngineTick(syms, q, now.UnixNano())
			eng.Match(&tk)
			sent++
		}
		if d := time.Since(sliceStart); d > 2*slice {
			slow++
		}
		if wait := slice - time.Since(sliceStart); wait > 0 {
			time.Sleep(wait)
		}
	}
	loadWall := time.Since(wallStart)

	// Burst: one gap tick, then time the drain to ring-empty.
	var drainMs int64 = -1
	if *layout == "gap" {
		before := fired.Load()
		gapStart := time.Now()
		tk := bench.EngineTick(syms, gap, time.Now().UnixNano())
		eng.Match(&tk)
		target := uint64(*cluster)
		deadline := time.Now().Add(120 * time.Second)
		for fired.Load()-before < target && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		drainMs = time.Since(gapStart).Milliseconds()
		if got := fired.Load() - before; got < target {
			log.Printf("drain incomplete after 120s: %d of %d", got, target)
		}
	}

	pprof.StopCPUProfile()
	_ = cpuF.Close()
	close(stop)
	<-done

	heapF, err := os.Create(*out + "/heap.pprof")
	if err != nil {
		log.Fatal(err)
	}
	if err := pprof.WriteHeapProfile(heapF); err != nil {
		log.Fatal(err)
	}
	_ = heapF.Close()
	gF, err := os.Create(*out + "/goroutine.txt")
	if err != nil {
		log.Fatal(err)
	}
	if err := pprof.Lookup("goroutine").WriteTo(gF, 1); err != nil {
		log.Fatal(err)
	}
	_ = gF.Close()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	st := eng.Stats()
	sum := bench.Summary{
		Params: bench.Params{
			Layer: layer, Scenario: *scenario, Layout: *layout,
			Alerts: *alerts, Symbols: *symbols, Cluster: *cluster, Rate: int(*rate),
		},
		Sent: sent, Accepted: sent,
		AchievedRate: float64(sent) / loadWall.Seconds(),
		NSPerTick:    float64(loadWall.Nanoseconds()) / float64(sent),
		Saturated:    slow*10 > slices, // >10% of slices overran: engine cannot keep pace
		RSSSeed:      rssSeed,
		RSSPlateau:   bench.ReadRSS(pid),
		RSSPeak:      bench.ReadPeakRSS(pid),
		HeapInuse:    ms.HeapInuse,
		GCCount:      ms.NumGC,
		CPUSeconds:   bench.ReadCPUSeconds(pid) - cpuSeed,
		Fired:        fired.Load(),
		RingDropped:  st.DroppedTriggers,
		DrainMillis:  drainMs,
		WallMillis:   time.Since(wallStart).Milliseconds(),
	}
	sum.CPUPctOneCore = 100 * sum.CPUSeconds / loadWall.Seconds()
	bench.Validate(&sum)
	b, _ := json.MarshalIndent(sum, "", "  ")
	if err := os.WriteFile(*out+"/summary.json", b, 0o644); err != nil {
		log.Fatal(err)
	}
	log.Printf("scenario %s/%s done: valid=%v achieved=%.0f tps rss_peak=%d MiB fired=%d drain=%dms",
		layer, *scenario, sum.Valid, sum.AchievedRate, sum.RSSPeak>>20, sum.Fired, sum.DrainMillis)
	if !sum.Valid {
		fmt.Fprintf(os.Stderr, "INVALID: %v\n", sum.InvalidReasons)
	}
}
```

Notes for the implementer:
- RSSPlateau here is simply end-of-load RSS: the engine heap is monotone after seed, so plateau == current; the service collector (Task 7) computes a real trailing mean.
- `log.Fatalf` on upsert error is correct: a failed seed poisons every gate downstream.
- The engine package must NOT be modified — if something seems missing from its API, the driver is wrong, not the engine.

- [ ] **Step 2: Verify it compiles and vets**

Run: `go vet ./scripts/benchengine/`
Expected: clean.

- [ ] **Step 3: Tiny end-to-end check (this is this task's test)**

Run:
```bash
go run ./scripts/benchengine -scenario tiny -layout parked -alerts 20000 -symbols 200 -rate 5000 -duration 3s -out "$CLAUDE_JOB_DIR/tmp/bench-tiny"
cat "$CLAUDE_JOB_DIR/tmp/bench-tiny/summary.json"
```
Expected: `"valid": true`, `achieved_rate` ≈ 5000, four artifact files present.
Then the gap path:
```bash
go run ./scripts/benchengine -scenario tiny-gap -layout gap -alerts 20000 -symbols 200 -cluster 5000 -rate 5000 -duration 3s -out "$CLAUDE_JOB_DIR/tmp/bench-tiny-gap"
```
Expected: `"valid": true`, `"triggers_fired": 5000`, `drain_ms` ≥ 0.

- [ ] **Step 4: Full suite still green**

Run: `go vet ./... && go test ./... -count=1`
Expected: PASS (engine untouched, bench package green).

- [ ] **Step 5: Commit**

```bash
git add scripts/benchengine/
git commit -m "feat(benchengine): in-process engine-layer perf driver"
```

---

### Task 5: chronod no-NATS mode

**Files:**
- Modify: `cmd/chronod/main.go:39` (flag doc), `:58-66` (publisher construction), `:86-88` (SubscribeTriggers guard)

**Interfaces:**
- Consumes: `pub.Publisher`, `pub.Noop` (both exist).
- Produces: `-nats-url ""` boots with a noop publisher — Tasks 6–7 depend on this.

- [ ] **Step 1: Make the change**

In `cmd/chronod/main.go`, replace the publisher block:

```go
	// NATS is a boot dependency: unreachable broker is a fatal error.
	// Later outages reconnect forever; publishes during them drop-and-count.
	publisher, err := pub.NewNATS(natsURL)
	if err != nil {
		return fmt.Errorf("nats: %w", err)
	}
```

with:

```go
	// NATS is a boot dependency: unreachable broker is a fatal error.
	// Later outages reconnect forever; publishes during them drop-and-count.
	// An empty URL is the no-broker mode: a noop publisher (publishes
	// succeed instantly) used by the performance campaign and any
	// broker-less deployment — the pump and store flips still run.
	var publisher pub.Publisher
	if natsURL == "" {
		publisher = pub.Noop{}
	} else {
		var err error
		publisher, err = pub.NewNATS(natsURL)
		if err != nil {
			return fmt.Errorf("nats: %w", err)
		}
	}
```

and the subscription block:

```go
	// Dashboard live feed: re-consume our own published triggers. The
	// handler fans out to browsers over SSE; it never blocks us.
	if err := publisher.SubscribeTriggers(statusSrv.HandleTrigger); err != nil {
		return fmt.Errorf("subscribe triggers: %w", err)
	}
```

with:

```go
	// Dashboard live feed: re-consume our own published triggers. The
	// handler fans out to browsers over SSE; it never blocks us.
	// Noop mode has no stream to subscribe to.
	if np, ok := publisher.(*pub.NATSPublisher); ok {
		if err := np.SubscribeTriggers(statusSrv.HandleTrigger); err != nil {
			return fmt.Errorf("subscribe triggers: %w", err)
		}
	}
```

Update the flag line (`main.go:41`):

```go
	natsURL := flag.String("nats-url", "nats://localhost:4222", "NATS server URL (trigger publishing); empty = no broker (noop publisher, no live trigger feed)")
```

- [ ] **Step 2: Verify**

Run: `go vet ./... && go test ./... -count=1`
Expected: PASS — nothing behavioral changed for non-empty URLs.
Then boot check (no unit tests exist for main; this is the test):

```bash
go build -o "$CLAUDE_JOB_DIR/tmp/chronod" ./cmd/chronod
"$CLAUDE_JOB_DIR/tmp/chronod" -nats-url "" -db "$CLAUDE_JOB_DIR/tmp/natsless.bbolt" -grpc-addr :19090 -http-addr :18080 &
sleep 2
curl -sf localhost:18080/stats | jq '.nats_connected, .uptime_sec'
kill %1
```
Expected: chronod boots without a broker, `/stats` answers (`nats_connected` true — Noop presents as a healthy sink), exit clean.

- [ ] **Step 3: Commit**

```bash
git add cmd/chronod/main.go
git commit -m "feat(chronod): -nats-url '' boots broker-less with noop publisher"
```

---

### Task 6: `scripts/benchfeed` — the service-layer driver

**Files:**
- Create: `scripts/benchfeed/main.go`

**Interfaces:**
- Consumes: Tasks 1–2 (`bench.Symbols/Parked/Trickle/GapCluster/Market/Emitter/StoreAlert/Tier/VenueName`), Task 5's broker-less chronod; `alertstore.Open/PutBatch/Close`; gRPC `chronov1.FeedServiceClient`, health client (chronofeed's dial pattern).
- Produces:
  - seed mode: seeds `<db>` and writes `<out>/seed.json` — `{layout, alerts, symbols, cluster, rate, venue}`
  - feed mode: writes `<out>/feed.json` — `{ticks_sent, ticks_accepted, ticks_dropped, saturated, drain_ms, drain_timeout, final_stats}` where `final_stats` is the last `/stats` snapshot (has `sys`, `engine`, `alerts_by_state`).

- [ ] **Step 1: Write the driver**

Create `scripts/benchfeed/main.go`:

```go
// Command benchfeed drives the service layer for performance scenarios:
// seed mode writes a scenario's alert layout into the bbolt store (before
// chronod opens it); feed mode streams the paced tick walk at a running
// chronod, fires the gap tick for burst layouts, and times the store-flip
// drain by polling /stats.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/alertstore"
	"github.com/emir/chrono-tree/internal/bench"
	"github.com/emir/chrono-tree/internal/price"
)

const seedChunk = 5000 // alerts per write tx, chronofeed's bulk-seed size

func main() {
	mode := flag.String("mode", "feed", "seed | feed")
	server := flag.String("server", "localhost:9090", "chronod gRPC address")
	dbPath := flag.String("db", "", "bbolt path (seed mode)")
	statsAddr := flag.String("stats", "localhost:8080", "chronod HTTP address for /stats drain polling")
	scenario := flag.String("scenario", "baseline", "scenario name")
	layout := flag.String("layout", "parked", "parked | trickle | gap")
	alerts := flag.Int("alerts", 1_000_000, "total alerts")
	symbols := flag.Int("symbols", 500, "symbol universe size")
	cluster := flag.Int("cluster", 0, "gap layout: cluster size k")
	rate := flag.Float64("rate", 20000, "target ticks/sec")
	duration := flag.Duration("duration", 4*time.Minute, "steady-load phase")
	seed := flag.Uint64("seed", 1, "market RNG seed")
	out := flag.String("out", ".", "results directory")
	flag.Parse()
	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}
	if *layout == "gap" && (*cluster <= 0 || *cluster >= *alerts) {
		log.Fatalf("gap layout needs 0 < cluster(%d) < alerts(%d)", *cluster, *alerts)
	}
	if *mode == "seed" {
		seedStore(*dbPath, *scenario, *layout, *alerts, *symbols, *cluster, *out, *rate)
		return
	}
	if err := feed(*server, *statsAddr, *scenario, *layout, *alerts, *symbols, *cluster, *rate, *duration, *seed, *out); err != nil {
		log.Fatal(err)
	}
}

func buildLayout(layout string, alerts, symbols, cluster int) ([]bench.Spec, bench.Quote) {
	syms := bench.Symbols(symbols)
	switch layout {
	case "parked":
		return bench.Parked(alerts, syms), bench.Quote{}
	case "trickle":
		return bench.Trickle(alerts, syms, 0.002), bench.Quote{}
	case "gap":
		return bench.GapCluster(alerts, cluster, syms)
	}
	log.Fatalf("unknown layout %q", layout)
	return nil, bench.Quote{}
}

func seedStore(dbPath, scenario, layout string, alerts, symbols, cluster int, out string, rate float64) {
	if dbPath == "" {
		log.Fatal("seed mode needs -db")
	}
	specs, _ := buildLayout(layout, alerts, symbols, cluster)
	store, err := alertstore.Open(dbPath)
	if err != nil {
		log.Fatalf("open store: %v (chronod already running with this db?)", err)
	}
	now := time.Now().UnixNano()
	t0 := time.Now()
	for i := 0; i < len(specs); i += seedChunk {
		end := min(i+seedChunk, len(specs))
		chunk := make([]alertstore.Alert, 0, end-i)
		for _, s := range specs[i:end] {
			chunk = append(chunk, bench.StoreAlert(s, now))
		}
		if err := store.PutBatch(chunk); err != nil {
			log.Fatalf("seed %d: %v", end, err)
		}
		if end%200_000 == 0 || end == len(specs) {
			log.Printf("seeded %d/%d (%.1fs)", end, len(specs), time.Since(t0).Seconds())
		}
	}
	if err := store.Close(); err != nil {
		log.Fatal(err)
	}
	meta := map[string]any{
		"scenario": scenario, "layout": layout, "alerts": len(specs),
		"symbols": symbols, "cluster": cluster, "rate": rate, "venue": bench.VenueName,
	}
	writeJSON(out+"/seed.json", meta)
	log.Printf("seed done: %d alerts into %s", len(specs), dbPath)
}
```

Wait — `outScenario` is undeclared. Pass `scenario` into `seedStore` instead (fix the signature: `seedStore(dbPath, scenario, layout string, alerts, symbols, cluster int, out string, rate float64)` and use it in the map). Continue:

```go
func feed(server, statsAddr, scenario, layout string, alerts, symbols, cluster int, rate float64, duration time.Duration, seed uint64, out string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := grpc.NewClient(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	hc := healthpb.NewHealthClient(conn)
	log.Printf("waiting for chronod %s", server)
	for {
		check, err := hc.Check(ctx, &healthpb.HealthCheckRequest{})
		if err == nil && check.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			break
		}
		if ctx.Err() != nil {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}

	fc := chronov1.NewFeedServiceClient(conn)
	stream, err := fc.StreamTicks(ctx)
	if err != nil {
		return err
	}
	syms := bench.Symbols(symbols)
	_, gap := buildLayout(layout, alerts, symbols, cluster) // specs live in the store; only the gap quote is needed here
	m := bench.NewMarket(syms, seed, layout == "gap")
	em := &bench.Emitter{Target: rate}
	const batchMax, slice = 256, 10*time.Millisecond
	batch := make([]*chronov1.Tick, 0, batchMax)
	send := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := stream.Send(&chronov1.TickBatch{Ticks: batch}); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	dec := func(i int) uint8 { return syms[i].Decimals }
	wire := func(q bench.Quote, now time.Time) *chronov1.Tick {
		d := dec(q.SymIdx)
		return &chronov1.Tick{
			Symbol: syms[q.SymIdx].Name,
			Bid:    price.Format(q.Bid, d), Ask: price.Format(q.Ask, d),
			Venue: bench.VenueName, Tier: bench.Tier(q.TierIdx),
			TsUnixNanos: now.UnixNano(),
		}
	}

	var sent uint64
	slices := int(duration / slice)
	slow := 0
	t0 := time.Now()
	for s := 0; s < slices && ctx.Err() == nil; s++ {
		want := em.Take(slice)
		sliceStart := time.Now()
		now := time.Now()
		for n := want; n > 0; n-- {
			q := m.Step()
			batch = append(batch, wire(q, now))
			sent++
			if len(batch) >= batchMax {
				if err := send(); err != nil {
					return err
				}
			}
		}
		if err := send(); err != nil {
			return err
		}
		if d := time.Since(sliceStart); d > 2*slice {
			slow++
		}
		if wait := slice - time.Since(sliceStart); wait > 0 {
			time.Sleep(wait)
		}
	}
	loadWall := time.Since(t0)

	// Burst: one gap tick, then time the drain by polling /stats until the
	// pump has fired the whole cluster (or 120s).
	drainMs, drainTimeout := int64(-1), false
	if layout == "gap" && ctx.Err() == nil {
		now := time.Now()
		if err := stream.Send(&chronov1.TickBatch{Ticks: []*chronov1.Tick{wire(gap, now)}}); err != nil {
			return err
		}
		gapStart := time.Now()
		deadline := gapStart.Add(120 * time.Second)
		for time.Now().Before(deadline) {
			fired := statsFired(statsAddr)
			if fired >= uint64(cluster) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		drainMs = time.Since(gapStart).Milliseconds()
		if statsFired(statsAddr) < uint64(cluster) {
			drainTimeout = true
		}
	}

	fs, err := stream.CloseAndRecv()
	if err != nil && err != io.EOF {
		return fmt.Errorf("close stream: %w", err)
	}
	accepted, dropped := uint64(0), uint64(0)
	if fs != nil {
		accepted, dropped = fs.GetAccepted(), fs.GetDropped()
	}
	if layout == "gap" {
		accepted++ // the gap tick itself
		sent++
	}
	final := statsSnapshot(statsAddr)
	res := map[string]any{
		"ticks_sent": sent, "ticks_accepted": accepted,
		"ticks_dropped": dropped, "saturated": slow*10 > slices,
		"drain_ms": drainMs, "drain_timeout": drainTimeout,
		"load_wall_ms": loadWall.Milliseconds(), "final_stats": final,
	}
	writeJSON(out+"/feed.json", res)
	log.Printf("feed done: sent=%d accepted=%d dropped=%d saturated=%v drain=%dms",
		sent, accepted, dropped, slow*10 > slices, drainMs)
	return nil
}
```

(`sent` counts every tick built by the pacing loop plus the gap tick — pacing emits exactly what it builds, so built == sent.)

Helpers (same file):

```go
// statsSnapshot fetches /stats once; nil if unreachable.
func statsSnapshot(addr string) map[string]any {
	r, err := http.Get("http://" + addr + "/stats")
	if err != nil {
		return nil
	}
	defer func() { _ = r.Body.Close() }()
	var m map[string]any
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		return nil
	}
	return m
}

// statsFired reads triggers_fired from /stats (0 if unreachable).
func statsFired(addr string) uint64 {
	m := statsSnapshot(addr)
	if m == nil {
		return 0
	}
	f, _ := m["triggers_fired"].(float64)
	return uint64(f)
}

func writeJSON(path string, v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		log.Fatal(err)
	}
}
```

- [ ] **Step 2: Verify compile + vet**

Run: `go vet ./scripts/benchfeed/`
Expected: clean.

- [ ] **Step 3: Tiny end-to-end check against a real chronod**

```bash
T="$CLAUDE_JOB_DIR/tmp/feedtiny"; mkdir -p "$T"
go run ./scripts/benchfeed -mode seed -db "$T/a.bbolt" -layout parked -alerts 5000 -symbols 200 -rate 5000 -out "$T"
go build -o "$T/chronod" ./cmd/chronod
"$T/chronod" -nats-url "" -db "$T/a.bbolt" -grpc-addr :19091 -http-addr :18081 & CPID=$!
go run ./scripts/benchfeed -mode feed -server localhost:19091 -stats localhost:18081 -layout parked -alerts 5000 -symbols 200 -rate 5000 -duration 5s -out "$T"
kill $CPID
cat "$T/feed.json" | jq '.ticks_accepted, .ticks_dropped'
```
Expected: accepted ≈ 25000, dropped 0. Then the gap path end-to-end (same steps with `-layout gap -cluster 2000`, drain_ms ≥ 0, no drain_timeout, and `final_stats.triggers_fired` ≥ 2000 — replay of the cluster means the gap tick must fire exactly 2000; check `.final_stats.triggers_fired == 2000`).

- [ ] **Step 4: Full suite**

Run: `go vet ./... && go test ./... -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/benchfeed/
git commit -m "feat(benchfeed): service-layer seeding and paced feed driver"
```

---

### Task 7: orchestrator, collector, smoke test

**Files:**
- Create: `scripts/perf/run.sh`
- Create: `scripts/perf/smoke.sh`
- Create: `scripts/perfcollect/main.go` (collector + gates for service runs)
- Test: `scripts/perfcollect/main_test.go`, `tests/perf_smoke_test.go`

**Interfaces:**
- Consumes: Tasks 1–6; `/debug/pprof` (heap, goroutine, `profile?seconds=30`), `/stats` 1 Hz (`sys.rss_bytes`, `sys.proc_cpu_percent`, `sys.db_bytes`, `engine.dropped_triggers`, `triggers_fired`), `bench.Validate`.
- Produces: `docs/perf/2026-09-06-campaign/<layer>-<scenario>/{summary.json,cpu.pprof,heap.pprof,goroutine.txt,pprof-top.txt,stats.jsonl,run.log,seed.json}` and the smoke proof.

- [ ] **Step 1: Write the collector's failing test**

Create `scripts/perfcollect/main_test.go`:

```go
package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/emir/chrono-tree/internal/bench"
)

func write(t *testing.T, dir, name string, v any) {
	t.Helper()
	b, _ := json.Marshal(v)
	if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCollectServiceRun(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "seed.json", map[string]any{
		"scenario": "baseline", "layout": "parked", "alerts": 1000,
		"symbols": 500, "cluster": 0, "rate": 20000,
	})
	write(t, dir, "feed.json", map[string]any{
		"ticks_sent": 100000, "ticks_accepted": 100000, "ticks_dropped": 0,
		"saturated": false, "drain_ms": -1, "load_wall_ms": 5000,
		"final_stats": map[string]any{
			"triggers_fired": 0,
			"engine":         map[string]any{"dropped_triggers": 0},
			"sys": map[string]any{
				"rss_bytes": 2097152, "db_bytes": 1048576, "proc_cpu_percent": 150.0,
			},
		},
	})
	// 1 Hz stats log: 120 samples, rss climbing then flat.
	lines := "sys:values\n"
	for i := 0; i < 120; i++ {
		rss := 1048576 + i*16384 // ramp for 60, flat after
		if i >= 60 {
			rss = 1048576 + 60*16384
		}
		l, _ := json.Marshal(map[string]any{
			"sys": map[string]any{"rss_bytes": rss, "proc_cpu_percent": 150.0},
		})
		lines += string(l) + "\n"
	}
	if err := os.WriteFile(filepath.Join(dir, "stats.jsonl"), []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	// Profile gate fixtures: non-empty profile files.
	os.WriteFile(filepath.Join(dir, "cpu.pprof"), []byte("profile"), 0o644)
	os.WriteFile(filepath.Join(dir, "heap.pprof"), []byte("profile"), 0o644)

	sum := collect(dir)
	if !sum.Valid {
		t.Fatalf("clean run invalid: %v", sum.InvalidReasons)
	}
	if sum.RSSPeak < sum.RSSPlateau || sum.RSSPlateau < sum.RSSSeed {
		t.Fatalf("rss order wrong: seed=%d plateau=%d peak=%d", sum.RSSSeed, sum.RSSPlateau, sum.RSSPeak)
	}
	if sum.CPUPctOneCore != 150 {
		t.Fatalf("cpu%% = %v, want 150", sum.CPUPctOneCore)
	}
	if sum.DBBytes != 1048576 {
		t.Fatalf("db bytes = %d", sum.DBBytes)
	}
	if sum.AchievedRate != 20 { // 100000 / 5000ms
		t.Fatalf("achieved = %v, want 20", sum.AchievedRate)
	}
	if sum.Params.Layer != "service" {
		t.Fatalf("layer = %s", sum.Params.Layer)
	}
	// summary.json written and re-readable
	b, err := os.ReadFile(filepath.Join(dir, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var again bench.Summary
	if err := json.Unmarshal(b, &again); err != nil {
		t.Fatal(err)
	}
	if !again.Valid {
		t.Fatal("written summary not valid")
	}
}

func TestCollectFlagsInvalidRun(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "seed.json", map[string]any{
		"scenario": "baseline", "layout": "parked", "alerts": 1000,
		"symbols": 500, "cluster": 0, "rate": 20000,
	})
	write(t, dir, "feed.json", map[string]any{
		"ticks_sent": 1000, "ticks_accepted": 990, "ticks_dropped": 10,
		"final_stats": map[string]any{"triggers_fired": 0, "sys": map[string]any{"rss_bytes": 1}},
	})
	os.WriteFile(filepath.Join(dir, "stats.jsonl"), []byte("\n"), 0o644)
	sum := collect(dir)
	if sum.Valid {
		t.Fatal("dropping run counted valid")
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./scripts/perfcollect/ -count=1`
Expected: FAIL — `undefined: collect`.

- [ ] **Step 3: Implement the collector**

Create `scripts/perfcollect/main.go`:

```go
// Command perfcollect merges a service-layer run directory (seed.json +
// feed.json + 1 Hz stats.jsonl) into a gated bench.Summary. Engine runs
// need no collector — benchengine writes summary.json itself.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/emir/chrono-tree/internal/bench"
)

func main() {
	dir := flag.String("dir", ".", "run directory")
	flag.Parse()
	sum := collect(*dir)
	b, _ := json.MarshalIndent(sum, "", "  ")
	if err := os.WriteFile(filepath.Join(*dir, "summary.json"), b, 0o644); err != nil {
		log.Fatal(err)
	}
	log.Printf("collected %s/%s: valid=%v", sum.Params.Layer, sum.Params.Scenario, sum.Valid)
	if !sum.Valid {
		log.Printf("INVALID: %v", sum.InvalidReasons)
	}
}

func readJSON(dir, name string, v any) bool {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return false
	}
	return json.Unmarshal(b, v) == nil
}

type seedMeta struct {
	Scenario string `json:"scenario"`
	Layout   string `json:"layout"`
	Alerts   int    `json:"alerts"`
	Symbols  int    `json:"symbols"`
	Cluster  int    `json:"cluster"`
	Rate     int    `json:"rate"`
}

type feedMeta struct {
	Sent         uint64 `json:"ticks_sent"`
	Accepted     uint64 `json:"ticks_accepted"`
	Dropped      uint64 `json:"ticks_dropped"`
	Saturated    bool   `json:"saturated"`
	DrainMS      int64  `json:"drain_ms"`
	DrainTimeout bool   `json:"drain_timeout"`
	LoadWallMS   int64  `json:"load_wall_ms"`
	Final        struct {
		TriggersFired uint64 `json:"triggers_fired"`
		Published     uint64 `json:"triggers_published"`
		PubDropped    uint64 `json:"triggers_publish_dropped"`
		Engine        struct {
			DroppedTriggers uint64 `json:"dropped_triggers"`
		} `json:"engine"`
		Sys struct {
			RSSBytes     uint64  `json:"rss_bytes"`
			DBBytes      int64   `json:"db_bytes"`
			ProcCPUPct   float64 `json:"proc_cpu_percent"`
			HeapBytes    uint64  `json:"heap_bytes"`
		} `json:"sys"`
	} `json:"final_stats"`
}

type statLine struct {
	Sys struct {
		RSSBytes   uint64  `json:"rss_bytes"`
		ProcCPUPct float64 `json:"proc_cpu_percent"`
	} `json:"sys"`
}

func collect(dir string) bench.Summary {
	var sum bench.Summary
	var seed seedMeta
	var feed feedMeta
	readJSON(dir, "seed.json", &seed)
	readJSON(dir, "feed.json", &feed)

	sum = bench.Summary{
		Params: bench.Params{
			Layer: "service", Scenario: seed.Scenario, Layout: seed.Layout,
			Alerts: seed.Alerts, Symbols: seed.Symbols, Cluster: seed.Cluster, Rate: seed.Rate,
		},
		Sent: feed.Sent, Accepted: feed.Accepted, Dropped: feed.Dropped,
		Saturated: feed.Saturated, DrainMillis: feed.DrainMS,
		Fired:         feed.Final.TriggersFired,
		Published:     feed.Final.Published,
		PublishDropped: feed.Final.PubDropped,
		RingDropped:   feed.Final.Engine.DroppedTriggers,
		DBBytes:       feed.Final.Sys.DBBytes,
		HeapInuse:     feed.Final.Sys.HeapBytes,
		WallMillis:    feed.LoadWallMS,
	}
	if feed.LoadWallMS > 0 {
		sum.AchievedRate = float64(feed.Accepted) / (float64(feed.LoadWallMS) / 1000)
		sum.NSPerTick = float64(feed.LoadWallMS) * 1e6 / float64(feed.Accepted)
	}
	sum.RSSSeed = feed.Final.Sys.RSSBytes // replaced by first stats sample below

	// 1 Hz stats log: peak = max rss; plateau = mean of the last 60
	// samples; cpu% = mean proc_cpu_percent across the window.
	f, err := os.Open(filepath.Join(dir, "stats.jsonl"))
	if err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		var rss []uint64
		var cpu []float64
		first := true
		for sc.Scan() {
			var l statLine
			if json.Unmarshal(sc.Bytes(), &l) != nil || l.Sys.RSSBytes == 0 {
				continue
			}
			if first {
				sum.RSSSeed = l.Sys.RSSBytes // post-replay steady point
				first = false
			}
			rss = append(rss, l.Sys.RSSBytes)
			cpu = append(cpu, l.Sys.ProcCPUPct)
		}
		_ = f.Close()
		for _, r := range rss {
			if r > sum.RSSPeak {
				sum.RSSPeak = r
			}
		}
		if n := len(rss); n > 0 {
			tail := rss[n-min(60, n):]
			var acc uint64
			for _, r := range tail {
				acc += r
			}
			sum.RSSPlateau = acc / uint64(len(tail))
		}
		if len(cpu) > 0 {
			var acc float64
			for _, c := range cpu {
				acc += c
			}
			sum.CPUPctOneCore = acc / float64(len(cpu))
			sum.CPUSeconds = sum.CPUPctOneCore / 100 * float64(feed.LoadWallMS) / 1000
		}
	}
	if feed.DrainTimeout {
		sum.InvalidReasons = append(sum.InvalidReasons, "drain exceeded 120s")
	}
	// Spec gate: profiles present and non-empty — a silent fetch failure
	// must not pass as a clean run.
	for _, p := range []string{"cpu.pprof", "heap.pprof"} {
		if fi, err := os.Stat(filepath.Join(dir, p)); err != nil || fi.Size() == 0 {
			sum.InvalidReasons = append(sum.InvalidReasons, "missing or empty "+p)
		}
	}
	bench.Validate(&sum)
	b, _ := json.MarshalIndent(sum, "", "  ")
	_ = os.WriteFile(filepath.Join(dir, "summary.json"), b, 0o644)
	return sum
}
```

The test's first stats.jsonl line is `"sys:values"` (invalid JSON) — the scanner skips it via the unmarshal error; `RSSSeed` then comes from the first valid line (1048576), which is what the ordering assertion expects.

- [ ] **Step 4: Run to verify they pass**

Run: `go test ./scripts/perfcollect/ -count=1`
Expected: PASS.

- [ ] **Step 5: Write the orchestrator**

Create `scripts/perf/run.sh` (executable, `chmod +x`):

```bash
#!/usr/bin/env bash
# run.sh <engine|service> <scenario> <results-root>
# Scenarios: baseline rate-100k rate-200k sym-1000 sym-2000 trickle
#            burst-100k burst-500k corner smoke
set -uo pipefail
cd "$(dirname "$0")/../.."

LAYER="$1"; SCN="$2"; ROOT="${3:-docs/perf/2026-09-06-campaign}"
ALERTS=1000000; SYMBOLS=500; RATE=20000; LAYOUT=parked; CLUSTER=0; DUR=240s
case "$SCN" in
  baseline)   ;;
  rate-100k)  RATE=100000 ;;
  rate-200k)  RATE=200000 ;;
  sym-1000)   SYMBOLS=1000 ;;
  sym-2000)   SYMBOLS=2000 ;;
  trickle)    LAYOUT=trickle ;;
  burst-100k) LAYOUT=gap; CLUSTER=100000 ;;
  burst-500k) LAYOUT=gap; CLUSTER=500000 ;;
  corner)     LAYOUT=gap; CLUSTER=100000; RATE=200000; SYMBOLS=2000 ;;
  smoke)      ALERTS=1000; SYMBOLS=200; RATE=5000; DUR=10s ;;
  *) echo "unknown scenario $SCN" >&2; exit 2 ;;
esac

DIR="$ROOT/$LAYER-$SCN"
mkdir -p "$DIR"
LOG="$DIR/run.log"
echo "=== $LAYER/$SCN layout=$LAYOUT alerts=$ALERTS symbols=$SYMBOLS rate=$RATE cluster=$CLUSTER dur=$DUR ===" | tee "$LOG"

if [ "$LAYER" = engine ]; then
  mkdir -p "$ROOT/.bin"
  go build -o "$ROOT/.bin/benchengine" ./scripts/benchengine
  "$ROOT/.bin/benchengine" -scenario "$SCN" -layout "$LAYOUT" -alerts "$ALERTS" \
    -symbols "$SYMBOLS" -cluster "$CLUSTER" -rate "$RATE" -duration "$DUR" -out "$DIR" 2>&1 | tee -a "$LOG"
  go tool pprof -top -nodecount=25 "$ROOT/.bin/benchengine" "$DIR/cpu.pprof" > "$DIR/pprof-top.txt" 2>>"$LOG" || true
  grep -q '"valid": true' "$DIR/summary.json"; RC=$?
else
  TMP="$DIR/tmp"; mkdir -p "$TMP"
  go run ./scripts/benchfeed -mode seed -db "$TMP/alerts.bbolt" -scenario "$SCN" \
    -layout "$LAYOUT" -alerts "$ALERTS" -symbols "$SYMBOLS" -cluster "$CLUSTER" \
    -rate "$RATE" -out "$DIR" 2>&1 | tee -a "$LOG"
  go build -o "$TMP/chronod" ./cmd/chronod
  "$TMP/chronod" -nats-url "" -db "$TMP/alerts.bbolt" -grpc-addr :19090 -http-addr :18080 \
    > "$TMP/chronod.log" 2>&1 &
  CPID=$!
  # 1 Hz stats sampler (runs the whole scenario, captures replay + load +
  # drain) and RSS cap: >8 GiB kills chronod before the box OOMs.
  RSS_CAP=$(( 8 * 1024 * 1024 * 1024 ))
  ( while kill -0 $CPID 2>/dev/null; do
      S=$(curl -sf localhost:18080/stats) || { sleep 1; continue; }
      echo "$S" >> "$DIR/stats.jsonl"
      if echo "$S" | jq -e --argjson cap $RSS_CAP '.sys.rss_bytes > $cap' >/dev/null 2>&1; then
        echo "RSS CAP EXCEEDED — killing chronod" >> "$LOG"; kill $CPID
      fi
      sleep 1
    done ) &
  SPID=$!
  # Wait for readiness (replay of 1M alerts can take minutes — untimed).
  until curl -sf localhost:18080/readyz >/dev/null; do
    kill -0 $CPID 2>/dev/null || { echo "chronod died during boot" | tee -a "$LOG"; break; }
    sleep 1
  done
  # Mid-run 30s CPU profile: start it at half the load phase.
  DURS=$(( ${DUR%s} ))
  ( sleep $(( DURS/2 - 15 )); curl -sf "localhost:18080/debug/pprof/profile?seconds=30" > "$DIR/cpu.pprof" ) &
  PPID2=$!
  go run ./scripts/benchfeed -mode feed -server localhost:19090 -stats localhost:18080 -scenario "$SCN" \
    -layout "$LAYOUT" -alerts "$ALERTS" -symbols "$SYMBOLS" -cluster "$CLUSTER" \
    -rate "$RATE" -duration "$DUR" -out "$DIR" 2>&1 | tee -a "$LOG"
  FEEDRC=$?
  curl -sf localhost:18080/debug/pprof/heap > "$DIR/heap.pprof" || echo "heap profile failed" | tee -a "$LOG"
  curl -sf "localhost:18080/debug/pprof/goroutine?debug=1" > "$DIR/goroutine.txt" || true
  kill $CPID 2>/dev/null; wait $CPID 2>/dev/null; kill $SPID $PPID2 2>/dev/null
  go run ./scripts/perfcollect -dir "$DIR" 2>&1 | tee -a "$LOG"
  go tool pprof -top -nodecount=25 "$TMP/chronod" "$DIR/cpu.pprof" > "$DIR/pprof-top.txt" 2>>"$LOG" || true
  rm -rf "$TMP"
  grep -q '"valid": true' "$DIR/summary.json"; RC=$?
fi
if [ $RC -eq 0 ]; then echo "RUN $LAYER/$SCN: VALID" | tee -a "$LOG"
else echo "RUN $LAYER/$SCN: INVALID (see summary.json)" | tee -a "$LOG"; fi
exit $RC
```

Both sides of the service branch agree on ports: chronod listens `-grpc-addr :19090 -http-addr :18080`; benchfeed dials `-server localhost:19090 -stats localhost:18080`; pprof/stats curls hit 18080. A run killed by the RSS cap fails its own gates (accepted != sent) and lands INVALID with the log line as the reason.

Create `scripts/perf/smoke.sh`:

```bash
#!/usr/bin/env bash
# smoke.sh: both layers, tiny parameters, ~40s total. CI gate for the harness.
set -uo pipefail
cd "$(dirname "$0")/../.."
ROOT="${CLAUDE_JOB_DIR:-/tmp}/perf-smoke"
rm -rf "$ROOT"; mkdir -p "$ROOT"
bash scripts/perf/run.sh engine smoke "$ROOT" || exit 1
bash scripts/perf/run.sh service smoke "$ROOT" || exit 1
echo "SMOKE OK"
```

`chmod +x scripts/perf/run.sh scripts/perf/smoke.sh`.

- [ ] **Step 6: Write the smoke test**

Create `tests/perf_smoke_test.go`:

```go
package tests

import (
	"os/exec"
	"testing"
	"time"
)

// TestPerfSmoke runs the full harness (both layers) at tiny parameters.
// It is the CI-proof that seeding, feeding, profiling, collecting, and
// the validity gates all still work together.
func TestPerfSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("smoke run is minutes-scale")
	}
	cmd := exec.Command("bash", "../scripts/perf/smoke.sh") // cwd is tests/; smoke.sh cds to the repo root
	done := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("smoke failed: %v", err)
		}
	case <-time.After(3 * time.Minute):
		_ = cmd.Process.Kill()
		t.Fatal("smoke exceeded 3 minutes")
	}
}
```

(The test runs from the repo root — `go test ./tests/` sets the working directory to the package dir `tests/`; fix the command to `exec.Command("bash", "../scripts/perf/smoke.sh")` and inside smoke.sh the `cd` already normalizes. Verify which applies by running it.)

- [ ] **Step 7: Run the smoke**

Run: `go test ./tests/ -run TestPerfSmoke -count=1 -v` (allow ~3 min)
Expected: PASS, output contains `RUN engine/smoke: VALID` and `RUN service/smoke: VALID`.

- [ ] **Step 8: Full suite + commit**

Run: `go vet ./... && go test ./... -count=1`
Expected: PASS.

```bash
git add scripts/perf/ scripts/perfcollect/ tests/perf_smoke_test.go
git commit -m "feat(perf): scenario orchestrator, service collector, smoke gate"
```

---

### Task 8: run the campaign + write the report

**Files:**
- Create: `docs/perf/2026-09-06-campaign/REPORT.md` (+ per-run `analysis.md` files inside each run directory)

**Interfaces:**
- Consumes: Task 7's `run.sh`; subagent dispatch (this task is executed across multiple subagents per the spec's execution section).

- [ ] **Step 1: Pre-flight**

Verify headroom before any launch: `free -g` must show ≥ 6 GB available; if not, stop and report (do not OOM the box). Confirm `git status` clean of staged harness files and the box otherwise quiet.

- [ ] **Step 2: Engine-layer runs (16 × ~5 min)**

```bash
for scn in baseline rate-100k rate-200k sym-1000 sym-2000 trickle burst-100k burst-500k; do
  bash scripts/perf/run.sh engine "$scn" || echo "engine/$scn INVALID"
done
```
Serial (each ~700 MB RSS). Two-at-a-time is allowed if `free -g` stays ≥ 4 GB, but serial is the default.

- [ ] **Step 3: Service-layer runs (serial, RAM-capped)**

```bash
for scn in baseline rate-100k rate-200k sym-1000 sym-2000 trickle burst-100k burst-500k; do
  bash scripts/perf/run.sh service "$scn" || echo "service/$scn INVALID"
done
```
Strictly serial; before each run re-check headroom. Optional `corner` scenario last if wall-clock allows.

- [ ] **Step 4: Per-run analysis**

For each run directory, write `analysis.md` with: the scenario's headline deltas vs the same-layer baseline (RSS plateau/peak, achieved rate, ns/tick, CPU%), and the pprof attribution — read `pprof-top.txt`, list the top 10 CPU functions and top 5 heap allocation sites, classify each as engine-core / gRPC decode / catalog+price / store / pump / GC, and name the single dominant consumer. Compare service vs engine for the same scenario: what did the wrapper add?

- [ ] **Step 5: REVIEW.md-style verification pass**

A separate reviewer pass (subagent) re-checks every run against raw artifacts: summary.json numbers traceable to stats.jsonl/feed.json, gates consistent with the conservation law, INVALID reasons (if any) genuine. Fix or re-run what fails; do not edit summaries by hand.

- [ ] **Step 6: REPORT.md**

Write `docs/perf/2026-09-06-campaign/REPORT.md`:

```markdown
# Performance Campaign Report — 2026-09-06

## Setup
host (12 cores / 13 GB), Go version, commit, scenario definitions table.

## Engine layer
| scenario | achieved tps | ns/tick | RSS seed→peak (MiB) | heap inuse | GC | CPU% (1 core) | fired | drain ms | valid |

Deltas vs baseline, one paragraph per dimension (rate, symbols, firing).

## Service layer
Same table; plus db size, publish path counts.

## Attribution
Per dimension: what dominates CPU (pprof top functions, classified), what
dominates heap. Engine vs service: the wrapper's cost breakdown.

## Findings & anomalies
Saturation points, ring drops, INVALID runs with reasons, drain times.

## Recommendations
What the numbers suggest (bottlenecks, capacity envelope, follow-ups).
```

- [ ] **Step 7: Commit**

```bash
git add docs/perf/2026-09-06-campaign/
git commit -m "docs(perf): 2026-09-06 campaign results and report"
```

If raw profiles push the commit past ~100 MB, first apply the shrink policy: keep `pprof-top.txt` + `summary.json` + `stats.jsonl`, gitignore `*.pprof` under `docs/perf/`, and note the raw-profiles location in REPORT.md.
