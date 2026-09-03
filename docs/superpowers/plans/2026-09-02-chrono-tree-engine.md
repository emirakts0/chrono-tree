# chrono-tree Engine (Phase A) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the pure-Go, zero-allocation, lock-free-read alert matching engine (library only — no network, no I/O) per `docs/superpowers/specs/2026-09-02-chrono-tree-design.md`.

**Architecture:** Per-symbol `btype.Table` trees (8 per symbol: price_type × direction) published as copy-on-write snapshots via `atomic.Pointer`; a single flusher goroutine applies batched mutations; the hot path (`Match`) reads snapshots lock-free and fires alerts via CAS on per-alert atomic status slots; triggers go to a bounded MPMC ring with drop+count; a reaper sweeps expired alerts and recycles slots on a grace delay.

**Tech Stack:** Go 1.27, `github.com/tidwall/btype`, stdlib only otherwise. Test deps: `go.uber.org/goleak`.

## Global Constraints

- Module path: `github.com/emir/chrono-tree`; Go 1.27; engine code lives in `engine/`.
- Index structure is strictly `github.com/tidwall/btype` (Tables with custom comparator).
- Hot path (`Match` → `fire` → `TryPush` → `trySubmit`) performs **zero heap allocations**; enforced by tests (`testing.AllocsPerRun`) and benchmarks (`b.ReportAllocs`).
- No dynamic strings, no pointer indirection, no interface boxing inside the hot path or the `entry` struct.
- Engine errors are sentinel `errors.New` values. Lifecycle states are `uint32`-backed enums.
- Every task: TDD — failing test first, then minimal implementation, then commit. All tests must pass with `-race`.
- `btype` API assumptions this plan codes against (pinned by the Task 1 conformance test):
  `btype.NewTableOptions(btype.TableOptions[T]{Compare: func(a, b T) int})` builds a table;
  `Ascend(key)`/`Descend(key)` return `iter.Seq[T]` over items `>= key` / `<= key`
  under the FULL comparator (tie-break included): a pivot equal to an existing
  item stops Descend AT that item, so a Descend probe must carry an id that
  sorts after every real id or same-price entries are skipped (pinned by the
  Task 1 conformance test; GTE scans use `entryKeyMax`);
  `Insert(item)` adds (no-op on exact duplicate), `Delete(item)` removes;
  `Copy()` returns an O(1) COW clone safe to mutate independently; `Release()` frees a retired copy.
  If any signature differs when Task 1 runs, **stop and adapt all later code in this plan to the real API** before continuing (the conformance test exists precisely to catch this early).

## File Structure (end state of Phase A)

```
engine/
├── btype_conformance_test.go  # Task 1 — pins the btype.Table behaviors we rely on
├── alert.go                   # Tasks 2,3 — AlertID, enums, entry, comparator, slotArena
├── trigger.go                 # Task 4  — Trigger, MPMC ring buffer
├── symbol.go                  # Task 5  — Interner
├── shard.go                   # Task 6  — snapshot, symbolState, treeIndex
├── engine.go                  # Task 7  — Config, Engine, New/Close, Stats, refs/meta
├── index.go                   # Tasks 8,9 — mutation queue, flusher, control plane
├── match.go                   # Task 10 — Tick, Match, fire
├── reaper.go                  # Task 11 — expiry table, reaper loop, sweep
├── engine_test.go             # Tasks 2-11 unit tests
├── oracle_test.go             # Task 12 — brute-force equivalence + race stress
└── bench_test.go              # Task 13 — benchmarks
```

---

### Task 1: Pin the btype.Table API

**Files:**
- Create: `engine/btype_conformance_test.go`
- Modify: `go.mod`, `go.sum` (add `github.com/tidwall/btype`)

**Interfaces:**
- Produces: verified knowledge that later tasks' code compiles; nothing exported.

- [ ] **Step 1: Add the dependency**

```bash
go get github.com/tidwall/btype@latest
```

- [ ] **Step 2: Write the conformance test**

Create `engine/btype_conformance_test.go`:

```go
package engine

import (
	"math"
	"slices"
	"testing"

	"github.com/tidwall/btype"
)

type confItem struct {
	price float64
	id    uint32
}

func confCompare(a, b confItem) int {
	if a.price < b.price {
		return -1
	}
	if a.price > b.price {
		return 1
	}
	switch {
	case a.id < b.id:
		return -1
	case a.id > b.id:
		return 1
	}
	return 0
}

func newConfTable() *btype.Table[confItem] {
	return btype.NewTableOptions(btype.TableOptions[confItem]{Compare: confCompare})
}

func TestBtypeConformance(t *testing.T) {
	tbl := newConfTable()
	items := []confItem{{10, 1}, {5, 2}, {20, 3}, {10, 0}, {20, 4}}
	for _, it := range items {
		tbl.Insert(it)
	}
	if got := tbl.Len(); got != 5 {
		t.Fatalf("Len = %d, want 5", got)
	}

	// Ascend(key): items >= key, ascending.
	var got []float64
	for it := range tbl.Ascend(confItem{price: 10}) {
		got = append(got, it.price)
	}
	if !slices.Equal(got, []float64{10, 10, 20, 20}) {
		t.Fatalf("Ascend(10) = %v, want [10 10 20 20]", got)
	}

	// Descend(key): items <= key under the FULL compare (price then id),
	// descending. A price-only pivot (id 0) stops at the first id at the
	// boundary price, so same-price items with a larger id are excluded.
	got = got[:0]
	for it := range tbl.Descend(confItem{price: 10}) {
		got = append(got, it.price)
	}
	if !slices.Equal(got, []float64{10, 5}) {
		t.Fatalf("Descend({10,0}) = %v, want [10 5]", got)
	}

	// A max-id probe includes every item at the boundary price: this is the
	// pivot form the engine's GTE scan must use.
	got = got[:0]
	for it := range tbl.Descend(confItem{price: 10, id: math.MaxUint32}) {
		got = append(got, it.price)
	}
	if !slices.Equal(got, []float64{10, 10, 5}) {
		t.Fatalf("Descend({10,max}) = %v, want [10 10 5]", got)
	}

	// Delete removes exactly one item (tie-break by id).
	tbl.Delete(confItem{10, 1})
	if got := tbl.Len(); got != 4 {
		t.Fatalf("Len after delete = %d, want 4", got)
	}

	// Copy is COW: mutating the copy must not affect the original.
	cp := newConfTable()
	*cp = *tbl.Copy()
	cp.Insert(confItem{99, 9})
	if tbl.Len() != 4 || cp.Len() != 5 {
		t.Fatalf("COW isolation broken: orig=%d copy=%d, want 4 and 5", tbl.Len(), cp.Len())
	}
	tbl.Release()
	cp.Release()
}
```

- [ ] **Step 3: Run the test**

```bash
go test ./engine/ -run TestBtypeConformance -v
```

Expected: PASS. If it fails to **compile** (e.g. `NewTableOptions` returns a different type, or `Ascend`/`Descend` signatures differ), run `go doc github.com/tidwall/btype | grep -A2 -iE 'table|ascend|descend'`, adapt this test and note the real signatures in the commit message — later tasks depend on the corrected API. If it fails an **assertion**, the semantics assumption is wrong; fix the plan's Task 10 scan logic accordingly (same commit).

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum engine/btype_conformance_test.go
git commit -m "feat(engine): pin btype.Table API with conformance test"
```

---

### Task 2: Core types — AlertID, enums, entry, comparator

**Files:**
- Create: `engine/alert.go`
- Test: `engine/engine_test.go` (created here)

**Interfaces:**
- Produces: `type AlertID [16]byte`; `type PriceType uint8` with `PriceBid/PriceAsk/PriceMid/PriceLast`; `type Direction uint8` with `DirGTE/DirLTE`; `type Status uint32` with `StatusZero/StatusActive/StatusPaused/StatusTriggered/StatusCancelled/StatusExpired`; `type entry struct` (unexported); `func compareEntry(a, b entry) int`; `func makeFlags(pt PriceType, dir Direction, autoDeactivate bool) uint8`.

- [ ] **Step 1: Write the failing tests**

Create `engine/engine_test.go`:

```go
package engine

import (
	"bytes"
	"testing"
	"unsafe"
)

func TestEntrySize(t *testing.T) {
	// Field order is chosen for minimal padding; hot arena must stay dense.
	if got := unsafe.Sizeof(entry{}); got != 48 {
		t.Fatalf("sizeof(entry) = %d, want 48 (check field order/padding)", got)
	}
}

func TestFlagRoundTrip(t *testing.T) {
	for _, pt := range []PriceType{PriceBid, PriceAsk, PriceMid, PriceLast} {
		for _, dir := range []Direction{DirGTE, DirLTE} {
			for _, auto := range []bool{false, true} {
				f := makeFlags(pt, dir, auto)
				e := entry{flags: f}
				if e.priceType() != pt {
					t.Fatalf("priceType round trip: got %d want %d", e.priceType(), pt)
				}
				if e.direction() != dir {
					t.Fatalf("direction round trip: got %d want %d", e.direction(), dir)
				}
				if e.autoDeactivate() != auto {
					t.Fatalf("autoDeactivate round trip: got %v want %v", e.autoDeactivate(), auto)
				}
			}
		}
	}
}

func TestCompareEntry(t *testing.T) {
	low := entry{price: 1.5}
	high := entry{price: 2.5}
	a := entry{price: 2.5, id: AlertID{1}}
	b := entry{price: 2.5, id: AlertID{2}}
	if compareEntry(low, high) >= 0 || compareEntry(high, low) <= 0 {
		t.Fatal("price ordering broken")
	}
	if compareEntry(a, b) >= 0 || compareEntry(b, a) <= 0 {
		t.Fatal("id tie-break broken")
	}
	if compareEntry(a, a) != 0 {
		t.Fatal("equality broken")
	}
	var zero AlertID
	if bytes.Compare(zero[:], AlertID{1}[:]) >= 0 {
		t.Fatal("zero AlertID must sort first (entryKey relies on it)")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./engine/ -run 'TestEntrySize|TestFlagRoundTrip|TestCompareEntry' -v
```

Expected: FAIL — `entry` and friends undefined.

- [ ] **Step 3: Write minimal implementation**

Create `engine/alert.go`:

```go
// Package engine is chrono-tree's in-memory alert evaluation engine:
// lock-free snapshot reads, COW batched mutations, CAS-gated exactly-once firing.
package engine

import "bytes"

// AlertID is a 128-bit UUID (v7 expected: time-ordered). Always stored by value.
type AlertID [16]byte

// PriceType selects which quote field of a tick an alert watches.
type PriceType uint8

const (
	PriceBid PriceType = iota
	PriceAsk
	PriceMid
	PriceLast
	priceTypeCount = 4
)

// Direction is the comparison direction of an alert's target price.
type Direction uint8

const (
	DirGTE Direction = iota // fires when market >= target
	DirLTE                  // fires when market <= target
)

// Status is the alert lifecycle state, held in an atomic slot so snapshot
// readers can retire an alert without mutating shared trees.
type Status uint32

const (
	StatusZero      Status = iota // slot not in use
	StatusActive
	StatusPaused
	StatusTriggered
	StatusCancelled
	StatusExpired
)

// entry is the hot-path alert record, stored by value inside btype.Table.
// Field order minimizes padding: 48 bytes on 64-bit (guarded by TestEntrySize).
type entry struct {
	price     float64 // target price, primary sort key
	id        AlertID // tie-breaker so equal prices coexist
	validFrom int64   // unix nanos
	expires   int64   // unix nanos; 0 = never
	idx       uint32  // index into Engine slot arena
	flags     uint8
}

const (
	flagActive         uint8 = 1 << 0
	flagPriceTypeMask  uint8 = 1<<1 | 1<<2
	flagPriceTypeShift       = 1
	flagDirection      uint8 = 1 << 3
	flagAutoDeactivate uint8 = 1 << 4
)

func (e *entry) priceType() PriceType {
	return PriceType(e.flags & flagPriceTypeMask >> flagPriceTypeShift)
}

func (e *entry) direction() Direction {
	if e.flags&flagDirection != 0 {
		return DirLTE
	}
	return DirGTE
}

func (e *entry) autoDeactivate() bool {
	return e.flags&flagAutoDeactivate != 0
}

func makeFlags(pt PriceType, dir Direction, autoDeactivate bool) uint8 {
	f := flagActive | uint8(pt)<<flagPriceTypeShift
	if dir == DirLTE {
		f |= flagDirection
	}
	if autoDeactivate {
		f |= flagAutoDeactivate
	}
	return f
}

// compareEntry orders entries by (price, id): price-ordered iteration with
// UUIDs breaking ties. btype pivots are compared with the FULL comparator, so
// boundary probes must be id-aware: AlertID{} sorts before any real UUID
// (correct for Ascend); a Descend probe needs an id that sorts after every
// real UUID or same-price entries are skipped (pinned by Task 1's test).
func compareEntry(a, b entry) int {
	if a.price < b.price {
		return -1
	}
	if a.price > b.price {
		return 1
	}
	return bytes.Compare(a.id[:], b.id[:])
}

// entryKey builds a probe entry for keyed Ascend seeks: id AlertID{} sorts
// first, so Ascend(entryKey(price)) yields every entry with price >= probe.
func entryKey(price float64) entry { return entry{price: price} }

// entryKeyMax builds a probe entry for keyed Descend seeks: the all-ones id
// sorts after every real UUID, so Descend(entryKeyMax(price)) yields every
// entry with price <= probe (including entries exactly at the boundary price).
func entryKeyMax(price float64) entry {
	var maxID AlertID
	for i := range maxID {
		maxID[i] = 0xff
	}
	return entry{price: price, id: maxID}
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./engine/ -run 'TestEntrySize|TestFlagRoundTrip|TestCompareEntry' -v
```

Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add engine/alert.go engine/engine_test.go
git commit -m "feat(engine): core alert types, 48-byte packed entry, comparator"
```

---

### Task 3: Slot arena

**Files:**
- Modify: `engine/alert.go`
- Test: `engine/engine_test.go` (append)

**Interfaces:**
- Produces (unexported): `type slotArena struct`; `func newSlotArena(maxAlerts uint64) *slotArena`; `func (a *slotArena) alloc() uint32` (stores `StatusZero`, returns fresh idx); `func (a *slotArena) get(idx uint32) *atomic.Uint32` (lock-free, hot-path safe); `func (a *slotArena) retire(idx uint32)` (cold path); `func (a *slotArena) recycle(before time.Time)` (moves grace-aged retired slots to the free list).

- [ ] **Step 1: Write the failing test**

Append to `engine/engine_test.go`:

```go
func TestSlotArena(t *testing.T) {
	a := newSlotArena(1000)
	seen := map[uint32]bool{}
	for i := 0; i < 100; i++ {
		idx := a.alloc()
		if seen[idx] {
			t.Fatalf("idx %d handed out twice", idx)
		}
		seen[idx] = true
		if s := Status(a.get(idx).Load()); s != StatusZero {
			t.Fatalf("fresh slot status = %v, want StatusZero", s)
		}
		a.get(idx).Store(uint32(StatusActive))
	}
	// Retire two slots; they must not be reusable until recycle's grace passes.
	a.retire(7)
	a.retire(8)
	for i := 0; i < 10; i++ {
		if idx := a.alloc(); idx == 7 || idx == 8 {
			t.Fatal("retired slot reused before recycle")
		}
	}
	// Recycle with a before-time in the future: retired slots return to use.
	a.recycle(time.Now().Add(time.Hour))
	reused := 0
	for i := 0; i < 2; i++ {
		idx := a.alloc()
		if idx == 7 || idx == 8 {
			reused++
			if s := Status(a.get(idx).Load()); s != StatusZero {
				t.Fatal("recycled slot not reset to StatusZero")
			}
		}
	}
	if reused != 2 {
		t.Fatal("recycle did not return both retired slots")
	}
}
```

Add `"time"` to the test file imports.

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./engine/ -run TestSlotArena -v
```

Expected: FAIL — `newSlotArena` undefined.

- [ ] **Step 3: Implement**

Append to `engine/alert.go`:

```go
// slotArena hands out dense uint32 indices into fixed-size chunks of atomic
// status words. The chunk-pointer slice has fixed length (set at construction
// from MaxAlerts) so hot-path get() never observes a growing slice header;
// chunks themselves are allocated lazily under mu. Publication safety: a
// chunk pointer is stored before any entry referencing the slot is submitted
// to the mutation queue, and the queue/atomic-pointer chain provides the
// happens-before edge to readers.
//
// Freed slots are retired, not immediately reused: a reader holding an old
// snapshot may still see the dead entry and touch its slot, so reuse waits
// for a grace period (2× reaper interval) driven by recycle().
type slotArena struct {
	mu      sync.Mutex
	chunks  []*slotChunk // fixed length, indexed idx>>slotChunkBits
	free    []uint32
	retired []retiredSlot
	next    uint32
}

type slotChunk = [slotChunkSize]atomic.Uint32 // 256 KiB

type retiredSlot struct {
	idx uint32
	at  time.Time
}

const slotChunkBits = 16
const slotChunkSize = 1 << slotChunkBits

func newSlotArena(maxAlerts uint64) *slotArena {
	n := (maxAlerts + slotChunkSize - 1) / slotChunkSize
	// Margin: slots in flight (allocated, removal queued) can briefly exceed
	// the live-alert count; one extra chunk absorbs any lag.
	n++
	return &slotArena{chunks: make([]*slotChunk, n)}
}

func (a *slotArena) alloc() uint32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	var idx uint32
	if n := len(a.free); n > 0 {
		idx = a.free[n-1]
		a.free = a.free[:n-1]
	} else {
		idx = a.next
		a.next++
	}
	ci := idx >> slotChunkBits
	if a.chunks[ci] == nil {
		a.chunks[ci] = new(slotChunk)
	}
	a.chunks[ci][idx&(slotChunkSize-1)].Store(uint32(StatusZero))
	return idx
}

// get returns the atomic status word for idx. Lock-free; hot-path safe.
func (a *slotArena) get(idx uint32) *atomic.Uint32 {
	return &a.chunks[idx>>slotChunkBits][idx&(slotChunkSize-1)]
}

// retire parks a freed slot until recycle moves it past its grace period.
func (a *slotArena) retire(idx uint32) {
	a.mu.Lock()
	a.retired = append(a.retired, retiredSlot{idx: idx, at: time.Now()})
	a.mu.Unlock()
}

// recycle returns retired slots older than before to the free list.
func (a *slotArena) recycle(before time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	i := 0
	for ; i < len(a.retired); i++ {
		if a.retired[i].at.After(before) {
			break
		}
		a.free = append(a.free, a.retired[i].idx)
	}
	a.retired = append(a.retired[:0], a.retired[i:]...)
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./engine/ -run 'TestSlotArena|TestEntrySize|TestFlagRoundTrip|TestCompareEntry' -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/alert.go engine/engine_test.go
git commit -m "feat(engine): slot arena with grace-delayed recycling"
```

---

### Task 4: Trigger ring buffer

**Files:**
- Create: `engine/trigger.go`
- Test: `engine/engine_test.go` (append)

**Interfaces:**
- Produces: `type Trigger struct { ID AlertID; Price float64; TS int64 }`; `type TriggerQueue struct`; `func NewTriggerQueue(capacity int) *TriggerQueue`; `func (q *TriggerQueue) TryPush(t Trigger) bool`; `func (q *TriggerQueue) Pop() (Trigger, bool)`; `func (q *TriggerQueue) PopBatch(dst []Trigger) int`; `func (q *TriggerQueue) Dropped() uint64`.

- [ ] **Step 1: Write the failing test**

Append to `engine/engine_test.go`:

```go
func TestTriggerQueueFIFO(t *testing.T) {
	q := NewTriggerQueue(4)
	for i := 0; i < 4; i++ {
		if !q.TryPush(Trigger{Price: float64(i)}) {
			t.Fatalf("push %d rejected on non-full queue", i)
		}
	}
	if q.TryPush(Trigger{}) {
		t.Fatal("push accepted on full queue")
	}
	if q.Dropped() != 1 {
		t.Fatalf("Dropped = %d, want 1", q.Dropped())
	}
	var got []float64
	for {
		tr, ok := q.Pop()
		if !ok {
			break
		}
		got = append(got, tr.Price)
	}
	if !slices.Equal(got, []float64{0, 1, 2, 3}) {
		t.Fatalf("FIFO broken: %v", got)
	}
	// Queue is empty again; slot reused after full cycle.
	if !q.TryPush(Trigger{Price: 9}) {
		t.Fatal("push rejected after drain")
	}
	tr, ok := q.Pop()
	if !ok || tr.Price != 9 {
		t.Fatal("reuse after drain broken")
	}
}

func TestTriggerQueuePopBatch(t *testing.T) {
	q := NewTriggerQueue(8)
	for i := 0; i < 5; i++ {
		q.TryPush(Trigger{Price: float64(i)})
	}
	dst := make([]Trigger, 3)
	if n := q.PopBatch(dst); n != 3 {
		t.Fatalf("PopBatch = %d, want 3", n)
	}
	if n := q.PopBatch(dst); n != 2 {
		t.Fatalf("PopBatch = %d, want 2", n)
	}
	if n := q.PopBatch(dst); n != 0 {
		t.Fatalf("PopBatch = %d, want 0", n)
	}
}

func TestTriggerQueueConcurrent(t *testing.T) {
	q := NewTriggerQueue(1024)
	const producers, each = 8, 10_000
	var wg sync.WaitGroup
	producersDone := make(chan struct{})
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				// Unique value per push: duplicate delivery is detectable.
				q.TryPush(Trigger{Price: float64(p*each + i)})
			}
		}(p)
	}
	delivered := make(map[float64]bool)
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			tr, ok := q.Pop()
			if ok {
				if delivered[tr.Price] {
					t.Errorf("trigger %v delivered twice", tr.Price)
					return
				}
				delivered[tr.Price] = true
				continue
			}
			select {
			case <-producersDone: // drained after all producers finished
				return
			default:
				runtime.Gosched()
			}
		}
	}()
	wg.Wait()
	close(producersDone)
	<-consumerDone
	// Drop+count contract: every push was either delivered exactly once or
	// counted in Dropped. With the consumer draining until empty after the
	// producers finish, every successful TryPush is eventually popped.
	total := producers * each
	if got := len(delivered) + int(q.Dropped()); got != total {
		t.Fatalf("delivered %d + dropped %d = %d, want %d",
			len(delivered), q.Dropped(), got, total)
	}
}
```

Add `"slices"`, `"sync"` to the test file imports.

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./engine/ -run TestTriggerQueue -v
```

Expected: FAIL — `NewTriggerQueue` undefined.

- [ ] **Step 3: Implement**

Create `engine/trigger.go`:

```go
package engine

import "sync/atomic"

// Trigger is the dispatch payload emitted when an alert fires: 32 bytes.
type Trigger struct {
	ID    AlertID
	Price float64
	TS    int64
}

type triggerCell struct {
	seq atomic.Uint64
	val Trigger
}

// TriggerQueue is a bounded lock-free MPMC ring buffer (Vyukov design).
// TryPush never blocks: on a full queue it returns false and bumps Dropped.
type TriggerQueue struct {
	buf        []triggerCell
	mask       uint64
	enqueuePos atomic.Uint64
	dequeuePos atomic.Uint64
	dropped    atomic.Uint64
}

// NewTriggerQueue builds a queue; capacity is rounded up to a power of two.
func NewTriggerQueue(capacity int) *TriggerQueue {
	if capacity < 2 {
		capacity = 2
	}
	p := 1
	for p < capacity {
		p <<= 1
	}
	q := &TriggerQueue{buf: make([]triggerCell, p), mask: uint64(p - 1)}
	for i := range q.buf {
		q.buf[i].seq.Store(uint64(i))
	}
	return q
}

func (q *TriggerQueue) TryPush(t Trigger) bool {
	for {
		pos := q.enqueuePos.Load()
		c := &q.buf[pos&q.mask]
		seq := c.seq.Load()
		switch dif := int64(seq) - int64(pos); {
		case dif == 0:
			if q.enqueuePos.CompareAndSwap(pos, pos+1) {
				c.val = t
				c.seq.Store(pos + 1)
				return true
			}
		case dif < 0: // full
			q.dropped.Add(1)
			return false
		default: // another producer claimed the slot; retry
		}
	}
}

func (q *TriggerQueue) Pop() (Trigger, bool) {
	for {
		pos := q.dequeuePos.Load()
		c := &q.buf[pos&q.mask]
		seq := c.seq.Load()
		switch dif := int64(seq) - int64(pos+1); {
		case dif == 0:
			if q.dequeuePos.CompareAndSwap(pos, pos+1) {
				v := c.val
				c.seq.Store(pos + q.mask + 1)
				return v, true
			}
		case dif < 0: // empty
			return Trigger{}, false
		default: // another consumer claimed the slot; retry
		}
	}
}

// PopBatch drains up to len(dst) triggers into dst, returning the count.
func (q *TriggerQueue) PopBatch(dst []Trigger) int {
	n := 0
	for n < len(dst) {
		t, ok := q.Pop()
		if !ok {
			break
		}
		dst[n] = t
		n++
	}
	return n
}

// Dropped reports triggers rejected because the queue was full.
func (q *TriggerQueue) Dropped() uint64 { return q.dropped.Load() }
```

- [ ] **Step 4: Run tests to verify they pass (with race detector)**

```bash
go test ./engine/ -race -run TestTriggerQueue -v
```

Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add engine/trigger.go engine/engine_test.go
git commit -m "feat(engine): lock-free MPMC trigger ring with drop+count"
```

---

### Task 5: Symbol interner

**Files:**
- Create: `engine/symbol.go`
- Test: `engine/engine_test.go` (append)

**Interfaces:**
- Produces: `type SymbolID uint32`; `func NewInterner() *Interner`; `func (in *Interner) Get(s string) (SymbolID, bool)`; `func (in *Interner) Intern(s string) SymbolID`; `func (in *Interner) Name(id SymbolID) string`.

- [ ] **Step 1: Write the failing test**

Append to `engine/engine_test.go`:

```go
func TestInterner(t *testing.T) {
	in := NewInterner()
	if _, ok := in.Get("USDTRY"); ok {
		t.Fatal("Get on empty interner returned true")
	}
	a := in.Intern("USDTRY")
	b := in.Intern("EURTRY")
	if a == b {
		t.Fatal("distinct symbols got same id")
	}
	if again := in.Intern("USDTRY"); again != a {
		t.Fatal("re-intern returned different id")
	}
	if got, ok := in.Get("USDTRY"); !ok || got != a {
		t.Fatal("Get after intern failed")
	}
	if in.Name(a) != "USDTRY" || in.Name(b) != "EURTRY" {
		t.Fatal("Name round trip broken")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./engine/ -run TestInterner -v
```

Expected: FAIL — `NewInterner` undefined.

- [ ] **Step 3: Implement**

Create `engine/symbol.go`:

```go
package engine

import "sync"

// SymbolID is a dense identifier produced by the Interner. SymbolIDs index
// the Engine's fixed symbolState array, so they must stay small and dense.
type SymbolID uint32

// Interner maps symbol strings to dense SymbolIDs. Interning is rare (new
// symbols only); Get is hot-path and takes a single RLock map probe.
type Interner struct {
	mu    sync.RWMutex
	ids   map[string]SymbolID
	names []string
}

func NewInterner() *Interner {
	return &Interner{ids: make(map[string]SymbolID)}
}

// Get returns the id of an already-interned symbol.
func (in *Interner) Get(s string) (SymbolID, bool) {
	in.mu.RLock()
	defer in.mu.RUnlock()
	id, ok := in.ids[s]
	return id, ok
}

// Intern returns the id of s, assigning a new one on first sight.
func (in *Interner) Intern(s string) SymbolID {
	in.mu.RLock()
	id, ok := in.ids[s]
	in.mu.RUnlock()
	if ok {
		return id
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	if id, ok := in.ids[s]; ok {
		return id
	}
	id = SymbolID(len(in.names))
	in.ids[s] = id
	in.names = append(in.names, s)
	return id
}

// Name returns the symbol string for id.
func (in *Interner) Name(id SymbolID) string {
	in.mu.RLock()
	defer in.mu.RUnlock()
	return in.names[id]
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./engine/ -run TestInterner -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/symbol.go engine/engine_test.go
git commit -m "feat(engine): symbol interner"
```

---

### Task 6: Snapshot & symbol state

**Files:**
- Create: `engine/shard.go`
- Test: `engine/engine_test.go` (append)

**Interfaces:**
- Consumes: `entry`, `compareEntry` (Task 2).
- Produces (unexported): `func treeIndex(pt PriceType, dir Direction) int`; `type snapshot struct` with `trees [8]btype.Table[entry]`; `func newSnapshot() *snapshot`; `func (s *snapshot) copy() *snapshot` (COW clone of all 8 trees); `func (s *snapshot) release()`; `type symbolState struct` with `snap atomic.Pointer[snapshot]`.

- [ ] **Step 1: Write the failing tests**

Append to `engine/engine_test.go`:

```go
func TestTreeIndexDistinct(t *testing.T) {
	seen := map[int]bool{}
	for pt := PriceType(0); pt < priceTypeCount; pt++ {
		for _, dir := range []Direction{DirGTE, DirLTE} {
			i := treeIndex(pt, dir)
			if i < 0 || i >= 8 {
				t.Fatalf("treeIndex(%d,%d)=%d out of range", pt, dir, i)
			}
			if seen[i] {
				t.Fatalf("treeIndex(%d,%d)=%d collides", pt, dir, i)
			}
			seen[i] = true
		}
	}
	if len(seen) != 8 {
		t.Fatalf("expected 8 distinct trees, got %d", len(seen))
	}
}

func TestSnapshotCopyIsolation(t *testing.T) {
	s := newSnapshot()
	ti := treeIndex(PriceBid, DirGTE)
	s.trees[ti].Insert(entry{price: 42.5, id: AlertID{1}, idx: 1, flags: makeFlags(PriceBid, DirGTE, true)})
	cp := s.copy()
	cp.trees[ti].Insert(entry{price: 10, id: AlertID{2}, idx: 2, flags: makeFlags(PriceBid, DirGTE, true)})
	if s.trees[ti].Len() != 1 || cp.trees[ti].Len() != 2 {
		t.Fatalf("COW isolation broken: orig=%d copy=%d, want 1 and 2", s.trees[ti].Len(), cp.trees[ti].Len())
	}
	s.release()
	cp.release()
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./engine/ -run 'TestTreeIndex|TestSnapshotCopy' -v
```

Expected: FAIL — `treeIndex`/`newSnapshot` undefined.

- [ ] **Step 3: Implement**

Create `engine/shard.go`:

```go
package engine

import (
	"sync/atomic"

	"github.com/tidwall/btype"
)

// treeIndex addresses the per-(priceType, direction) trees of a snapshot.
// Partitioning by all three dimensions upfront means a scan touches exactly
// one tree and every entry it reaches is a candidate by construction.
func treeIndex(pt PriceType, dir Direction) int {
	return int(pt)<<1 | int(dir)
}

// snapshot is an immutable view of one symbol's alert trees, published via
// atomic.Pointer and retired with release() once the last reader is done.
type snapshot struct {
	trees [8]btype.Table[entry]
}

func newSnapshot() *snapshot {
	s := &snapshot{}
	for i := range s.trees {
		s.trees[i] = *btype.NewTableOptions(btype.TableOptions[entry]{Compare: compareEntry})
	}
	return s
}

// copy returns a private COW clone, safe to mutate before republishing.
func (s *snapshot) copy() *snapshot {
	n := &snapshot{}
	for i := range s.trees {
		n.trees[i] = *s.trees[i].Copy()
	}
	return n
}

func (s *snapshot) release() {
	for i := range s.trees {
		s.trees[i].Release()
	}
}

// symbolState publishes snapshots for one symbol.
type symbolState struct {
	snap atomic.Pointer[snapshot]
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./engine/ -run 'TestTreeIndex|TestSnapshotCopy' -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/shard.go engine/engine_test.go
git commit -m "feat(engine): per-symbol 8-tree snapshots with COW copy/release"
```

---

### Task 7: Engine skeleton — Config, New/Close, cold records

**Files:**
- Create: `engine/engine.go`
- Modify: `go.mod`, `go.sum` (add `go.uber.org/goleak`)
- Test: `engine/engine_test.go` (append)

**Interfaces:**
- Consumes: everything from Tasks 2–6.
- Produces: `type Config struct`; `func DefaultConfig() Config`; `type AlertSpec struct`; `type AlertMeta struct`; `type Stats struct`; `type Engine struct`; `func New(cfg Config) *Engine`; `func (e *Engine) Close()` (idempotent); `func (e *Engine) Triggers() *TriggerQueue`; `func (e *Engine) Stats() Stats`. Sentinels: `ErrClosed`, `ErrNotFound`, `ErrInvalidStatus`, `ErrInvalidTransition`, `ErrSymbolLimit`, `ErrAlertLimit`. Unexported: `type alertRef struct { sid SymbolID; e entry }`, engine fields `mutQ chan mutation`, `expQ chan expEntry`, `refs`, `meta`, `live`, `done chan struct{}`, `flushWG`, `reapWG`, `closed atomic.Bool` (types `mutation` and `expEntry` arrive in Tasks 8/11 — declare them in this task so the struct compiles).

- [ ] **Step 1: Write the failing test**

Append to `engine/engine_test.go`:

```go
func TestEngineNewClose(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	if e.Triggers() == nil {
		t.Fatal("nil trigger queue")
	}
	if s := e.Stats(); s.Live != 0 || s.DroppedTriggers != 0 {
		t.Fatalf("fresh engine stats = %+v, want zeros", s)
	}
	e.Close()
	e.Close() // must be idempotent
}
```

- [ ] **Step 2: Add goleak and run the test**

```bash
go get go.uber.org/goleak@latest
go test ./engine/ -run TestEngineNewClose -v
```

Expected: FAIL — `New`/`DefaultConfig` undefined.

- [ ] **Step 3: Implement**

Create `engine/engine.go`:

```go
package engine

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/btype"
)

// Sentinel errors returned by the control plane.
var (
	ErrClosed            = errors.New("chrono-tree: engine closed")
	ErrNotFound          = errors.New("chrono-tree: alert not found")
	ErrInvalidStatus     = errors.New("chrono-tree: invalid status value")
	ErrInvalidTransition = errors.New("chrono-tree: invalid status transition")
	ErrSymbolLimit       = errors.New("chrono-tree: symbol limit exceeded")
	ErrAlertLimit        = errors.New("chrono-tree: max alerts exceeded")
)

// Config bounds all preallocated structures. See DefaultConfig.
type Config struct {
	MaxSymbols         uint32        // fixed symbolState array size
	MaxAlerts          uint64        // live alert cap
	MutationQueueDepth int           // bounded mutation queue
	FlushBatch         int           // max ops applied per flush cycle
	RingSize           int           // trigger ring capacity (rounded to pow2)
	ReaperInterval     time.Duration // expiry sweep + slot recycle period
}

func DefaultConfig() Config {
	return Config{
		MaxSymbols:         1 << 16,
		MaxAlerts:          10_000_000,
		MutationQueueDepth: 4096,
		FlushBatch:         256,
		RingSize:           1 << 16,
		ReaperInterval:     time.Second,
	}
}

// AlertSpec is the validated control-plane input for Upsert.
type AlertSpec struct {
	ID             AlertID
	Symbol         string
	PriceType      PriceType
	Direction      Direction
	TargetPrice    float64
	ValidFrom      int64 // unix nanos
	Expires        int64 // unix nanos; 0 = never
	AutoDeactivate bool
	Meta           AlertMeta // cold data, stored verbatim
}

func (a *AlertSpec) validate() error {
	if a.ID == (AlertID{}) {
		return errors.New("chrono-tree: alert id is zero")
	}
	if a.Symbol == "" {
		return errors.New("chrono-tree: symbol is empty")
	}
	if a.PriceType >= priceTypeCount {
		return errors.New("chrono-tree: invalid price type")
	}
	if a.Direction > DirLTE {
		return errors.New("chrono-tree: invalid direction")
	}
	if a.Expires != 0 && a.Expires <= a.ValidFrom {
		return errors.New("chrono-tree: expires at or before valid-from")
	}
	return nil
}

// AlertMeta is the cold record: everything the notification pipeline needs
// after a trigger fires. Never touched by Match.
type AlertMeta struct {
	ID          AlertID
	Symbol      string
	UserID      string
	Segment     string
	Channels    []string
	Notes       string
	CreatedAt   int64 // unix nanos
	PriceType   PriceType
	Direction   Direction
	TargetPrice float64
}

// Stats is a point-in-time engine snapshot for observability.
type Stats struct {
	Live            uint64
	DroppedTriggers uint64
}

// alertRef locates an alert's index structures for control-plane ops.
type alertRef struct {
	sid SymbolID
	e   entry
}

// mutation is a queued index change, applied by the flusher (Task 8).
type mutation struct {
	op   mutOp
	sid  SymbolID
	e    entry
	done chan struct{} // op == mutSync: closed once applied
}

type mutOp uint8

const (
	mutInsert mutOp = iota
	mutRemove
	mutSync
)

// expEntry registers an alert with the reaper's expiry table (Task 11).
type expEntry struct {
	expires int64
	sid     SymbolID
	e       entry
}

// Engine is the alert evaluation engine. Zero network, zero I/O.
type Engine struct {
	cfg    Config
	syms   *Interner
	states []symbolState // fixed len MaxSymbols, indexed by SymbolID
	slots  *slotArena

	mutQ     chan mutation
	expQ     chan expEntry
	triggers *TriggerQueue
	expiry   btype.Table[expEntry] // owned by the reaper only

	mu   sync.Mutex // guards refs, meta, live
	refs map[AlertID]*alertRef
	meta map[AlertID]*AlertMeta
	live uint64

	done    chan struct{}
	flushWG sync.WaitGroup
	reapWG  sync.WaitGroup
	closed  atomic.Bool
}

func New(cfg Config) *Engine {
	e := &Engine{
		cfg:      cfg,
		syms:     NewInterner(),
		states:   make([]symbolState, cfg.MaxSymbols),
		slots:    newSlotArena(cfg.MaxAlerts),
		mutQ:     make(chan mutation, cfg.MutationQueueDepth),
		expQ:     make(chan expEntry, cfg.MutationQueueDepth),
		triggers: NewTriggerQueue(cfg.RingSize),
		refs:     make(map[AlertID]*alertRef),
		meta:     make(map[AlertID]*AlertMeta),
		done:     make(chan struct{}),
	}
	// Flusher (Task 8) and reaper (Task 11) goroutines are started here as
	// their tasks land; Close waits on both WaitGroups.
	return e
}

// Close stops the flusher and reaper and releases all snapshots. Idempotent.
func (e *Engine) Close() {
	if !e.closed.CompareAndSwap(false, true) {
		return
	}
	close(e.done)
	e.flushWG.Wait()
	e.reapWG.Wait()
	for i := range e.states {
		if s := e.states[i].snap.Load(); s != nil {
			s.release()
			e.states[i].snap.Store(nil)
		}
	}
}

// Triggers exposes the trigger ring for downstream consumption.
func (e *Engine) Triggers() *TriggerQueue { return e.triggers }

// Stats reports live alerts and dropped triggers.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	live := e.live
	e.mu.Unlock()
	return Stats{Live: live, DroppedTriggers: e.triggers.Dropped()}
}
```

- [ ] **Step 4: Run all tests to verify they pass**

```bash
go test ./engine/ -race -v
```

Expected: PASS (all tests so far).

- [ ] **Step 5: Commit**

```bash
git add engine/engine.go engine/engine_test.go go.mod go.sum
git commit -m "feat(engine): engine skeleton, config, cold records, sentinel errors"
```

---

### Task 8: Mutation queue + flusher (COW publication)

**Files:**
- Create: `engine/index.go`
- Modify: `engine/engine.go` (start flusher in `New`)
- Test: `engine/engine_test.go` (append)

**Interfaces:**
- Consumes: `mutation`/`mutOp` (Task 7), `snapshot.copy/release` (Task 6), `slotArena.retire` (Task 3).
- Produces (unexported): `func (e *Engine) submit(m mutation) error` (blocking, `ErrClosed` on shutdown); `func (e *Engine) trySubmit(m mutation) bool` (non-blocking); `func (e *Engine) Sync()` (blocks until all previously submitted ops are applied); `func (e *Engine) stateFor(sym string) (SymbolID, error)`; `func (e *Engine) runFlusher()` (launched by `New`).

- [ ] **Step 1: Write the failing test**

Append to `engine/engine_test.go`:

```go
func TestFlusherAppliesMutations(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	sid, err := e.stateFor("USDTRY")
	if err != nil {
		t.Fatal(err)
	}
	if e.states[sid].snap.Load() == nil {
		t.Fatal("stateFor did not publish an initial snapshot")
	}
	ent := entry{price: 42.5, id: AlertID{1}, idx: e.slots.alloc(),
		validFrom: 1, flags: makeFlags(PriceBid, DirGTE, true)}
	e.slots.get(ent.idx).Store(uint32(StatusActive))

	e.submit(mutation{op: mutInsert, sid: sid, e: ent})
	e.Sync()
	ti := treeIndex(PriceBid, DirGTE)
	if got := e.states[sid].snap.Load().trees[ti].Len(); got != 1 {
		t.Fatalf("after insert Len=%d, want 1", got)
	}

	e.submit(mutation{op: mutRemove, sid: sid, e: ent})
	e.Sync()
	if got := e.states[sid].snap.Load().trees[ti].Len(); got != 0 {
		t.Fatalf("after remove Len=%d, want 0", got)
	}
	// mutRemove cleanup: refs/meta deleted, live decremented, slot retired.
	if _, ok := e.refs[ent.id]; ok {
		t.Fatal("refs entry survived removal")
	}
}

func TestSyncBarrier(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	sid, _ := e.stateFor("EURTRY")
	for i := 0; i < 100; i++ {
		idx := e.slots.alloc()
		e.slots.get(idx).Store(uint32(StatusActive))
		e.mu.Lock()
		e.refs[AlertID{byte(i + 1)}] = &alertRef{sid: sid, e: entry{
			price: float64(i), id: AlertID{byte(i + 1)}, idx: idx,
			flags: makeFlags(PriceLast, DirLTE, false)}}
		e.live++
		e.mu.Unlock()
		e.submit(mutation{op: mutInsert, sid: sid,
			e: entry{price: float64(i), id: AlertID{byte(i + 1)}, idx: idx,
				validFrom: 1, flags: makeFlags(PriceLast, DirLTE, false)}})
	}
	e.Sync()
	if got := e.states[sid].snap.Load().trees[treeIndex(PriceLast, DirLTE)].Len(); got != 100 {
		t.Fatalf("Len=%d, want 100", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./engine/ -run 'TestFlusher|TestSyncBarrier' -v
```

Expected: FAIL — `submit`/`Sync`/`stateFor` undefined.

- [ ] **Step 3: Implement**

Create `engine/index.go`:

```go
package engine

// stateFor interns sym and guarantees an initial published snapshot.
// The fast path re-checks the bound: a symbol previously interned-then-
// rejected (past MaxSymbols) stays in the interner forever, and returning
// its out-of-range sid would panic e.states[sid] in applyBatch.
func (e *Engine) stateFor(sym string) (SymbolID, error) {
	if sid, ok := e.syms.Get(sym); ok {
		if uint64(sid) < uint64(len(e.states)) {
			return sid, nil
		}
		return 0, ErrSymbolLimit
	}
	sid := e.syms.Intern(sym)
	if uint64(sid) >= uint64(len(e.states)) {
		return 0, ErrSymbolLimit
	}
	st := &e.states[sid]
	if st.snap.Load() == nil {
		st.snap.CompareAndSwap(nil, newSnapshot())
	}
	return sid, nil
}

// submit blocks until m is queued (control plane only; the hot path uses
// trySubmit). The pre-check prefers the done signal so a racing submit is
// less likely to land in mutQ after the flusher's final drain; callers must
// still stop submitting before Close (residual window accepted by design).
func (e *Engine) submit(m mutation) error {
	select {
	case <-e.done:
		return ErrClosed
	default:
	}
	select {
	case e.mutQ <- m:
		return nil
	case <-e.done:
		return ErrClosed
	}
}

// trySubmit is the hot path's best-effort, never-blocking enqueue. A miss
// only defers cleanup (stale entries are skipped via their status slot and
// swept later); it can never cause a missed or duplicate trigger.
func (e *Engine) trySubmit(m mutation) bool {
	select {
	case e.mutQ <- m:
		return true
	default:
		return false
	}
}

// Sync blocks until every mutation submitted before Sync has been applied.
// Returns immediately if the engine is closed (the barrier cannot complete).
func (e *Engine) Sync() {
	done := make(chan struct{})
	if err := e.submit(mutation{op: mutSync, done: done}); err != nil {
		return
	}
	<-done
}

// runFlusher is the single writer: it drains the mutation queue in batches,
// applies each batch to COW copies, and republishes per-symbol snapshots.
func (e *Engine) runFlusher() {
	defer e.flushWG.Done()
	batch := make([]mutation, 0, e.cfg.FlushBatch)
	for {
		select {
		case m := <-e.mutQ:
			batch = append(batch, m)
		drain:
			for len(batch) < cap(batch) {
				select {
				case m := <-e.mutQ:
					batch = append(batch, m)
				default:
					break drain
				}
			}
			e.applyBatch(batch)
			batch = batch[:0]
		case <-e.done:
			for {
				select {
				case m := <-e.mutQ:
					batch = append(batch, m)
					if len(batch) == cap(batch) {
						e.applyBatch(batch)
						batch = batch[:0]
					}
				default:
					e.applyBatch(batch)
					return
				}
			}
		}
	}
}

// applyBatch applies ops grouped by symbol so each symbol is copied once,
// then publishes all snapshots atomically before releasing the old ones.
func (e *Engine) applyBatch(batch []mutation) {
	type pending struct {
		st   *symbolState
		old  *snapshot
		next *snapshot
	}
	bySym := make(map[SymbolID]*pending) // cold path; map is fine
	var syncs []chan struct{}
	for _, m := range batch {
		switch m.op {
		case mutSync:
			syncs = append(syncs, m.done)
			continue
		}
		p := bySym[m.sid]
		if p == nil {
			st := &e.states[m.sid]
			old := st.snap.Load()
			p = &pending{st: st, old: old, next: old.copy()}
			bySym[m.sid] = p
		}
		switch m.op {
		case mutInsert:
			p.next.trees[treeIndex(m.e.priceType(), m.e.direction())].Insert(m.e)
		case mutRemove:
			p.next.trees[treeIndex(m.e.priceType(), m.e.direction())].Delete(m.e)
			// Slot bookkeeping: retire the slot, and clean refs/meta/live only
			// if the live ref still matches this exact entry (a same-ID upsert
			// may have already replaced it).
			e.slots.retire(m.e.idx)
			e.mu.Lock()
			if r, ok := e.refs[m.e.id]; ok && r.e.idx == m.e.idx {
				delete(e.refs, m.e.id)
				delete(e.meta, m.e.id)
				e.live--
			}
			e.mu.Unlock()
		}
	}
	for _, p := range bySym {
		p.st.snap.Store(p.next)
		p.old.release()
	}
	for _, d := range syncs {
		close(d)
	}
}
```

In `engine.go`, `New`, replace the trailing comment with the flusher start:

```go
	e.flushWG.Add(1)
	go e.runFlusher()
	return e
}
```

(Delete the placeholder comment lines about Tasks 8/11; the reaper launch is
added by Task 11 in the same spot.)

- [ ] **Step 4: Run all tests to verify they pass**

```bash
go test ./engine/ -race -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/index.go engine/engine.go engine/engine_test.go
git commit -m "feat(engine): COW flusher with batched mutation application"
```

---

### Task 9: Control plane — Upsert, Cancel, SetStatus

**Files:**
- Modify: `engine/index.go` (append)
- Test: `engine/engine_test.go` (append)

**Interfaces:**
- Consumes: `submit`, `stateFor` (Task 8), `AlertSpec.validate` (Task 7), `slotArena` (Task 3).
- Produces: `func (e *Engine) Upsert(a AlertSpec) error`; `func (e *Engine) Cancel(id AlertID) error`; `func (e *Engine) SetStatus(id AlertID, target Status) error`. Unexported: `func (e *Engine) removeIfLive(id AlertID, want Status) (mutation, error)`; `func (e *Engine) submitExpiry(x expEntry) error`.

- [ ] **Step 1: Write the failing tests**

Append to `engine/engine_test.go`:

```go
func testSpec(id byte, sym string, pt PriceType, dir Direction, price float64) AlertSpec {
	return AlertSpec{
		ID: AlertID{id}, Symbol: sym, PriceType: pt, Direction: dir,
		TargetPrice: price, ValidFrom: 1, AutoDeactivate: true,
		Meta: AlertMeta{ID: AlertID{id}, Symbol: sym},
	}
}

func TestUpsertInsertAndReplace(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 42.5)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	sid, _ := e.syms.Get("USDTRY")
	if got := e.states[sid].snap.Load().trees[treeIndex(PriceBid, DirGTE)].Len(); got != 1 {
		t.Fatalf("Len=%d, want 1", got)
	}
	if s := e.Stats(); s.Live != 1 {
		t.Fatalf("Live=%d, want 1", s.Live)
	}
	// Replace same ID with a different price: still exactly one live alert.
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 43)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if s := e.Stats(); s.Live != 1 {
		t.Fatalf("after replace Live=%d, want 1", s.Live)
	}
	ti := treeIndex(PriceBid, DirGTE)
	n := 0
	for en := range e.states[sid].snap.Load().trees[ti].All() {
		n++
		if en.price != 43 {
			t.Fatalf("stale entry price=%v, want 43", en.price)
		}
	}
	if n != 1 {
		t.Fatalf("entries=%d, want 1", n)
	}
}

func TestUpsertValidation(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	cases := []AlertSpec{
		{Symbol: "X", PriceType: PriceBid, Direction: DirGTE},                       // zero ID
		{ID: AlertID{1}, PriceType: PriceBid, Direction: DirGTE},                    // empty symbol
		{ID: AlertID{1}, Symbol: "X", PriceType: priceTypeCount, Direction: DirGTE}, // bad price type
		{ID: AlertID{1}, Symbol: "X", PriceType: PriceBid, Direction: 99},           // bad direction
		{ID: AlertID{1}, Symbol: "X", PriceType: PriceBid, Direction: DirGTE,
			ValidFrom: 100, Expires: 50}, // expires before valid
	}
	for i, c := range cases {
		if err := e.Upsert(c); err == nil {
			t.Fatalf("case %d: expected validation error", i)
		}
	}
}

func TestUpsertLimits(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	// MaxAlerts covers the alert-limit probe plus the five live alerts needed
	// for the symbol-limit section (USDTRY + S0..S2).
	cfg.MaxAlerts = 5
	cfg.MaxSymbols = 4
	e := New(cfg)
	defer e.Close()
	for i := byte(0); i < 2; i++ {
		if err := e.Upsert(testSpec(i+1, "USDTRY", PriceBid, DirGTE, 42.5)); err != nil {
			t.Fatal(err)
		}
	}
	// MaxSymbols=4 with USDTRY already interned: S0..S2 fill the remaining
	// slots; S3 must be rejected.
	for i := byte(0); i < 3; i++ {
		if err := e.Upsert(testSpec(10+i, fmt.Sprintf("S%d", i), PriceBid, DirGTE, 1)); err != nil {
			t.Fatalf("symbol %d: err=%v, want nil", i, err)
		}
	}
	// live == MaxAlerts now: a fresh insert must be rejected.
	if err := e.Upsert(testSpec(3, "USDTRY", PriceBid, DirGTE, 42.5)); err != ErrAlertLimit {
		t.Fatalf("err=%v, want ErrAlertLimit", err)
	}
	if err := e.Upsert(testSpec(20, "S3", PriceBid, DirGTE, 1)); err != ErrSymbolLimit {
		t.Fatalf("err=%v, want ErrSymbolLimit", err)
	}
}

func TestCancelAndSetStatus(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 42.5))
	e.Sync()
	if err := e.SetStatus(AlertID{1}, StatusPaused); err != nil {
		t.Fatal(err)
	}
	if err := e.SetStatus(AlertID{1}, StatusActive); err != nil {
		t.Fatal(err)
	}
	if err := e.SetStatus(AlertID{1}, StatusActive); err != nil {
		t.Fatal("idempotent same-state SetStatus should succeed:", err)
	}
	if err := e.SetStatus(AlertID{1}, Status(99)); err != ErrInvalidStatus {
		t.Fatalf("err=%v, want ErrInvalidStatus", err)
	}
	if err := e.Cancel(AlertID{1}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if s := e.Stats(); s.Live != 0 {
		t.Fatalf("Live=%d after cancel, want 0", s.Live)
	}
	if err := e.Cancel(AlertID{1}); err != ErrNotFound {
		t.Fatalf("err=%v, want ErrNotFound", err)
	}
	if err := e.SetStatus(AlertID{2}, StatusPaused); err != ErrNotFound {
		t.Fatalf("err=%v, want ErrNotFound", err)
	}
}
```

Add `"fmt"` to the test file imports.

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./engine/ -run 'TestUpsert|TestCancelAndSetStatus' -v
```

Expected: FAIL — `Upsert`/`Cancel`/`SetStatus` undefined.

- [ ] **Step 3: Implement**

Append to `engine/index.go`:

```go
// Upsert inserts a new alert or atomically replaces the one with the same ID.
// Blocking on the mutation queue; returns ErrClosed, ErrAlertLimit,
// ErrSymbolLimit, or a validation error.
func (e *Engine) Upsert(a AlertSpec) error {
	if err := a.validate(); err != nil {
		return err
	}
	if e.closed.Load() {
		return ErrClosed
	}
	sid, err := e.stateFor(a.Symbol)
	if err != nil {
		return err
	}
	var replace mutation
	hasReplace := false
	e.mu.Lock()
	if ref, ok := e.refs[a.ID]; ok {
		old := e.slots.get(ref.e.idx)
		old.CompareAndSwap(uint32(StatusActive), uint32(StatusCancelled))
		old.CompareAndSwap(uint32(StatusPaused), uint32(StatusCancelled))
		replace = mutation{op: mutRemove, sid: ref.sid, e: ref.e}
		hasReplace = true
		delete(e.refs, a.ID)
		delete(e.meta, a.ID)
		e.live--
	} else if e.live >= e.cfg.MaxAlerts {
		e.mu.Unlock()
		return ErrAlertLimit
	}
	e.mu.Unlock()

	idx := e.slots.alloc()
	e.slots.get(idx).Store(uint32(StatusActive))
	ent := entry{
		price:     a.TargetPrice,
		id:        a.ID,
		validFrom: a.ValidFrom,
		expires:   a.Expires,
		idx:       idx,
		flags:     makeFlags(a.PriceType, a.Direction, a.AutoDeactivate),
	}
	meta := a.Meta
	e.mu.Lock()
	e.refs[a.ID] = &alertRef{sid: sid, e: ent}
	e.meta[a.ID] = &meta
	e.live++
	e.mu.Unlock()

	// Submission order is queue order: removal of the old entry lands before
	// the new insert. The slot is Active before publication, but the entry is
	// unreachable to readers until the insert flushes.
	if hasReplace {
		if err := e.submit(replace); err != nil {
			return err
		}
	}
	if err := e.submit(mutation{op: mutInsert, sid: sid, e: ent}); err != nil {
		return err
	}
	if a.Expires != 0 {
		return e.submitExpiry(expEntry{expires: a.Expires, sid: sid, e: ent})
	}
	return nil
}

// submitExpiry registers an alert with the reaper's expiry table.
func (e *Engine) submitExpiry(x expEntry) error {
	select {
	case e.expQ <- x:
		return nil
	case <-e.done:
		return ErrClosed
	}
}

// removeIfLive CASes the alert's slot from ACTIVE/PAUSED to want and returns
// its removal mutation. refs/meta/live cleanup happens in applyBatch.
func (e *Engine) removeIfLive(id AlertID, want Status) (mutation, error) {
	e.mu.Lock()
	ref, ok := e.refs[id]
	e.mu.Unlock()
	if !ok {
		return mutation{}, ErrNotFound
	}
	s := e.slots.get(ref.e.idx)
	for {
		cur := Status(s.Load())
		switch cur {
		case StatusActive, StatusPaused:
			if s.CompareAndSwap(uint32(cur), uint32(want)) {
				return mutation{op: mutRemove, sid: ref.sid, e: ref.e}, nil
			}
		default: // TRIGGERED (removal already queued), CANCELLED, EXPIRED
			return mutation{}, ErrInvalidTransition
		}
	}
}

// Cancel permanently retires an alert.
func (e *Engine) Cancel(id AlertID) error {
	m, err := e.removeIfLive(id, StatusCancelled)
	if err != nil {
		return err
	}
	return e.submit(m)
}

// SetStatus transitions an alert between ACTIVE and PAUSED, or cancels it.
// Same-state transitions are idempotent; terminal states reject everything.
func (e *Engine) SetStatus(id AlertID, target Status) error {
	switch target {
	case StatusActive, StatusPaused, StatusCancelled:
	default:
		return ErrInvalidStatus
	}
	if target == StatusCancelled {
		m, err := e.removeIfLive(id, StatusCancelled)
		if err != nil {
			return err
		}
		return e.submit(m)
	}
	e.mu.Lock()
	ref, ok := e.refs[id]
	e.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	from, to := StatusActive, StatusPaused
	if target == StatusActive {
		from, to = StatusPaused, StatusActive
	}
	s := e.slots.get(ref.e.idx)
	for {
		cur := Status(s.Load())
		switch cur {
		case from:
			if s.CompareAndSwap(uint32(cur), uint32(to)) {
				return nil
			}
		case to:
			return nil // already there; idempotent
		default:
			return ErrInvalidTransition
		}
	}
}
```

- [ ] **Step 4: Run all tests to verify they pass**

```bash
go test ./engine/ -race -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/index.go engine/engine_test.go
git commit -m "feat(engine): control plane — upsert, cancel, status transitions"
```

---

### Task 10: Hot path — Tick, Match, fire

**Files:**
- Create: `engine/match.go`
- Test: `engine/engine_test.go` (append)

**Interfaces:**
- Consumes: snapshots (Task 6), flusher (Task 8), trigger queue (Task 4), slots (Task 3).
- Produces: `type Tick struct { Symbol string; Bid, Ask, Mid, Last float64; Present uint8; TS int64 }`; `func TickAllPresent() uint8`; `func (e *Engine) Match(t *Tick)`. Unexported: `func (e *Engine) fire(sid SymbolID, en *entry, price, ts int64)`.

- [ ] **Step 1: Write the failing tests**

Append to `engine/engine_test.go`:

```go
func drainTriggers(e *Engine) []Trigger {
	var out []Trigger
	dst := make([]Trigger, 64)
	for {
		n := e.Triggers().PopBatch(dst)
		if n == 0 {
			return out
		}
		out = append(out, dst[:n]...)
	}
}

func TestMatchGTEFiresOnce(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 42.5))
	e.Sync()
	tick := Tick{Symbol: "USDTRY", Bid: 43, Present: TickAllPresent(), TS: 100}
	e.Match(&tick)
	e.Match(&tick) // duplicate delivery of the same tick must not re-fire
	got := drainTriggers(e)
	if len(got) != 1 || got[0].ID != (AlertID{1}) || got[0].Price != 43 {
		t.Fatalf("triggers=%+v, want one fire of alert 1 at 43", got)
	}
	e.Sync()
	sid, _ := e.syms.Get("USDTRY")
	if n := e.states[sid].snap.Load().trees[treeIndex(PriceBid, DirGTE)].Len(); n != 0 {
		t.Fatalf("fired alert still indexed: %d entries", n)
	}
}

func TestMatchLTEAndBoundary(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	// LTE at exactly the market price must fire (<=).
	e.Upsert(testSpec(2, "USDTRY", PriceAsk, DirLTE, 50))
	e.Sync()
	e.Match(&Tick{Symbol: "USDTRY", Ask: 50, Present: TickAllPresent(), TS: 100})
	got := drainTriggers(e)
	if len(got) != 1 || got[0].ID != (AlertID{2}) {
		t.Fatalf("triggers=%+v, want one fire of alert 2", got)
	}
}

func TestMatchSkips(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	// Paused.
	e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 42.5))
	e.SetStatus(AlertID{1}, StatusPaused)
	// Not yet valid.
	e.Upsert(AlertSpec{ID: AlertID{2}, Symbol: "USDTRY", PriceType: PriceBid,
		Direction: DirGTE, TargetPrice: 42.5, ValidFrom: 200, AutoDeactivate: true})
	// Expired (lazy inline check; reaper runs on 1s cadence, don't wait).
	e.Upsert(AlertSpec{ID: AlertID{3}, Symbol: "USDTRY", PriceType: PriceBid,
		Direction: DirGTE, TargetPrice: 42.5, ValidFrom: 1, Expires: 50, AutoDeactivate: true})
	// Wrong price type: alert on BID, tick carries only ASK.
	e.Upsert(testSpec(4, "USDTRY", PriceBid, DirGTE, 42.5))
	e.Sync()
	e.Match(&Tick{Symbol: "USDTRY", Ask: 100, Present: 1 << uint(PriceAsk), TS: 100})
	if got := drainTriggers(e); len(got) != 0 {
		t.Fatalf("skips failed, triggers=%+v", got)
	}
	// Unknown symbol: no-op.
	e.Match(&Tick{Symbol: "NOPE", Bid: 100, Present: TickAllPresent(), TS: 100})
	// Not triggered by later valid tick for alert 1 (still paused) — sanity.
	e.Match(&Tick{Symbol: "USDTRY", Bid: 100, Present: 1 << uint(PriceBid), TS: 100})
	if got := drainTriggers(e); len(got) != 0 {
		t.Fatalf("paused alert fired: %+v", got)
	}
}

func TestMatchZeroAllocs(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	for i := byte(0); i < 50; i++ {
		e.Upsert(testSpec(i+1, fmt.Sprintf("S%d", i%5), PriceType(i%4),
			Direction(i%2), float64(i)*10))
	}
	e.Sync()
	tick := Tick{Symbol: "S3", Bid: 1e9, Ask: 1e9, Mid: 1e9, Last: 1e9,
		Present: TickAllPresent(), TS: 1 << 40}
	allocs := testing.AllocsPerRun(200, func() { e.Match(&tick) })
	if allocs != 0 {
		t.Fatalf("Match allocates: %.0f allocs/op, want 0", allocs)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./engine/ -run 'TestMatch' -v
```

Expected: FAIL — `Tick`/`Match` undefined.

- [ ] **Step 3: Implement**

Create `engine/match.go`:

```go
package engine

// Tick is a market tick. Present is a bitmask of PriceType bits for the
// quote fields the feed actually carries; absent fields are not evaluated.
type Tick struct {
	Symbol  string
	Bid     float64
	Ask     float64
	Mid     float64
	Last    float64
	Present uint8
	TS      int64 // unix nanos
}

// TickAllPresent returns a Present mask covering all four price types.
func TickAllPresent() uint8 { return 1<<priceTypeCount - 1 }

func priceOf(t *Tick, pt PriceType) float64 {
	switch pt {
	case PriceBid:
		return t.Bid
	case PriceAsk:
		return t.Ask
	case PriceMid:
		return t.Mid
	default:
		return t.Last
	}
}

// Match evaluates a tick against every indexed alert for its symbol.
// Lock-free and allocation-free; it never blocks and never mutates shared
// trees — firing is CAS-gated per alert, tree removal is deferred.
func (e *Engine) Match(t *Tick) {
	if t.Present == 0 {
		return
	}
	sid, ok := e.syms.Get(t.Symbol)
	if !ok {
		return
	}
	snap := e.states[sid].snap.Load()
	if snap == nil {
		return
	}
	for pt := PriceType(0); pt < priceTypeCount; pt++ {
		if t.Present&(1<<uint(pt)) == 0 {
			continue
		}
		price := priceOf(t, pt)
		// GTE fires when market >= target ⇔ every target <= price:
		// Descend from the tick price downward — all entries qualify. The
		// probe carries the max id so entries exactly at the tick price are
		// not skipped by the id tie-break.
		for en := range snap.trees[treeIndex(pt, DirGTE)].Descend(entryKeyMax(price)) {
			e.fire(sid, &en, price, t.TS)
		}
		// LTE fires when market <= target ⇔ every target >= price:
		// Ascend from the tick price upward — all entries qualify.
		for en := range snap.trees[treeIndex(pt, DirLTE)].Ascend(entryKey(price)) {
			e.fire(sid, &en, price, t.TS)
		}
	}
}

// fire performs the exactly-once transition for one candidate entry: lazy
// validity window, CAS ACTIVE→TRIGGERED, dispatch, deferred removal.
func (e *Engine) fire(sid SymbolID, en *entry, price, ts int64) {
	if ts < en.validFrom {
		return
	}
	if en.expires != 0 && ts >= en.expires {
		return
	}
	s := e.slots.get(en.idx)
	for {
		cur := s.Load()
		if Status(cur) != StatusActive {
			return // paused, or already fired/retired by another path
		}
		if s.CompareAndSwap(cur, uint32(StatusTriggered)) {
			break
		}
	}
	e.triggers.TryPush(Trigger{ID: en.id, Price: price, TS: ts})
	e.trySubmit(mutation{op: mutRemove, sid: sid, e: *en})
}
```

- [ ] **Step 4: Run all tests to verify they pass**

```bash
go test ./engine/ -race -v
```

Expected: PASS — including `TestMatchZeroAllocs`.

**If `TestMatchZeroAllocs` fails:** the allocation is almost certainly the `iter.Seq`
closure returned by `Ascend`/`Descend` escaping to the heap. In that case, before
touching engine code, check `go doc github.com/tidwall/btype.Table | grep -iE 'seek|iter|walk'`
for a cursor/callback iteration API that does not allocate, and switch the two
scan loops to it (keeping the same entry order). If no allocation-free iteration
exists, pool the iterator objects with a `sync.Pool` reclaimed after each scan —
the invariant to preserve is `AllocsPerRun(...) == 0`, not the specific loop form.

- [ ] **Step 5: Commit**

```bash
git add engine/match.go engine/engine_test.go
git commit -m "feat(engine): lock-free zero-alloc Match hot path with CAS firing"
```

---

### Task 11: Reaper — expiry sweep + slot recycling

**Files:**
- Create: `engine/reaper.go`
- Modify: `engine/engine.go` (start reaper in `New`)
- Test: `engine/engine_test.go` (append)

**Interfaces:**
- Consumes: `expEntry` (Task 7), `submit`/`trySubmit` (Task 8), `slots.recycle` (Task 3), `expQ` (Task 7).
- Produces (unexported): `func compareExp(a, b expEntry) int`; `func (e *Engine) runReaper()` (launched by `New`); `func (e *Engine) sweep(now int64) int`.

- [ ] **Step 1: Write the failing test**

Append to `engine/engine_test.go`:

```go
func TestReaperExpires(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = 20 * time.Millisecond
	e := New(cfg)
	defer e.Close()
	expiry := time.Now().Add(40 * time.Millisecond).UnixNano()
	e.Upsert(AlertSpec{ID: AlertID{9}, Symbol: "USDTRY", PriceType: PriceBid,
		Direction: DirGTE, TargetPrice: 42.5, ValidFrom: 1, Expires: expiry,
		AutoDeactivate: true})
	e.Sync()
	if s := e.Stats(); s.Live != 1 {
		t.Fatalf("Live=%d, want 1", s.Live)
	}
	// Wait long enough for expiry + one sweep + one flush.
	deadline := time.Now().Add(2 * time.Second)
	for e.Stats().Live != 0 {
		if time.Now().After(deadline) {
			t.Fatal("alert never expired")
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.Sync()
	sid, _ := e.syms.Get("USDTRY")
	if n := e.states[sid].snap.Load().trees[treeIndex(PriceBid, DirGTE)].Len(); n != 0 {
		t.Fatalf("expired entry still indexed: %d", n)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./engine/ -run TestReaper -v
```

Expected: FAIL — the alert never expires (`runReaper` undefined; `New` doesn't start it).

- [ ] **Step 3: Implement**

Create `engine/reaper.go`:

```go
package engine

import (
	"time"

	"github.com/tidwall/btype"
)

// compareExp orders the reaper's expiry table by (expires, idx).
func compareExp(a, b expEntry) int {
	if a.expires < b.expires {
		return -1
	}
	if a.expires > b.expires {
		return 1
	}
	switch {
	case a.e.idx < b.e.idx:
		return -1
	case a.e.idx > b.e.idx:
		return 1
	}
	return 0
}

// runReaper is the sole owner of the expiry table. It drains expQ
// registrations, sweeps entries past their deadline, and recycles slots
// whose grace period (2× interval, long after any snapshot reader) elapsed.
func (e *Engine) runReaper() {
	defer e.reapWG.Done()
	e.expiry = *btype.NewTableOptions(btype.TableOptions[expEntry]{Compare: compareExp})
	ticker := time.NewTicker(e.cfg.ReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case x := <-e.expQ:
			e.expiry.Insert(x)
		case now := <-ticker.C:
			e.sweep(now.UnixNano())
			e.slots.recycle(time.Now().Add(-2 * e.cfg.ReaperInterval))
		case <-e.done:
			for {
				select {
				case x := <-e.expQ:
					e.expiry.Insert(x)
					continue
				default:
				}
				return
			}
		}
	}
}

// sweep expires entries whose deadline has passed. Slot CAS guards against
// racing fires and cancels; removal (and slot retirement) goes through the
// flusher exactly like a trigger removal.
func (e *Engine) sweep(now int64) int {
	const maxPerSweep = 10_000
	var due []expEntry
	for x := range e.expiry.All() { // table is expiry-ordered
		if x.expires > now || len(due) >= maxPerSweep {
			break
		}
		due = append(due, x)
	}
	for _, x := range due {
		e.expiry.Delete(x) // after iteration completes; safe
		s := e.slots.get(x.e.idx)
		if s.CompareAndSwap(uint32(StatusActive), uint32(StatusExpired)) ||
			s.CompareAndSwap(uint32(StatusPaused), uint32(StatusExpired)) {
			e.submit(mutation{op: mutRemove, sid: x.sid, e: x.e})
		}
	}
	return len(due)
}
```

In `engine.go`, `New`, add the reaper launch next to the flusher start:

```go
	e.flushWG.Add(1)
	go e.runFlusher()
	e.reapWG.Add(1)
	go e.runReaper()
	return e
}
```

Note: `sweep` deletes from `e.expiry` only after the iteration loop over the
prefix has completed (`due` is fully collected first), so mid-iteration
mutation never happens.

- [ ] **Step 4: Run all tests to verify they pass**

```bash
go test ./engine/ -race -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/reaper.go engine/engine.go engine/engine_test.go
git commit -m "feat(engine): expiry reaper with slot grace recycling"
```

---

### Task 12: Oracle equivalence + concurrency stress

**Files:**
- Create: `engine/oracle_test.go`

**Interfaces:**
- Consumes: full public engine API (`New`, `Upsert`, `Sync`, `Match`, `Triggers`, `Close`, `SetStatus`, `Cancel`).

- [ ] **Step 1: Write the oracle test**

Create `engine/oracle_test.go`:

```go
package engine

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// TestOracle compares the engine against a brute-force linear scan over the
// same alert set and tick sequence: the set of fired alert IDs and the price
// each fired at must match exactly.
func TestOracle(t *testing.T) {
	defer goleak.VerifyNone(t)
	rng := rand.New(rand.NewSource(1))
	e := New(DefaultConfig())
	defer e.Close()

	const nAlerts, nTicks = 500, 300
	const base = int64(1 << 40) // tick timestamps: base + k*second
	const step = int64(1e9)

	type rec struct {
		spec AlertSpec
	}
	var recs []rec
	for i := 0; i < nAlerts; i++ {
		validFrom := base - int64(rng.Intn(2))*step
		var expires int64
		if rng.Intn(4) == 0 {
			expires = base + int64(rng.Intn(nTicks/10))*step
		}
		s := AlertSpec{
			ID:             mkID(uint32(i + 1)),
			Symbol:         fmt.Sprintf("S%d", rng.Intn(20)),
			PriceType:      PriceType(rng.Intn(int(priceTypeCount))),
			Direction:      Direction(rng.Intn(2)),
			TargetPrice:    float64(rng.Intn(400)) + float64(rng.Intn(4))/4,
			ValidFrom:      validFrom,
			Expires:        expires,
			AutoDeactivate: true,
		}
		if err := e.Upsert(s); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
		recs = append(recs, rec{spec: s})
	}
	e.Sync()

	ticks := make([]Tick, nTicks)
	for k := range ticks {
		ticks[k] = Tick{
			Symbol:  fmt.Sprintf("S%d", rng.Intn(20)),
			Bid:     float64(rng.Intn(400)) + 0.5,
			Ask:     float64(rng.Intn(400)) + 0.5,
			Mid:     float64(rng.Intn(400)) + 0.5,
			Last:    float64(rng.Intn(400)) + 0.5,
			Present: TickAllPresent(),
			TS:      base + int64(k)*step,
		}
	}

	// Brute force: per alert, the FIRST tick (in order) satisfying all
	// conditions, mirroring the spec's filter stages.
	expected := map[AlertID]float64{}
	for _, r := range recs {
		for _, tk := range ticks {
			if tk.Symbol != r.spec.Symbol {
				continue
			}
			price := priceOf(&tk, r.spec.PriceType)
			if tk.TS < r.spec.ValidFrom {
				continue
			}
			if r.spec.Expires != 0 && tk.TS >= r.spec.Expires {
				continue
			}
			hit := (r.spec.Direction == DirGTE && price >= r.spec.TargetPrice) ||
				(r.spec.Direction == DirLTE && price <= r.spec.TargetPrice)
			if hit {
				expected[r.spec.ID] = price
				break // ONCE semantics
			}
		}
	}

	for k := range ticks {
		e.Match(&ticks[k])
	}
	got := map[AlertID]float64{}
	for _, tr := range drainTriggers(e) {
		if _, dup := got[tr.ID]; dup {
			t.Fatalf("alert %v fired twice", tr.ID)
		}
		got[tr.ID] = tr.Price
	}

	if len(got) != len(expected) {
		t.Fatalf("fired %d alerts, expected %d", len(got), len(expected))
	}
	for id, price := range expected {
		if gp, ok := got[id]; !ok || gp != price {
			t.Fatalf("alert %v: fired at %v (ok=%v), expected %v", id, gp, ok, price)
		}
	}
}

func mkID(v uint32) AlertID {
	return AlertID{0x01, 0x91, 0xb4, 0x02, byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}
```

Note: the oracle relies on expiry being decided by the *lazy inline* check
(`TS >= expires`), and the reaper interval (1s) is far longer than the test's
runtime, so reaper interference is impossible.

- [ ] **Step 2: Run the oracle test**

```bash
go test ./engine/ -race -run TestOracle -v
```

Expected: PASS. Any mismatch is a real correctness bug — debug with the
failing alert ID (the seed is fixed, so it reproduces).

- [ ] **Step 3: Write the stress test**

Append to `engine/oracle_test.go`:

```go
// TestStressRace hammers one hot symbol with concurrent tick producers and
// mutators. Invariants: no alert ever fires twice; no panic under -race; no
// goroutine leaks.
func TestStressRace(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	const nAlerts = 2000
	for i := 0; i < nAlerts; i++ {
		e.Upsert(AlertSpec{
			ID: mkID(uint32(i + 1)), Symbol: "HOT", PriceType: PriceType(i % 4),
			Direction: Direction(i % 2), TargetPrice: float64(100 + i%400),
			ValidFrom: 1, AutoDeactivate: true,
		})
	}
	e.Sync()

	const producers, mutators = 4, 2
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(p)))
			price := 300.0
			for {
				select {
				case <-stop:
					return
				default:
				}
				price += rng.NormFloat64()
				if price < 1 {
					price = 1
				}
				e.Match(&Tick{Symbol: "HOT", Bid: price, Ask: price, Mid: price,
					Last: price, Present: TickAllPresent(), TS: time.Now().UnixNano()})
			}
		}(p)
	}
	for m := 0; m < mutators; m++ {
		wg.Add(1)
		go func(m int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(1000 + int64(m)))
			for i := 0; i < 2000; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := mkID(uint32(rng.Intn(nAlerts) + 1))
				switch rng.Intn(3) {
				case 0:
					e.Upsert(AlertSpec{ID: id, Symbol: "HOT",
						PriceType: PriceBid, Direction: DirGTE,
						TargetPrice: float64(100 + rng.Intn(400)),
						ValidFrom: 1, AutoDeactivate: true})
				case 1:
					e.SetStatus(id, Status(rng.Intn(3)+1)) // may fail; ignore
				case 2:
					e.Cancel(id) // may fail; ignore
				}
			}
		}(m)
	}
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	e.Sync()

	fired := map[AlertID]bool{}
	for _, tr := range drainTriggers(e) {
		if fired[tr.ID] {
			t.Fatalf("alert %v fired twice under concurrency", tr.ID)
		}
		fired[tr.ID] = true
	}
}
```

Add `"sync"` to the test file imports.

- [ ] **Step 4: Run the full suite with the race detector**

```bash
go test ./engine/ -race -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/oracle_test.go
git commit -m "test(engine): brute-force oracle and concurrency stress"
```

---

### Task 13: Benchmarks

**Files:**
- Create: `engine/bench_test.go`

**Interfaces:**
- Consumes: full public engine API.

- [ ] **Step 1: Write the benchmarks**

Create `engine/bench_test.go`:

```go
package engine

import (
	"fmt"
	"os"
	"testing"
)

// benchMatch builds an engine with `alerts` alerts spread over 1000 symbols
// and measures Match on a tick that fires nothing (sparse) — every tree
// lookup is a seek + immediate stop, the dominant shape under sustained load.
func benchSparse(b *testing.B, alerts int) {
	e := New(DefaultConfig())
	defer e.Close()
	const symbols = 1000
	perSym := alerts / symbols / 2
	for s := 0; s < symbols; s++ {
		sym := fmt.Sprintf("SYM%04d", s)
		for i := 0; i < perSym; i++ {
			// GTE targets far above the tick price; LTE targets far below.
			e.Upsert(AlertSpec{ID: mkID(uint32(s*perSym*2 + i*2)), Symbol: sym,
				PriceType: PriceType(i % 4), Direction: DirGTE,
				TargetPrice: 1000 + float64(i)*0.01, ValidFrom: 1, AutoDeactivate: true})
			e.Upsert(AlertSpec{ID: mkID(uint32(s*perSym*2 + i*2 + 1)), Symbol: sym,
				PriceType: PriceType(i % 4), Direction: DirLTE,
				TargetPrice: 100 - float64(i)*0.01, ValidFrom: 1, AutoDeactivate: true})
		}
	}
	e.Sync()
	tick := Tick{Symbol: "SYM0500", Bid: 500, Ask: 500, Mid: 500, Last: 500,
		Present: TickAllPresent(), TS: 1 << 40}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Match(&tick)
	}
}

func BenchmarkMatchSparse1M(b *testing.B) { benchSparse(b, 1_000_000) }

func BenchmarkMatchSparse10M(b *testing.B) {
	if os.Getenv("CHRONO_BENCH_10M") == "" {
		b.Skip("set CHRONO_BENCH_10M=1 to run the 10M benchmark")
	}
	benchSparse(b, 10_000_000)
}

// BenchmarkMatchDenseSkip measures the sustained-load shape: alerts already
// TRIGGERED, so every scan touches entries whose slot CAS fails. This is the
// per-tick cost the engine pays between price gaps.
func BenchmarkMatchDenseSkip(b *testing.B) {
	e := New(DefaultConfig())
	defer e.Close()
	const perTree = 10_000
	for i := 0; i < perTree; i++ {
		e.Upsert(AlertSpec{ID: mkID(uint32(i*2)), Symbol: "SYM0500",
			PriceType: PriceType(i % 4), Direction: DirGTE,
			TargetPrice: 100 + float64(i)*0.01, ValidFrom: 1, AutoDeactivate: true})
		e.Upsert(AlertSpec{ID: mkID(uint32(i*2+1)), Symbol: "SYM0500",
			PriceType: PriceType(i % 4), Direction: DirLTE,
			TargetPrice: 900 - float64(i)*0.01, ValidFrom: 1, AutoDeactivate: true})
	}
	e.Sync()
	tick := Tick{Symbol: "SYM0500", Bid: 500, Ask: 500, Mid: 500, Last: 500,
		Present: TickAllPresent(), TS: 1 << 40}
	e.Match(&tick) // fire everything once; removals flush out
	e.Sync()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Match(&tick)
	}
}
```

- [ ] **Step 2: Run the benchmarks**

```bash
go test ./engine/ -run '^$' -bench BenchmarkMatch -benchtime 2s
```

Expected: `BenchmarkMatchSparse1M` and `BenchmarkMatchDenseSkip` report `0 allocs/op`. Record the ns/op numbers in the commit message — they are the baseline the spec's "sub-microsecond" claim is tracked against. If allocs/op is nonzero, apply the Task 10 contingency (allocation-free iteration API or pooled iterators) before proceeding.

- [ ] **Step 3: Commit**

```bash
git add engine/bench_test.go
git commit -m "bench(engine): sparse/dense Match benchmarks, 1M default + gated 10M"
```

---

**AMENDMENT (landed during Task 10 execution):** btype's `Release()` clears Table
headers in place, so the Task 6/8 store-then-release pattern raced with in-flight
snapshot readers. The landed engine amends the snapshot lifecycle: `snapshot` carries
seq-cst `readers`/`retired` words; `Match` pins before scanning (retry on retired);
the flusher parks retired snapshots and releases them in a later `applyBatch` sweep
once `readers == 0` (Close drains the parked list). The hot path remains lock-free
and alloc-free (~3 extra atomic ops per Match). Also: `fire`'s price parameter is
`float64` (the plan's `int64` sketch did not compile), and `TestMatchSkips`'s final
assertion was corrected (alert 4 legitimately fires on the closing BID tick; the
test now asserts alerts 1–3 stay silent).

## Self-Review (conducted after writing; fixes applied inline)

**Spec coverage** (spec section → tasks):
- §3 architecture/packages → Tasks 1–13 file layout ✓
- §4 data model (48-byte entry, btype.Table comparator, cold record, interning) → Tasks 2, 5, 6, 7 ✓
- §5 partitioning (8 trees, Ascend/Descend targeted scans) → Tasks 6, 10 ✓
- §6 COW publication (bounded queue, batch, publish/release, control-plane blocking) → Task 8 ✓
- §7 hot path + exactly-once CAS + split fire/remove → Task 10 ✓
- §8 lifecycle (status CAS transitions, lazy expiry + reaper, slot recycling) → Tasks 9, 11 ✓
- §9 dispatch (Vyukov ring, drop+count) → Task 4 ✓
- §11 sentinel errors, validation-at-boundary → Tasks 7, 9 ✓
- §12 testing (oracle, stress, zero-alloc, benchmarks) → Tasks 10, 12, 13 ✓

**Known deviations from spec text (intentional, recorded):**
1. Spec §7 pseudocode checks `expires` only; the plan's `fire` checks `validFrom` first then `expires` — matches the spec's PRE_FILTER stage ordering.
2. Spec §8 says the reaper "frees idx slots"; the plan adds a 2×-interval grace before reuse (slot-recycling race hardening not visible at spec altitude).
3. Spec §11 lists `ErrQueueFull` for the engine; the engine's control plane *blocks* (per spec §6) and only the service returns `UNAVAILABLE` — `ErrQueueFull` therefore belongs to Phase B's service layer, not the engine.

**Type consistency:** `mutation{op, sid, e, done}` / `mutOp` (Task 7, used 8–11), `expEntry{expires, sid, e}` (Task 7, used 9, 11), `slotArena.alloc/get/retire/recycle` (Task 3, used 7–11), `treeIndex(pt, dir)` (Task 6, used 8–11), `AlertID`/`PriceType`/`Direction`/`Status` (Task 2, used everywhere) — verified consistent across tasks.

**Out of scope (Phase B plan, to be written after Phase A lands):** gRPC API, service layer, `cmd/chronod`, `cmd/bench` latency harness.

