# chrono-tree — Design Spec

**Date:** 2026-09-02
**Status:** Approved (brainstorming complete)
**Module:** `github.com/emir/chrono-tree` (Go 1.27)

## 1. Purpose

An ultra-low-latency, zero-allocation, in-memory alert evaluation engine holding 1M–10M
concurrent financial price alerts, matched against a continuous tick stream with
sub-microsecond lookups, minimal RAM, and a bounded GC footprint. Delivered as a full
service: a pure Go engine library, a gRPC service layer around it, and a runnable
daemon/benchmark harness.

## 2. Requirements Summary

| Requirement | Decision |
|---|---|
| Language | Idiomatic Go 1.27, generics, cache-conscious layout |
| Index structure | `github.com/tidwall/btype` (strictly) |
| Hot path | Zero per-tick heap allocations; no dynamic strings |
| Concurrency model | Copy-on-write snapshot publication (Approach 3) |
| Trigger semantics | `ONCE` only for v1: fire → deactivate → remove |
| Symbol universe | ~1k–10k symbols, unevenly distributed with a hot core |
| Overflow policy | Bounded queue, drop-newest triggers, increment metric |
| Persistence | Volatile cache; upstream store re-feeds via ingestion API on boot |
| Deliverable | Engine library + gRPC service + `cmd/` daemon & benchmark harness |

## 3. Architecture

```
chrono-tree/
├── engine/                  # benchmarkable core — no network, no I/O
│   ├── symbol.go            # symbol interning: string → SymbolID (uint32)
│   ├── alert.go             # hot alert record, cold metadata, status slots
│   ├── shard.go             # per-symbol state: 8 btype tables + snapshot ptr
│   ├── index.go             # COW mutation queue + flusher (single writer)
│   ├── match.go             # Match(tick) — lock-free hot path
│   ├── trigger.go           # MPMC ring buffer, drop+count
│   └── reaper.go            # background expiry sweeper
├── api/                     # protobuf definitions (gRPC service)
├── service/                 # gRPC server wiring engine ↔ API, config
├── cmd/chronod/             # runnable daemon
└── cmd/bench/               # load harness, p50/p99/p999 latency report
```

### Data flow

- **Control plane** (rare): gRPC `Upsert/UpdateStatus/Cancel` → validated → op appended
  to a bounded mutation queue → **flusher goroutine** (single writer) drains the batch
  onto a `Copy()` of the affected symbol's trees → publishes via `atomic.Pointer` →
  `Release()` on the retired snapshot.
- **Data plane** (hot): tick → intern symbol (map read, no alloc) → `atomic.Load`
  snapshot → scan 8 tree slots → fire matches via CAS on per-alert status slots →
  push `Trigger` onto ring buffer. No locks, no allocations, no coordination with the
  flusher.
- **Reaper**: single goroutine owns a global expiry-ordered tree (sole writer, no
  contention), sweeping expired alerts and enqueuing removals through the mutation queue.

## 4. Data Model

### Hot record (stored by value inside the b-tree)

```go
type entry struct {          // 48 bytes, pointer-free
    price    float64 // target price — primary sort key
    id       [16]byte // UUIDv7 (alert_id), tie-breaker so equal prices coexist
    idx      uint32  // slot into the engine's status-slots array
    validFrom int64  // unix nanos
    expires  int64  // unix nanos; 0 = never
    flags    uint8  // bit0 active, bits1-2 price_type, bit3 direction, bit4 auto_deactivate
}
```

Stored in `btype.Table[entry]` with a custom `Compare` on `(price, id)`. Rationale:
`btype.Map` requires `cmp.Ordered` keys; `[16]byte` is not `cmp.Ordered`, and a
composite struct key isn't either — `btype.Table`'s custom comparator is the intended
escape hatch and keeps whole records inside tree nodes (no key→pointer indirection).

### Cold record

`map[AlertID]*AlertMeta` — user context, segment, notification channels, notes, symbol
string, timestamps as real types. Looked up only after a trigger fires; lives entirely
off the hot path.

### Symbol interning

`USDTRY` → `SymbolID (uint32)`; `[]*symbolState` indexed by SymbolID. Interning map is
`RWMutex`-guarded (writes rare). Tick symbol lookup uses a zero-alloc string view of
the incoming bytes (`unsafe` zero-copy, standard market-data trick); benchmarked
against a plain string conversion and the winner kept.

## 5. Index Partitioning

Each `symbolState` holds **8** `btype.Table[entry]` — one per
(price_type ∈ {BID, ASK, MID, LAST}) × (direction ∈ {GTE, LTE}), addressed as
`trees[priceType<<1|dir]`. Every entry reached by a scan is a candidate by
construction: symbol, price type, and direction never need per-entry checks.

Scan shapes (targeted order, no scanning of non-matching records):

- **GTE** fires when `market ≥ target` → all targets `≤ market` → `Descend(market)`
  walks downward from the tick price, terminating at the first target below it.
- **LTE** fires when `market ≤ target` → all targets `≥ market` → `Ascend(market)`
  walks upward from the tick price, terminating at the first target above it.

Rationale for 8 trees vs fewer: a single tree keyed `(price, id)` cannot distinguish
directions, and per-entry direction filtering would reintroduce scans of non-matching
records. 8 trees keeps every iteration purely over qualifying entries.

## 6. COW Publication

Single global flusher goroutine (v1; write load is light):

```
mutations: chan op (bounded, default 4096)
flusher:   for batch := drain(mutations, max 256):
               for each affected symbol:
                   next := snap.Copy()          // O(1), ref-counted COW
                   apply batch ops to next      // allocates shadow path nodes only
                   atomic.Pointer.Store(next)
                   old.Release()
```

Readers `atomic.Load` the snapshot; `btype`'s ref-counting keeps in-use nodes alive
until `Release()` reclaims them after the last reader lets go. The mutation queue is
bounded and submit may block (control plane may wait; the data plane never feels it).
If the queue is persistently saturated, control RPCs return `UNAVAILABLE` — mutations
are never dropped; only notification triggers are.

## 7. Hot Path — `Match(tick)` and Exactly-Once Firing

COW creates one wrinkle: an ONCE alert that fires must be deactivated, but a snapshot
reader cannot mutate the tree. Solution — **split "fire" from "remove"**:

```
Match(tick):
  sid      := intern[tick.Symbol]                       // no alloc
  state    := symbols[sid]
  snap     := state.ptr.Load()                          // lock-free
  for pt in price types present in tick:
      price := tick.field(pt)
      for e in snap.tree[pt|GTE].Descend(price):        // targets ≤ price
          fire(e, price)
      for e in snap.tree[pt|LTE].Ascend(price):         // targets ≥ price
          fire(e, price)

fire(e, price):
  if e.expires/validFrom fail vs tick.TS:  continue     // lazy validity, inline
  if !slots[e.idx].CAS(ACTIVE, TRIGGERED):  continue    // exactly-once gate
  triggers.TryPush(Trigger{e.id, price, tick.TS})       // drop+count if full
  mutations.TryEnqueue(removeOp(e))                     // lazy removal, best-effort
```

- `slots` is an engine-level `[]atomic.Uint32` indexed by `idx` (freelist allocator).
  The CAS makes firing idempotent under concurrent ticks and stale snapshots: a fired
  entry lingering in the tree until the flusher removes it is simply re-skipped.
- `removeOp` enqueue is best-effort non-blocking; a miss defers removal to the reaper.
  Nothing on the hot path ever blocks.
- Per-tick cost: one map probe + one atomic load + up to 4 tree scans over qualifying
  entries only. Target: **0 allocs/op**, low-hundreds-of-ns for sparse hits.

## 8. Lifecycle

- **Status transitions** (`PAUSED`/`ACTIVE`/`TRIGGERED`/`CANCELLED`/`EXPIRED`) live in
  the atomic slot — pause/resume is a CAS, instantly visible to all readers, no COW
  round-trip. Only structural changes (create, cancel, target-price change) go through
  the mutation queue; a price update is remove+insert in one op.
- **Expiry, two layers:**
  1. Lazy inline check in `fire` (correctness backstop, ~2 int64 compares).
  2. Reaper goroutine owns a global `btype.Table` ordered by `(expiresAt, idx)`,
     sweeps past-`now` entries each interval (default 1s), CASes slots to `EXPIRED`,
     enqueues removals, frees `idx` slots. RAM held by dead entries is bounded by
     `expiry_grace + flusher lag`.
- **Slot allocator:** freelist over a geometrically growing chunk array; chunks never
  move (readers hold raw indices across scans); recycled only after reaper and flusher
  retire them. Dense, cache-friendly at 10M entries.

## 9. Dispatch

MPMC ring buffer (`trigger.go`): power-of-two capacity (default 1<<16) of 32-byte
`Trigger{id [16]byte; price float64; ts int64}`, atomic head/tail with per-producer
tickets. Full → `TryPush` fails → `droppedTriggers` counter increments (exported
metric). Consumers drain via `Pop` or batched `PopBatch`. No per-trigger goroutines.

## 10. Memory & GC Budget (10M alerts)

Hot entries 48B + b-tree node overhead (~1.5×) + slots 4B + cold metadata ~120B
⇒ roughly **2.5–3 GB**. Pointer density is minimal (cold map + tree internals only),
so GC scan cost stays near-linear in cold objects; the hot arena is effectively
invisible to GC. Shadow-node allocations from COW are amortized away by the light
write load.

## 11. Service Layer (gRPC)

| RPC | Notes |
|---|---|
| `UpsertAlert(FinancialPriceAlert) → Ack` | idempotent on `alert_id`; validates schema |
| `UpdateAlertStatus(id, status) → Ack` | ACTIVE/PAUSED/CANCELLED transitions |
| `StreamTicks(stream Tick) → Ack` | bidi tick ingestion (plus single `SubmitTick`) |
| `StreamTriggers(TriggerFilter) → stream Trigger` | downstream consumer; drains ring buffer |
| `GetStats() → Stats` | alert counts, queue depth, dropped triggers, flush lag |

Config (flags/env): ring capacity, mutation queue depth, flusher batch max, reaper
interval, listen address, max alerts.

Backpressure contract: tick ingestion never blocks; control RPCs may return
`UNAVAILABLE` when the mutation queue is full; lagging `StreamTriggers` consumers
see drops (accepted by design).

Error handling: validation errors rejected at the API boundary before the engine is
touched; engine returns typed sentinels (`ErrNotFound`, `ErrInvalidTransition`,
`ErrQueueFull`); a poisoned mutation op is rejected and logged — snapshot publication
is all-or-nothing per batch, and the flusher never panics the process.

## 12. Testing & Verification

- **Correctness:** table tests per filter stage; a brute-force oracle — for randomized
  alert sets and tick sequences, the engine's trigger set must equal a naive linear
  scan's set (catches CAS races, ordering bugs, stale-snapshot double-fires). Run with
  `-race`.
- **Concurrency stress:** N tick producers × M mutators hammering one hot symbol;
  goroutine-leak check (`goleak`).
- **Benchmarks (`cmd/bench`):** at 1M and 10M alerts — `ns/op`, `allocs/op` (hard
  assert **0** in CI via `testing.AllocsPerRun`), p50/p99/p999 wall latency per tick
  under concurrent mutation churn, full latency report from the synthetic harness.

## 13. Out of Scope (v1)

- `REPEAT` / re-arm trigger frequencies (schema enum reserved).
- Persistence (snapshots, WAL) — volatile cache, upstream re-feed.
- Per-segment priority queues (drop policy is uniform drop-newest).
- Per-symbol flusher sharding (single global flusher until benchmarks say otherwise).
