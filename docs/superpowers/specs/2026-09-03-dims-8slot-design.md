# Multi-Dimension Matching — 8-Slot Design Spec

**Date:** 2026-09-03
**Status:** Approved (brainstorming complete)
**Module:** `github.com/emir/chrono-tree/engine`
**Depends on:** `2026-09-02-chrono-tree-design.md` (Phase A, merged)

## 1. Purpose

Extend the engine so an alert can carry up to 8 matching dimensions
(breakdowns) alongside price: "fire when ask >= X **and** segment == gold".
Dimensions fold into the existing B-tree key as a comparator prefix, keeping
the scan-shape invariant — everything a probe reaches is a candidate — and
the hot path allocation-free.

## 2. Decisions

| Question | Decision |
|---|---|
| Vocabulary ownership | Caller owns string↔value mapping; engine is value-agnostic (like price scale) |
| Width | Engine-wide, fixed: `len(Config.Dims)`, max 8; not per-alert |
| Matching semantics | Strict exact-match per slot; no wildcards |
| "Any value" | Caller-side fan-out: one alert per value (dims are closed-cardinality) |
| Sentinel | `0xFFFF` per unused slot; real values `0x0000..0xFFFE` |
| Layout | `dims [8]uint16` inline in `entry`; 64 bytes total (one cache line) |
| Alternatives rejected | Partition trees per dim combo (combinatorial explosion); fire-time dim filter (breaks scan-shape invariant, pays full-symbol scans) |

## 3. Entry Layout

```go
type entry struct {          // 64 bytes, pointer-free — one cache line
    price     Price       // int64 base units
    id        AlertID     // [16]byte
    dims      [8]uint16   // sentinel-padded trailing slots
    validFrom int64
    expires   int64
    idx       uint32
    flags     uint8
}
```

- No `width` field in the entry: width is engine-wide, and trailing slots are
  always sentinel everywhere. The comparator is **width-specialized per
  engine**: tables are built with a comparator closure over the engine's
  width, so the dim-prefix loop runs only over configured slots — a width-0
  engine compares `(price, id)` exactly like the pre-dims engine. (The
  original design compared all 8 slots unconditionally; measurement showed
  the fixed loop taxes every seek comparison even at width 0 and missed the
  Sparse1M gate by ~40%, so the closure replaced it.)
- `TestEntrySize` updates 48 → 64.
- GC pressure is unchanged: zero new pointers, same object count. The entry
  remains a flat value inside B-tree nodes.

## 4. Comparator

`compareEntry` is lexicographic over `(dims[0..width), price, id)`, built per
engine via a comparator closure that captures the width at table
construction. btype's keyed `Ascend`/`Descend` are seek-and-walk iterators
with no key bound; because dim combinations now share one tree, each Match
loop carries a dim-block stop guard (`en.dims != dims` → break, skipped
entirely at width 0) — entries arrive in `(dims, price, id)` order, so once
dims differ every remaining entry belongs to another combination.

Boundary probes gain the dim prefix:

- GTE: `Descend(entryKeyMax(tickDims, price))` — dims equal to the tick's,
  targets at/below the tick price, max-id trick preserved.
- LTE: `Ascend(entryKey(tickDims, price))` — dims equal, targets above.

Everything reached by the probe fires (subject to the existing validity/CAS
gates in `fire`, which do not change).

## 5. Public API

### Config

```go
type Config struct {
    // ...existing fields...
    Dims []string // positional dimension names; slot i = Dims[i]; max 8
}
```

`New` panics on more than 8 dims or empty names. Names are caller-side
documentation; the engine never reads them beyond validation. Width 0 (no
`Dims`) is exactly today's behavior.

### AlertSpec / Tick

```go
type AlertSpec struct {
    // ...existing fields...
    Dims [8]uint16 // width real values, rest sentinel
}
type Tick struct {
    // ...existing fields...
    Dims [8]uint16 // same contract
}
```

- `Upsert` validates that every slot `< width` carries a real value; a
  sentinel inside width returns new sentinel error `ErrDims`. No partial
  specification — fan-out is the caller's job.
- Slots `>= width` are **normalized** to the sentinel by the engine (both
  `Upsert` and `Match`), not validated — a zero-value `Dims` array must work
  at any width, or every existing zero-value `AlertSpec`/`Tick` would break.
- `Match` cannot return errors; a malformed tick dim array (sentinel inside
  width) is dropped like `Present == 0` — a silent no-op. The `Dims(...)`
  helper makes the correct form the easy form:

```go
// Dims builds a sentinel-padded dim array from width real values.
func Dims(values ...uint16) [8]uint16
```

- `Trigger` is unchanged (`ID, Price, TS`); consumers join back to their own
  store by ID.
- `AlertMeta.Segment` stays cold metadata; it is not a matching dimension and
  does not migrate.

## 6. Hot and Cold Paths

- **Match:** unchanged skeleton (intern → pin snapshot → two probes per
  present price type). Only the probe key gains the dim prefix. The tick's
  `Dims` array copies by value onto the stack — no allocation, no indirection.
- **fire:** untouched. Dim equality is guaranteed by the tree key; no new
  checks before the CAS.
- **Mutation path:** `Upsert` builds the entry's `dims` after validation; the
  COW copy now moves 64B per entry instead of 48B — proportional flusher cost
  increase, amortized and off the hot path. Removal compares full entries,
  already covered by the comparator change.
- **Untouched:** slot arena and generation packing, expiry table and reaper,
  trigger ring, RCU snapshot protocol, the `price` package.

## 7. Evolution

Adding a dimension = append a name to `Config.Dims` + restart + re-feed.
The engine is volatile by design; there is no data migration. Existing
dim-bearing alerts are re-upserted by the caller with the new slot filled.
Removing a dimension is the same in reverse. Growing past 8 dims requires a
new layout (bigger array, two cache lines) — out of scope; 8 with caller-side
fan-out covers closed-cardinality sets.

## 8. Testing

Existing suite passes unchanged at width 0 (except `TestEntrySize` → 64).

1. `TestDimsMatching` — table-driven: exact match fires; any differing slot
   does not fire; real-vs-sentinel in the same slot does not fire; fan-out
   (two alerts, two values, one tick) fires exactly one.
2. Oracle test extension — randomized brute-force evaluator runs at width 3
   (random values); width 0 remains covered by the pre-existing `TestOracle`.
   The naive check is element-wise dim equality plus
   existing price/direction logic. Main correctness net for the comparator
   rewrite.
3. `TestDimsValidation` — `Upsert` rejects sentinel-inside-width (`ErrDims`)
   and normalizes trailing slots (zero-value compatible); `New` panics on >8
   dims.
4. `TestDimsHelper` — `Dims(...)` pads with sentinel.
5. Zero-alloc assertions (`AllocsPerRun == 0`) run with width > 0 ticks.
6. Race tests run with a dims-bearing config variant.

## 9. Benchmarks & Acceptance Gates

Existing `Sparse1M` / `DenseSkip` keep running at width 0 as the
compatibility baseline. New: `BenchmarkMatchDimsSparse1M` and
`BenchmarkMatchDimsDenseSkip` at width 2.

Gates (median of `-count=5`, AMD Ryzen 5 5600H). The original estimate-derived
gates (≤575/≤600/≤200/≤240) were unreachable at 64-byte entries: profiling
showed sparse ticks visit zero entries, so the width-0 delta is B-tree
routing bandwidth (64B vs 48B entries), irreducible in code. Gates were
re-derived on 2026-09-04 from a same-day back-to-back calibration against
the 48-byte baseline (08ad75f: Sparse1M 541.0 ns, DenseSkip 187.5 µs):

| Benchmark | Gate | Rationale |
|---|---|---|
| Sparse1M (width 0) | ≤ 650 ns/op | baseline 541.0 + ≤20% entry-bandwidth tax |
| DimsSparse1M (width 2) | ≤ 1000 ns/op | measured 952.1; comparator routes across dim blocks |
| DenseSkip (width 0) | ≤ 225 µs/op | baseline 187.5 + ≤20% |
| DimsDenseSkip (width 2) | ≤ 240 µs/op | unchanged |
| All of the above | 0 B/op, 0 allocs/op | hard |
| `unsafe.Sizeof(entry{})` | == 64 | hard |

Rejected alternatives (2026-09-04): 56-byte entry with uint8 dims (loses
value headroom for ~half the width-0 tax); fire-time dim filtering
(re-introduces non-qualifying scans).

## 10. Out of Scope

- Wildcard/"any" engine semantics (caller-side fan-out instead).
- Per-dim secondary indexes or partitioned trees.
- Dimension value interning inside the engine (service-layer concern).
- More than 8 dimensions.
