# engine/sym-1000 — 2026-09-06 campaign

Layer: engine | layout: parked | alerts: 1,000,000 | symbols: 1000 (2x baseline) | rate: 20000 tps | cluster: 0 | duration: 240 s

## Headline numbers

| Metric | Value | Target / Baseline |
|---|---|---|
| Achieved tps | 19,999.99 | target 20,000 — hit, unsaturated |
| ns/tick | 50,000.02 | 50,000 (pacer-limited, not CPU-bound) |
| RSS seed | 972.3 MiB | — |
| RSS plateau | 992.3 MiB | — |
| RSS peak | 997.7 MiB | baseline 1,000.4 MiB (flat) |
| Heap inuse | 547.9 MiB | — |
| GC count | 40 (~1 per 6 s over 240 s) | — |
| CPU (% of one core) | 7.09 % | baseline 6.32 % (+0.77 pp, ~ +12 % rel) |
| Triggers fired | 0 | parked layout, none expected |
| Drain | -1 ms | no triggers, no drain path exercised |
| Valid | **true** | `RUN engine/sym-1000: VALID` |

Ticks: 4,800,000 sent / 4,800,000 accepted / 0 dropped / 0 ring drops. Wall 240.0 s.

## Top 10 CPU functions (flat, of 16.42 s samples over 240 s)

| # | Function | flat % | Class |
|---|---|---|---|
| 1 | `btype.(*Table[entry]).initCompare.func2` | 22.78 | engine-core (alert-tree comparator closure) |
| 2 | `runtime.nanotime` | 18.45 | runtime (clock reads) |
| 3 | `btype.(*tree[entry]).search` | 13.03 | engine-core (Match tree search) |
| 4 | `internal/runtime/syscall/linux.Syscall6` | 8.10 | runtime (syscalls) |
| 5 | `runtime.futex` | 6.58 | runtime (scheduler sleep/wake) |
| 6 | `time.runtimeNow` | 4.32 | runtime (clock) |
| 7 | `btype.(*tree[entry]).Descend` | 2.38 | engine-core (Match iteration) |
| 8 | `engine.(*Engine).Match` | 1.46 | engine-core |
| 9 | `engine.entryKeyMax` | 1.46 | engine-core |
| 10 | `btype.(*Table[entry]).Descend` | 1.34 | engine-core |

Engine-core ~42 % flat, runtime/scheduler/clock ~37 % flat; GC proper is absent from the top (idle workload, low live-set pressure). Driver overhead (`bench.(*Market).Step` 1.22 %, `bench.EngineTick` 0.85 %) is minor. `Engine.Match` cum is 49.9 %, with `tree.search` at 36.5 % cum and `Table.initCompare` at 23.4 % cum — read-path matching with comparator invocation is the dominant engine CPU cost.

## Top 5 heap sites (alloc_space, 8.98 GB total allocated over 240 s)

| # | Site | flat % | Class |
|---|---|---|---|
| 1 | `btype.(*tree[entry]).newNode` (via `Table.Insert`/`copyNode` <- `applyBatch`) | 88.95 | engine-core (COW B-tree node copy-on-insert) |
| 2 | `engine.(*snapshot).copy` | 5.01 | engine-core (snapshot copies) |
| 3 | `btype.(*tree[expiry]).newNode` <- `runReaper` | 2.01 | engine-core (liveness/expiry tree) |
| 4 | `engine.(*Engine).Upsert` | 1.98 | engine-core |
| 5 | `engine.(*Engine).applyBatch` | 0.98 | engine-core (batch path) |

(`bench.parked` driver overhead: 0.66 %.) Effectively 100 % of steady-state allocation is engine-core, dominated by copy-on-write node duplication during batch inserts.

## Conclusion

The workload is allocation-bound in the engine write path, not CPU-bound: COW B-tree node copying (`newNode` <- `copyNode` <- `applyBatch`) accounts for ~89 % of 8.98 GB allocated and drives the 40 GCs, while the read path spends its CPU in comparator invocation and tree search during `Match`. Against the 500-symbol baseline (1 M alerts, 20 000 tps, 6.32 % core, 1 000.4 MiB peak, 50 000 ns/tick), doubling the symbol count at the same alert total kept peak RSS essentially flat (997.7 vs 1 000.4 MiB) and ns/tick identical at the 50 000 pacer floor, but core usage rose from 6.32 % to 7.09 % (~12 % relative). The shallower per-symbol trees did not reduce per-tick cost: engine work scales with total alerts (comparator + node-copy per upsert/reap), not with per-symbol tree depth, and the extra 500 symbols add a small positive marginal cost in per-symbol table/timer bookkeeping. Marginal cost of symbols is therefore slightly positive but negligible at this scale (~0.0015 % of a core per additional symbol).
