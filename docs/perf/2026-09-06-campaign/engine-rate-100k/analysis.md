# engine/rate-100k — analysis

Run: 2026-09-06, parked layout, 1,000,000 alerts, 500 symbols, target rate 100,000 ticks/s,
240 s wall (24,000,000 ticks sent/accepted, 0 dropped, not saturated). Verdict: `RUN engine/rate-100k: VALID`.

## Headline numbers

| Metric | Value |
|---|---|
| Achieved tps | 99,999.6 (target 100,000) |
| ns/tick | 10,000.04 (pacing interval = 1/rate) |
| RSS seed | 998.1 MiB |
| RSS plateau | 675.3 MiB |
| RSS peak | 999.8 MiB |
| Heap inuse | 521.7 MiB |
| GC count | 45 |
| CPU (% of one core) | 13.33% (31.99 CPU-s / 240.0 s wall) |
| Triggers fired | 0 |
| Drain | -1 ms (n/a, no triggers) |
| Valid | true |

## Top 10 CPU functions (pprof-top.txt)

| # | flat | flat% | Function | Class |
|---|---|---|---|---|
| 1 | 8.20s | 26.12% | `btype.(*Table[entry]).initCompare.func2` | engine-core (index comparator) |
| 2 | 4.77s | 15.20% | `btype.(*tree[entry]).search` | engine-core (tree lookup) |
| 3 | 2.90s | 9.24% | `runtime.nanotime` | runtime (clock reads) |
| 4 | 1.85s | 5.89% | `btype.(*tree[entry]).Descend` | engine-core (scan) |
| 5 | 1.39s | 4.43% | `engine.entryKeyMax` | engine-core (key bound) |
| 6 | 1.23s | 3.92% | `internal/runtime/syscall/linux.Syscall6` | runtime / driver-overhead (pacing syscalls) |
| 7 | 1.02s | 3.25% | `engine.(*Engine).Match` | engine-core (dispatch; cum 71.9%) |
| 8 | 0.90s | 2.87% | `runtime.futex` | runtime (scheduler) |
| 9 | 0.87s | 2.77% | `btype.(*Table[entry]).Ascend` | engine-core (scan) |
| 10 | 0.62s | 1.98% | `time.runtimeNow` | runtime / driver-overhead (tick clock) |

Roughly ~80% of flat CPU is engine-core (btype tree search/compare/scan + engine.Match), ~17%
runtime (nanotime, futex, syscalls), with only a sliver attributable to bench-driver overhead
(`bench.(*Market).Step` 1.24%, `bench.EngineTick` 0.48%).

## Top 5 heap sites (alloc_space, 9,697 MB total allocated over 240 s)

| # | flat | flat% | Site | Class |
|---|---|---|---|---|
| 1 | 8,653 MB | 89.23% | `btype.(*tree[entry]).newNode` via `Table.Insert`/`copyNode` under `applyBatch` | engine-core (copy-on-write tree nodes, price index) |
| 2 | 481 MB | 4.96% | `engine.(*snapshot).copy` | engine-core (snapshot copies) |
| 3 | 196 MB | 2.02% | `btype.(*tree[expiry]).newNode` under `runReaper` | engine-core (expiry index inserts) |
| 4 | 182 MB | 1.87% | `engine.(*Engine).Upsert` | engine-core |
| 5 | 91 MB | 0.94% | `engine.(*Engine).applyBatch` | engine-core |

All five are engine-core; allocation is dominated (~89%) by copy-on-write node allocation in the
btype entry tree during batch apply. GC keeps it in check: 45 GCs total and steady-state heap
inuse of ~522 MiB despite ~40 MB/s of allocation churn.

## Dominant consumer / baseline comparison

The dominant consumer is the engine core itself — specifically the btype price-index tree
(comparator, search, and copy-on-write node allocation), which accounts for the top CPU entry,
~43% cumulative tree search cost, and 89% of all allocated bytes. Against the baseline
(20,000 tps: 6.32% of one core, 15.17 CPU-s for 4.8M ticks, RSS peak 1000.4 MiB, ns/tick 50,000),
raising the rate 5x to 100,000 tps cost only 2.1x CPU (13.33%, 31.99 CPU-s for 24M ticks) while
memory was flat: RSS peak 999.8 vs 1000.4 MiB, heap inuse 521.7 vs 520.0 MiB, and identical GC
count (45). Per-tick CPU actually fell from ~3.16 µs to ~1.33 µs, and the marginal cost of the
extra 19.2M ticks was ~0.88 µs/tick (16.8 CPU-s), so the 5x rate delta is CPU-cheap and carries
no memory penalty — the run is far from saturation at 13% of one core.
