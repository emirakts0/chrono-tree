# engine/baseline — 2026-09-06 campaign

Scenario: parked layout, 1,000,000 alerts, 500 symbols, cluster 0, target rate 20,000 ticks/s, 240 s wall.
Result: `RUN engine/baseline: VALID` (4,800,000 ticks sent = accepted, 0 dropped, not saturated).

## Headline numbers

| Metric | Value |
|---|---|
| Achieved tps (target 20,000) | 19,999.95 (100.00%) |
| ns/tick | 50,000.1 |
| RSS seed | 999.3 MiB |
| RSS plateau | 698.0 MiB |
| RSS peak | 1,000.4 MiB |
| Heap inuse | 519.7 MiB |
| GC count | 45 |
| CPU (% of one core) | 6.32% |
| Triggers fired | 0 |
| Drain | -1 ms (n/a — nothing to drain, 0 fired) |
| Valid | true |

## Top 10 CPU functions (pprof cpu, 14.59 s samples over 240 s)

| # | flat | flat% | Function | Class |
|---|---|---|---|---|
| 1 | 3.28s | 22.48% | `runtime.nanotime` | runtime |
| 2 | 2.09s | 14.32% | `btype.(*Table[entry]).initCompare.func2` | engine-core |
| 3 | 1.65s | 11.31% | `internal/runtime/syscall/linux.Syscall6` | runtime |
| 4 | 1.16s | 7.95% | `btype.(*tree[entry]).search` | engine-core |
| 5 | 0.82s | 5.62% | `runtime.futex` | runtime |
| 6 | 0.78s | 5.35% | `time.runtimeNow` | runtime |
| 7 | 0.54s | 3.70% | `btype.(*tree[entry]).Descend` | engine-core |
| 8 | 0.38s | 2.60% | `btype.(*Table[entry]).Ascend` | engine-core |
| 9 | 0.28s | 1.92% | `engine.entryKeyMax` | engine-core |
| 10 | 0.20s | 1.37% | `engine.makeEntryCompare.func1` | engine-core |

Note: `nanotime`/`runtimeNow`/`Syscall6` clock reads are runtime code but are driven by the tick-pacing loop — the engine only needs ~6% of one core at 20k tps, so clock/scheduler overhead dominates samples. No GC frames appear in the top 25; `Engine.Match` has 39.75% cumulative but only 1.30% flat.

## Top 5 heap allocation sites (alloc_space, 9.43 GB total over the run)

| # | flat | flat% | Site | Class |
|---|---|---|---|---|
| 1 | 8.40GB | 89.10% | `btype.(*tree[price/id/dims/validity entry]).newNode` — via `Engine.applyBatch` -> `Table.Insert` -> COW (`cowRoot`/`cowChild`/`copyNode`) | engine-core |
| 2 | 0.47GB | 4.97% | `engine.(*snapshot).copy` | engine-core |
| 3 | 0.19GB | 1.96% | `btype.(*tree[expiry entry]).newNode` — reaper expiry table | engine-core |
| 4 | 0.18GB | 1.94% | `engine.(*Engine).Upsert` | engine-core |
| 5 | 0.10GB | 1.01% | `engine.(*Engine).applyBatch` (cum 8.97GB / 95.08%) | engine-core |

No driver-overhead site exceeds 0.7% (`internal/bench.parked` at 0.06GB); no runtime/GC allocator site in the top 5. This layer has no gRPC/store/pump.

## Dominant consumer

CPU in this scenario is dominated by engine-core B-tree work (comparators, search, ascend/descend, ~30% flat) with a large runtime share (~45%: clock reads via `nanotime`/`runtimeNow`/`Syscall6` plus scheduler `futex`/`stealWork`) that is an artifact of pacing 20k ticks/s across 240 s on an engine that only needs ~6% of one core. RAM is entirely structural: 9.4 GB of allocation churn comes from copy-on-write B-tree node copies in `applyBatch` (89% at `newNode`), settling to ~520 MiB heap inuse / ~1,000 MiB RSS peak for 1M alerts, with only 45 GC cycles because freed COW nodes are recycled cheaply. The one anomaly worth noting is that RSS peak (1,000.4 MiB) is essentially the seed spike (999.3 MiB): the runtime never returns the seed high-water mark to the OS even though the steady-state plateau is only 698 MiB — RSS tracks the high-water mark, not live heap.
