# engine/rate-200k — 2026-09-06 campaign

Run: `run.sh engine rate-200k docs/perf/2026-09-06-campaign` — VALID (exit 0).
Layout `parked`, 1,000,000 alerts, 500 symbols, cluster 0, target rate 200,000 ticks/s, 240 s wall.

## Headline numbers

| Metric | Value |
|---|---|
| Achieved tps | 199,999.5 / 200,000 target (100.000%) |
| ns/tick (pacing) | 5,000.01 |
| RSS seed | 993.7 MiB |
| RSS plateau | 705.4 MiB |
| RSS peak | 994.8 MiB |
| Heap inuse | 525.0 MiB |
| GC count | 45 (over 240 s) |
| CPU (one core) | 19.19% |
| Triggers fired | 0 |
| Drain ms | -1 (n/a, no drain in this scenario) |
| Saturated | false |
| Valid | true |

48,000,000 ticks sent / accepted, 0 dropped; rss_seed 1,041,281,024 B, rss_plateau 739,667,968 B, rss_peak 1,042,657,280 B, heap_inuse 550,633,472 B, cpu 46.05 s over 240 s.

## Top CPU functions (pprof-top.txt, flat, 45.50 s samples)

| # | Function | flat % | Class |
|---|---|---|---|
| 1 | btype.Table[entry].initCompare.func2 | 28.55% | engine-core (price-index comparator) |
| 2 | btype.tree[entry].search | 12.81% | engine-core (B-tree search) |
| 3 | btype.tree[entry].Descend | 8.70% | engine-core (range descend) |
| 4 | engine.entryKeyMax | 5.89% | engine-core |
| 5 | btype.Table[entry].Ascend | 4.57% | engine-core (range ascend) |
| 6 | runtime.nanotime | 4.53% | runtime (driver pacing clock) |
| 7 | engine.(*Engine).Match | 3.80% flat / 82.55% cum | engine-core |
| 8 | btype.Table[entry].Descend | 3.60% | engine-core |
| 9 | engine.makeEntryCompare.func1 | 2.57% | engine-core |
| 10 | btype.tree[entry].nodeAscend | 1.89% | engine-core |

Roughly 80%+ of CPU is engine-core (btype B-tree ops + comparators under `Engine.Match`), ~7-9% runtime/driver overhead (nanotime, futex, Syscall6, time.Now, rand.NormFloat64, bench Market.Step), and essentially zero GC in the top list — 45 GCs in 240 s keep collection invisible (<1% of samples).

## Top heap sites (alloc_space, 9.49 GB total)

| # | Site | flat | Class |
|---|---|---|---|
| 1 | btype.tree[entry].newNode (via Table.Insert / cow0) | 8.44 GB (88.9%) | engine-core — COW B-tree node copies on insert/update path |
| 2 | engine.(*snapshot).copy | 0.46 GB (4.9%) | engine-core — snapshot copies |
| 3 | btype.tree[expiry].newNode (via Table.Insert) | 0.19 GB (2.0%) | engine-core — expiry index maintenance |
| 4 | engine.(*Engine).Upsert | 0.18 GB (1.9%) | engine-core |
| 5 | engine.(*Engine).applyBatch | 0.10 GB (1.1%) | engine-core — parent of #1 (flusher path) |

Allocation is >94% engine-core, dominated by copy-on-write B-tree node allocation in the flusher's applyBatch; nothing from GC, runtime, or driver in the top sites.

## Conclusion

Dominant consumer is the engine-core COW B-tree: ~80% of CPU sits in comparators/search/descend under `Engine.Match`, and ~89% of allocation is `btype.tree[entry].newNode` from copy-on-write inserts in `applyBatch` — matching the structural RSS profile (peak 994.8 MiB ≈ 2x db-equivalent footprint, unchanged from baseline's 1000.4 MiB and rate-100k's 999.8 MiB, since memory is set by the 1M-alert index, not the rate). CPU does NOT scale linearly with rate: 20k→100k→200k tps gives 6.32%→13.33%→19.19% of one core, i.e. 10x rate costs 3.0x CPU; core-seconds per tick fall from ~3.16 µs (20k) to ~1.33 µs (100k) to ~0.96 µs (200k), so the curve bends downward (sublinear) with the marginal cost between 100k and 200k at ~0.59 µs/tick — fixed overhead (flusher/reaper floor of roughly 4-7%) amortizes as rate rises. The engine is nowhere near saturation at 200k tps (~0.19 of one core) and ns/tick stays at the paced 5,000 ns (baseline 50,000, rate-100k 10,000 — pure pacing, not engine cost).
