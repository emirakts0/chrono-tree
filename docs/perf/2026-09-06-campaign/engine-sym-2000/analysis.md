# engine/sym-2000 — 1M alerts, 2000 symbols, 20k tps, parked (zero-fire)

| metric | value | baseline (500 syms) | sym-1000 |
|---|---|---|---|
| achieved tps | 19999.98 / 20000 | 19999.95 | 19999.99 |
| ns/tick (pacer floor) | 50000.0 | 50000.1 | 50000.0 |
| RSS seed→plateau→peak MiB | 988 → 1012 → 1016 | 999 → 698 → 1000 | 986 → ? → 998 |
| heap inuse MiB | 543 | 520 | — |
| GC cycles | 34 | 45 | — |
| CPU % of one core | 7.76 | 6.32 | 7.09 |
| fired | 0 (parked) | 0 | 0 |
| valid | true | true | true |

## CPU attribution (top of 240s profile, 18.04s samples = 7.52% of one core)
- `btype.Table.initCompare.func2` (entry comparator) 27.6% flat — engine-core
- `btype.tree.search` 20.2% flat / 48.5% cum — engine-core (Match path)
- `runtime.nanotime` 13.1% — pacer clock (driver overhead)
- `syscall.Syscall6` 6.9% — pacer sleep (driver overhead)
- Remainder: Ascend/Descend comparators + scheduler — engine-core + runtime

## Heap attribution (alloc_space, 8.1 GB total)
- `btype.tree.newNode` (entry tables) 87.6% — COW node copies under `Engine.applyBatch`
- `snapshot.copy` 5.6% — engine-core
- expiry-table `newNode` 2.2%, `Upsert` 2.1% — seed-time
- `applyBatch` cum 94.4%

## Dominant consumer
Same shape as every engine-layer scenario so far: RAM is dominated by copy-on-write
B-tree node churn during the 1M-alert seed (87.6% of all allocation), CPU during load
is dominated by comparator + tree-search work inside `Match`. Doubling symbols again
at fixed 1M alerts adds ~0.67pp of one core (6.32 → 7.09 → 7.76) — near-linear in
symbol count, ~0.0007% core/symbol, with RSS peak flat (~1000 MiB): total alert count,
not tree depth or symbol count, sets memory. At 20k tps the engine still uses well
under 8% of one core; the pacer's clock/sleep frames remain visible in the profile
(~20%) because the engine is essentially idle at this rate.
