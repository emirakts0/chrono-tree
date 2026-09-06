# engine/trickle — 1M alerts (trickle ladder, band 0.6), 500 syms, 20k tps

| metric | value | parked baseline |
|---|---|---|
| achieved tps | 19999.96 | 19999.95 |
| CPU % of one core | 7.25 | 6.32 |
| RSS seed→plateau→peak MiB | ~999 → 765 → 1049 | 999 → 698 → 1000 |
| heap inuse MiB | 520 | 520 |
| GC cycles | 45 | 45 |
| fired | 138,298 (13.8% of ladder) | 0 |
| valid | true | true |

## CPU attribution
- `runtime.nanotime` 17.9% + `time.runtimeNow` 8.4% + `Syscall6` 9.4% + `futex` 7.6% — pacer/driver ~43%
- `btype` comparator 16.4% + `tree.search` 6.7% flat (23.1% cum) — engine-core Match work
- Firing work adds ~0.9pp of one core over parked (6.32 → 7.25) for ~575 fires/s sustained.

## Heap attribution
- COW `newNode` (seed-time) still dominates cumulative alloc (~87% of 9.6 GB under applyBatch);
  firing adds no significant steady-state allocation site of its own (trigger ring is preallocated).

## Dominant consumer
Firing is nearly free at the engine layer: +0.9pp of one core at ~575 fires/s, no new
allocation sites, RSS peak +49 MiB over parked (trigger ring churn + tree-removal COW).
The ladder depletes fastest early (~70% of fires in the first quarter) — inherent to a
uniform ladder under a mean-reverting walk; documented in REPORT.md caveats.
