# engine/burst-500k — 1M alerts (500k gap-cluster + 500k parked), 500 syms, 20k tps

| metric | value | burst-100k |
|---|---|---|
| achieved tps | 19999.96 | 19999.96 |
| CPU % of one core | 7.50 | 7.56 |
| RSS seed→plateau→peak MiB | ~988 → ? → 1021 | → 682 → 1012 |
| GC cycles | 20 | 36 |
| fired / ring_dropped | 500,000 / 0 | 100,000 / 0 |
| drain (ring-empty after gap tick) | 44 ms | 12 ms |
| valid | true | true |

## CPU attribution
Comparator 24.2% + tree.search 14.1% (39.4% cum) — engine-core; pacer ~24%.
A 3-goroutine process at rest (consumer parked, flusher/reaper idle in the
goroutine profile) — the burst leaves no residue.

## Dominant consumer
Half a million simultaneous triggers drain in 44 ms with zero ring drops: the
consumer's concurrent PopBatch keeps the 65,536-slot ring from ever filling.
Drain scales ~linearly with burst size (12 ms @ 100k → 44 ms @ 500k ≈ 90 ns/trigger).
RSS peak is flat vs every other scenario — bursts do not raise the memory envelope.
