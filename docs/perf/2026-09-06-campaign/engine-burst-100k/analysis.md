# engine/burst-100k — 1M alerts (100k gap-cluster + 900k parked), 500 syms, 20k tps

| metric | value | parked baseline |
|---|---|---|
| achieved tps | 19999.96 | 19999.95 |
| CPU % of one core | 7.56 | 6.32 |
| RSS seed→plateau→peak MiB | ~988 → 682 → 1012 | 999 → 698 → 1000 |
| GC cycles | 36 | 45 |
| fired / ring_dropped | 100,000 / 0 | 0 |
| drain (ring-empty after gap tick) | 12 ms | — |
| valid | true | true |

## CPU attribution
Engine-core Match work dominates: comparator 30.5% + tree.search 15.3% (47.4% cum);
pacer clock/sleep ~23%. Profile is the 4-minute load phase (single fired tick lands
in-window but is negligible); the burst itself costs almost nothing here.

## Dominant consumer
A 100k simultaneous trigger burst is a non-event for the engine: one tick match,
100k CAS-gated fires pushed synchronously, ring drained by the consumer in 12 ms,
zero drops (ring capacity 65,536 handles it because the consumer drains concurrently).
CPU +1.2pp over parked for carrying 100k clustered rungs in one symbol's trees.
