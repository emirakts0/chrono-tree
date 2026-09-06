# service/burst-100k — chronod, 1M alerts (100k gap-cluster + 900k parked), 20k tps

| metric | value | service parked baseline |
|---|---|---|
| achieved tps | 20000 (4.8M sent=accepted, 0 dropped) | 20000 |
| CPU % of one core | 25.59 | 24.55 |
| RSS peak MiB | 1499 | 1441 |
| cluster delivered / ring-dropped | 65,600 / 34,400 | — |
| store flips (triggered) | 65,600 | 0 |
| drain (gap → all delivered + flipped) | **15.1 s** | — |
| valid | true (conservation: 65,600+34,400 = 100,000) | true |

## The burst-capacity finding
A single gap tick fires all 100k cluster alerts inside one Match; the engine's
trigger ring holds 65,536, and chronod's pump (64/batch, store round-trip per
batch) cannot drain fast enough to keep the ring from overflowing: **34,400
triggers (34.4%) are lost to ring overflow before delivery.** The engine-layer
run lost 0 — its dedicated consumer drains concurrently during the push — so
the loss is specific to the production pump's batch-then-store-then-flip pace.
The 65,600 delivered triggers were enriched, published (noop) and flipped in
the store within 15.1 s of the gap tick.

## Dominant consumer
Burst handling at the service layer is pump-bound: 15 s of store read/write
work to deliver ~65k triggers (≈4.3k flips/s sustained). CPU stays at parked
levels (25.6%) because the drain happens after the measured load window — the
cost shows as drain latency, not CPU. RSS peak +58 MiB over parked (delivered
batch + flip buffers).
