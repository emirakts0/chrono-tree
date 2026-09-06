# service/burst-100k (postopt) — chronod, 1M alerts (100k gap-cluster + 900k parked), 20k tps

| metric | postopt | before (campaign) | Δ |
|---|---|---|---|
| achieved tps | 20000 (4.8M sent=accepted, 0 dropped) | 20000 | — |
| CPU % of one core | 25.36 | 25.59 | −0.2pp |
| RSS peak MiB | 1590 | 1499 | +91 MiB |
| cluster delivered / ring-dropped | **100,000 / 0** | 65,600 / 34,400 | +34,400 delivered, loss 34.4% → 0% |
| drain (gap → all delivered + flipped) | **2.82 s** | 15.11 s | 5.4× faster |
| valid | true (conservation: 100,000 + 0 = 100,000) | true | — |

## Burst-loss finding: fixed

The 100k cluster now crosses the service boundary intact. Two levers: the
trigger ring default grew 65,536 → 1,048,576 slots (`f1eeee0`), so the ring
absorbs the whole cluster the engine's Match pushes in one gap tick; and the
pipelined pump (`2b072f7`, 4096-wide enrich overlapping flip) drains it at
~35k triggers/s instead of the batch-then-store-then-flip ~4.4k/s. Loss drops
34.4% → 0 and drain drops 15.1 s → 2.8 s.

The +91 MiB RSS peak over before is the price: ring slots (1M × ~276 B) plus
delivered-batch/flip buffers. The before-run lost triggers silently but was
leaner; the fix trades memory for correctness.

## Dominant consumer

Same as before at this scale: parked-ladder matching (comparator + search)
with the drain mostly outside the CPU window; the pump store path shows up in
the drain, not the load-phase mean.
