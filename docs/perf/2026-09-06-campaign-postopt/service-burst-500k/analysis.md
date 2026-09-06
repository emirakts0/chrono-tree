# service/burst-500k (postopt) — chronod, 1M alerts (500k gap-cluster + 500k parked), 20k tps

| metric | postopt | before (campaign) | Δ |
|---|---|---|---|
| achieved tps | 20000 (4.8M sent=accepted, 0 dropped) | 20000 | — |
| CPU % of one core | 23.65 | 24.23 | −0.6pp |
| RSS peak MiB | 1770 | 1494 | +276 MiB |
| cluster delivered / ring-dropped | **500,000 / 0** | 65,664 / 434,336 | +434,336 delivered, loss 86.9% → 0% |
| drain (gap → all delivered + flipped) | **9.07 s** | 14.91 s | 1.6× faster |
| valid | true (conservation: 500,000 + 0 = 500,000) | true | — |

## Burst-loss finding: fixed at 7.6× the old ring capacity

The worst before-case (86.9% of a 500k cluster silently ring-lost) now
delivers every trigger: `fired + ring_dropped == cluster` holds exactly with
`ring_dropped = 0`. The 1,048,576-slot ring (`f1eeee0`) absorbs the whole
500k push, and the pipelined pump (`2b072f7`) drains at ~55k triggers/s —
12.6× the before-run's effective ~4.4k/s (which only ever delivered ring
capacity anyway).

Memory is the trade: peak RSS 1,494 → 1,770 MiB (+276 MiB ≈ 1M ring slots at
~276 B plus 500k delivered/flip buffers, plateau 1,526 MiB). Drain is 9.1 s
of store tx work after the gap tick — latency, not loss, and CPU stays at
parked levels (23.6%).

## Dominant consumer

Drain-phase store transactions (BatchGet + MarkTriggeredBatch); load-phase
CPU is matching-dominated as in the parked baseline.
