# service/burst-500k — chronod, 1M alerts (500k gap-cluster + 500k parked), 20k tps

| metric | value | burst-100k (service) |
|---|---|---|
| achieved tps | 20000 (4.8M sent=accepted, 0 dropped) | 20000 |
| CPU % of one core | 24.23 | 25.59 |
| RSS peak MiB | 1494 | 1499 |
| cluster delivered / ring-dropped | 65,664 / 434,336 | 65,600 / 34,400 |
| store flips (triggered) | 65,664 | 65,600 |
| drain (gap → all delivered + flipped) | 14.9 s | 15.1 s |
| valid | true (65,664+434,336 = 500,000) | true |

## The burst-capacity finding, quantified
Ring capacity (65,536) is the hard ceiling on simultaneous burst delivery at
the service layer: delivered triggers are flat (~65.6k) whether the burst is
100k or 500k — everything above capacity is lost (34.4% at 100k, 86.9% at
500k). The pump delivers and flips its ~65.6k in ~15 s regardless of burst
size (~4.4k flips/s), so drain latency is burst-size-invariant; loss ratio is
not. Mitigation directions (for the report, not this campaign): larger ring,
or a pump that streams BatchGet while the ring drains.

## Dominant consumer
Same shape as burst-100k: the cost of a burst is post-match pump work
(store reads + flip writes), capping delivery at ~4.4k triggers/s with a
65,536-trigger absorption ceiling. Ingest CPU (24.2%) is unaffected by burst size.
