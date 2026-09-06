# service/trickle (postopt) — chronod, 1M alerts trickle (band 0.6), 500 syms, 20k tps

| metric | postopt | before (campaign) | Δ |
|---|---|---|---|
| achieved tps | 20000 (4.8M sent=accepted, 0 dropped) | 20000 | — |
| CPU % of one core | **75.73** | 83.33 | −7.60pp (−9.1%) |
| RSS plateau→peak MiB | 1384 → 1485 | 1504 → 1522 | −37 MiB peak |
| heap inuse MiB | 557 | 697 | −140 MiB |
| fired / published | 138,298 / 138,298 | 138,298 / 138,298 | identical |
| ring_dropped | 0 | 0 | — |
| valid | true | true | — |

## Firing-CPU finding: partly fixed

The pipelined pump (commit `2b072f7` — 4096-wide enrich overlapping flip)
cuts sustained-firing CPU by 7.6pp of one core (83.3 → 75.7) while delivering
the exact same fire set (138,298 — cross-layer parity with the engine holds).
Delivery is no longer strictly serialized behind the store flip: enrichment
for the next batch overlaps the previous batch's MarkTriggeredBatch tx,
hiding part of the ~1.7 ms/trigger CPU the before-run measured.

Still ~51pp above the parked baseline (24.6%), so trigger delivery remains
the service's dominant cost — the fix improves the slope, it does not remove
the frontier. Remaining pump work (BatchGet enrichment, payload build, store
write) is inherent to delivery.

## Dominant consumer

Profile shape is unchanged: the syscall path (store tx + publish) dominates
flat samples, then comparator/search for matching. Matching stays cheap; the
pump path is where the ~0.76 of a core goes.
