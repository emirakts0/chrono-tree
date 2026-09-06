# service/trickle — chronod, 1M alerts trickle (band 0.6), 500 syms, 20k tps

| metric | value | service parked baseline | engine trickle |
|---|---|---|---|
| achieved tps | 20000 (4.8M sent=accepted, 0 dropped) | 20000 | 20000 |
| CPU % of one core | **83.33** | 24.55 | 7.25 |
| RSS plateau→peak MiB | 1504 → 1522 | 1441 → 1441 | → 1049 |
| heap inuse MiB | 697 | 865 | 520 |
| fired | 138,298 (13.8% of ladder) | 0 | 138,298 |
| ring_dropped | 0 | 0 | 0 |
| valid | true | true | true |

## The firing pipeline is the service's dominant CPU cost
Fires cross-layer exactly (138,298 on both layers — the deterministic walk and
dims parity hold at campaign scale). But delivering them costs the wrapper
+57.8pp of one core over its parked baseline (25.6 → 83.3) for ~575 fires/s:
per fire the pump does a bbolt BatchGet, builds the enriched payload, a noop
publish, and a MarkTriggeredBatch write tx — ~1.7 ms of CPU per trigger across
the batch path. The engine layer's own firing cost was +0.9pp for the same
fire rate: virtually all delivery cost lives in the service wrapper.

## Dominant consumer
At steady trickle the service runs at ~83% of one core: ~31% tick ingestion
(engine Match + parse + decode), ~70%-of-a-core-equivalent trigger delivery
(pump + store reads/writes). Trigger *delivery*, not matching, is what caps
service-layer capacity under sustained firing.
