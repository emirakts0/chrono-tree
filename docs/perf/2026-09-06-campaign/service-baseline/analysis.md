# service/baseline — chronod, 1M alerts parked, 500 syms, 20k tps, broker-less

| metric | value | engine baseline | wrapper delta |
|---|---|---|---|
| achieved tps | 20000 (4800000 sent=accepted, 0 dropped) | 19999.95 | — |
| RSS seed→plateau→peak MiB | 1331 → 1441 → 1441 | 999 → 698 → 1000 | +~440 MiB |
| heap inuse MiB | 865 | 520 | +345 |
| db size | 815 MB | — | — |
| CPU % of one core | 24.55 (58.9 CPU-s over 240 s) | 6.32 | +18.2pp |
| fired | 0 | 0 | — |
| valid | true | true | — |

## What the wrapper adds
Same 1M-alert engine core, plus: gRPC decode of 4.8M ticks, two catalog map
lookups per tick (venue/tier) + symbol lookup, price.Parse of bid+ask strings,
dims construction, stats/metrics counters, and (0 here) pump/publish/store-flip.
That bundle roughly quadruples CPU (6.32 → 24.55% of one core ≈ 1.23 µs/tick
extra) and adds ~440 MiB RSS (bbolt mmap counts toward RSS; Go heap +345 MiB).

## Dominant consumer
CPU: per-tick protocol + parsing + catalog work (engine Match itself is ~26% of
what the engine-layer profile showed). RAM: the engine index (~520 MiB heap) +
persisted store structures — RAM remains alert-count-bound, exactly the structural
RSS≈2×db profile seen in the 1M-alert memory work.
