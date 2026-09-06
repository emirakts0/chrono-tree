# service/rate-200k — chronod, 1M alerts parked, 500 syms, 200k tps

| metric | value | 20k | 100k |
|---|---|---|---|
| achieved tps | 200000 (48M sent=accepted, 0 dropped) | 20000 | 99999.6 |
| CPU % of one core | 50.27 (120.6 CPU-s / 240 s) | 24.55 | 35.88 |
| RSS plateau→peak MiB | 1454 → 1454 | 1441 | 1464 |
| heap inuse MiB | 521 | 865 | 629 |
| saturated | false | false | false |
| valid | true | true | true |

## CPU attribution
- engine.Match cum 37.8% (comparator 14.0%, tree.search 7.1%)
- protobuf decode 4.7% flat (16.8% cum incl. marshal helpers) — rising with rate as
  per-batch costs become per-tick
- price.Parse 3.1%, pacer/syscall/runtime ~15%
- Per-tick marginal CPU: (50.27−35.88)% / 100k ≈ 0.14 µs — constant marginal cost,
  still no knee at 200k tps.

## Dominant consumer
The service holds 200k tps on half a core with zero drops — not feeder-bound
(the driver hit its own 200k target). Scaling 20k→200k tps is nearly linear
(24.6 → 35.9 → 50.3% of one core) with mild sublinearity; RSS is rate-invariant
(1.45 GB, set by the 1M-alert index + store). On this 12-core box the service
layer's single-process ceiling is far above 200k tps.
