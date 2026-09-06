# service/rate-100k — chronod, 1M alerts parked, 500 syms, 100k tps

| metric | value | service baseline (20k) |
|---|---|---|
| achieved tps | 99999.6 (24M sent=accepted, 0 dropped) | 20000 |
| CPU % of one core | 35.88 (86.1 CPU-s / 240 s) | 24.55 |
| RSS plateau→peak MiB | 1464 → 1464 | 1441 → 1441 |
| heap inuse MiB | 629 | 865 |
| saturated | false | — |
| valid | true | true |

## CPU attribution (30 s mid-load profile)
- `ingestTick` cum 60.9% — the whole per-tick path
  - inside it: `engine.Match` cum 37.5% (comparator 15.3%, tree.search 6.0%)
  - wrapper share: price.Parse 5.1% flat, protobuf decode ~4-5% flat (unmarshalPointerEager 3.9%),
    catalog lookups + counters in the remainder
- Runtime (nanotime/syscall/futex/mallocgc) ~12%

## Dominant consumer
At 5× baseline rate the service scales to 35.9% of one core (baseline 24.6%):
the marginal cost of a tick is ~0.14 µs, sublinear because fixed per-batch costs
(gRPC framing, batch loops) amortize. Engine Match is ~2/3 of the per-tick cost;
the wrapper's parse+decode+catalog ~1/3. Zero drops at 100k tps — no saturation.
