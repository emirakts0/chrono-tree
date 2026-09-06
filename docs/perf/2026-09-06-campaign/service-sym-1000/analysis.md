# service/sym-1000 — chronod, 1M alerts parked, 1000 syms, 20k tps

| metric | value | service baseline (500 syms) |
|---|---|---|
| achieved tps | 20000 (4.8M sent=accepted, 0 dropped) | 20000 |
| CPU % of one core | 25.39 | 24.55 |
| RSS seed→plateau→peak MiB | 1358 → 1449 → 1450 | 1331 → 1441 → 1441 |
| heap inuse MiB | 596 | 865 |
| db size | 801 MB | 815 MB |
| valid | true | true |

## Dominant consumer
Doubling the symbol count at fixed 1M alerts costs +0.84pp of one core
(24.55 → 25.39) — the same near-linear, tiny symbol scaling the engine layer
showed (6.32 → 7.09), plus the wrapper's fixed per-tick costs. RSS is flat:
the registry's learned entries (~1000 map entries, tens of KB) are invisible,
and the db is marginally smaller (shorter per-symbol trees, same records).
Symbol count is not a capacity axis for either layer; alert count is.
