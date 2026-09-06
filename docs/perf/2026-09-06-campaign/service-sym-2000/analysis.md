# service/sym-2000 — chronod, 1M alerts parked, 2000 syms, 20k tps

| metric | value | 500 syms | 1000 syms |
|---|---|---|---|
| achieved tps | 20000 (4.8M sent=accepted, 0 dropped) | 20000 | 20000 |
| CPU % of one core | 25.59 | 24.55 | 25.39 |
| RSS peak MiB | 1484 | 1441 | 1450 |
| heap inuse MiB | 583 | 865 | 596 |
| db size | 817 MB | 815 MB | 801 MB |
| valid | true | true | true |

## Dominant consumer
2000 symbols: +0.20pp over sym-1000 (25.39 → 25.59) — sublinear tail; the
symbol-count axis is essentially flat for the service too (24.55 → 25.39 → 25.59
across 4× symbols). RSS peak +34 MiB vs baseline (~2000 registry entries plus
run-to-run variance). Confirms at both layers: capacity planning keys on alert
count (≈630 B engine + ~0.9 KB persisted per alert), not symbol count.
