# Benchmarks

AMD Ryzen 5 5600H (6 cores / 12 threads; g12 is SMT), Linux/amd64, go1.27.0.
Medians of `count=3`. The hot path allocates nothing; Upsert is 1 alloc/op.

```
go test ./engine -run '^$' -bench='SweepHold(100k|1M|5M)(Dims)?$' -benchtime=1x -benchmem -count=3
go test ./engine -run '^$' -bench='Sweep(100k|1M|5M)(Dims)?$'    -benchtime=1x -benchmem -count=3
go test ./engine -run '^$' -bench='Upsert' -benchmem -count=3
```

## SweepHold (fires pinned at 50k, population scales 100k → 5M)

| Benchmark | g1 ns/op | g4 ns/op | g12 ns/op |
|---|---:|---:|---:|
| SweepHold100k | 559.5 | 130.4 | 103.2 |
| SweepHold1M | 584.8 | 159.4 | 114.0 |
| SweepHold5M | 736.2 | 163.2 | 115.3 |
| SweepHold100kDims | 327.5 | 87.7 | 61.6 |
| SweepHold1MDims | 434.0 | 123.7 | 67.4 |
| SweepHold5MDims | 553.7 | 149.4 | 80.8 |

## Sweep (fires proportional to population, 50–70%)

| Benchmark | g1 ns/op | g4 ns/op | g12 ns/op |
|---|---:|---:|---:|
| Sweep100k | 616.6 | 154.2 | 105.9 |
| Sweep1M | 4125 | 530.5 | 756.9 |
| Sweep5M | 11306 | 2945 | 3801 |
| Sweep100kDims | 343.0 | 98.0 | 59.4 |
| Sweep1MDims | 1347 | 365.4 | 230.6 |
| Sweep5MDims | 5506 | 1424 | 1119 |

## Control plane

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Upsert | 552.6 | 1363 | 1 |
| UpsertReplace | 459.4 | 1177 | 1 |
| UpsertParallel | 659.6 | 1518 | 1 |
| Cancel | 324.5 | 484 | 0 |
| FireRemoval | 100.9 | 111 | 0 |
| SyncLatency | 9374 | 17634 | 8 |

UpsertParallel serializes on the single engine mutex, so 12 goroutines yield ~1.5M ops/s aggregate vs ~1.8M single-goroutine.

## Mutation queue

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| MutQMutex | 12.9 | 0 | 0 |
| MutQMutexContended | 32.9 | 0 | 0 |
| MutQChan | 62.6 | 0 | 0 |
| MutQChanContended | 72.4 | 0 | 0 |
| MutQUMPSC | 55.2 | 0 | 0 |
| MutQUMPSCContended | 64.4 | 40 | 0 |
