# Performance Campaign: engine and service under load, with pprof attribution

Date: 2026-09-06
Status: approved (design), pending implementation plan

## Context

The dashboard shows throughput, but nothing systematically answers "what does
RAM and CPU cost under different conditions, and what code dominates?"
Existing pieces: `engine/bench_test.go` measures ns/op per Match (sparse and
dense-skip) but not sustained-load RAM, trigger bursts, or CPU attribution;
chronod already serves `/debug/pprof` on its monitoring port
(`internal/server/server.go:169`); chronofeed is a load driver but its symbol
universe is hard-wired to `catalog.Default()` (~500) and its mean-reverting
walk cannot produce a mass simultaneous trigger burst.

The campaign is a **committed, repeatable harness** plus a first-run report:
future changes re-run the same scenarios and compare.

## Decisions

- **Deliverable**: repeatable harness in-repo (drivers + orchestrator +
  validity gates) and results committed under `docs/perf/2026-09-06-campaign/`.
- **Matrix shape**: one-factor-at-a-time (OFAT) spine. Baseline S0 = 1M
  alerts, 500 symbols, 20k ticks/s, zero-fire parked ladder. Each scenario
  moves exactly one factor. Optional capstone corner S8.
- **Per-layer native harness**: engine measured in-process by a dedicated
  driver; service measured as the real chronod process profiled via its
  existing pprof endpoints. Side-by-side profiles isolate what the service
  wrapper (gRPC decode, catalog locks, price parse, store writes, pump) costs
  on top of the engine.
- **Scenario duration**: ~4 min steady load per scenario (plus untimed
  seeding), CPU profile sampled 30 s mid-run, heap at plateau, burst drain
  timed at the end.
- **Machine budget**: 12 cores / 13 GB. Service-layer runs are serial;
  engine-layer runs (~700 MB each) may run two at a time. A hard RSS cap
  (8 GB) aborts a run cleanly rather than OOMing the box.
- chronod runs **broker-less** in service scenarios, via a new
  `-nats-url ""` mode that boots a noop publisher (chronod currently treats
  NATS as a boot dependency, so "no NATS" must mean noop, not unreachable):
  publishes succeed instantly, so the pump's enrich-and-flip path and the
  store's batch writes are measured without a broker in the way.

## Scenarios

| #  | Name        | Change from baseline S0                    |
|----|-------------|--------------------------------------------|
| S0 | baseline    | — (1M alerts, 500 sym, 20k tps, zero fire) |
| S1 | rate-100k   | 100k ticks/s                               |
| S2 | rate-200k   | 200k ticks/s                               |
| S3 | sym-1000    | 1000 symbols                               |
| S4 | sym-2000    | 2000 symbols                               |
| S5 | trickle     | TrickleLadder: steady few-hundred fires/s  |
| S6 | burst-100k  | quiet soak → one gap tick fires 100k       |
| S7 | burst-500k  | same, 500k                                 |
| S8 | corner (opt)| 200k tps × 2000 sym × burst-100k           |

S0–S7 run on **both** layers (engine, service) = 16 runs; S8 optional.
Alerts stay at 1M in every scenario except that burst runs need K cluster
alerts (they sit inside the same 1M budget: K clustered + rest parked).

## Components

1. **`internal/bench` (new package) — shared scenario engine.** Deterministic
   generators used identically by both drivers:
   - `Symbols(n)`: deterministic universe with the catalog's 2/4/8 decimals
     mix, extendable to 1000/2000 (no recompile needed by the service thanks
     to the live registry).
   - Layouts: `ParkedLadder` (targets far from price, zero fires — the
     ingest-cost control), `TrickleLadder` (mean-reversion band, few
     fires/s), `GapCluster(k)` (k alerts clustered at one price; the gap
     tick fires all k at once).
   - A paced tick stream source shared by both drivers.
2. **`scripts/benchengine` — engine-layer driver.** Builds the engine
   directly (engine/ stays frozen; the driver only imports it), seeds via
   `Upsert`, drives `Match` at the target rate, consumes `Triggers()`.
   Writes runtime/pprof CPU/heap/goroutine profiles plus `summary.json`
   (achieved rate, ns/tick, peak RSS from `/proc/self`, burst drain time).
3. **`scripts/benchfeed` — service-layer driver.** Seeds the bbolt store
   directly (chronofeed's `seedAlerts` pattern, scenario layouts), then
   streams to real chronod over gRPC at the target rate, including the gap
   tick. Records FeedStatus accepted/dropped.
4. **`scripts/perf/run.sh` — orchestrator.** Per scenario: launch chronod →
   run benchfeed → mid-run fetch 30s CPU profile + heap profile from
   `/debug/pprof` → record `/stats`, `/proc/<pid>` RSS, db file size →
   teardown → write the results directory. Enforces the serial/parallel
   scheduling and the RSS cap.

## Phasing and measurements

Per scenario: untimed seed → ~4 min steady load (30s CPU profile sampled
mid-run; heap snapshot at plateau) → bursts: gap tick at minute 4, drain
timed to ring-empty (engine) / store-flip completion (service) → teardown,
final heap + RSS.

`summary.json` per run:
- Throughput: ticks sent / accepted / dropped, achieved vs target rate,
  ns/tick.
- RAM: RSS at seed-end / plateau / peak (bursts especially), heap
  inuse_space, GC count, db file size (service).
- CPU: process CPU seconds and % of one core.
- Firing: triggers fired / published / dropped; burst drain latency.

Attribution (the analysis deliverable): per scenario, `go tool pprof -top`
of CPU and heap; top functions classified engine-core / gRPC decode /
catalog+price / store / pump; engine vs service layer compared.

## Results layout

```
docs/perf/2026-09-06-campaign/
  <layer>-<scenario>/        # e.g. service-burst-500k/
    summary.json cpu.pprof heap.pprof goroutine.txt pprof-top.txt run.log
  REPORT.md
```

Raw profiles included by default (~60–100 MB total); if git bloat shows,
shrink to pprof-top + summaries with raw profiles gitignored.

## Run-validity gates

A run counts only if: accepted == sent and drops == 0 (non-malformed
scenarios), firing matches the layout — parked: none; gap: the conservation
law fired + ring_dropped == k (a trigger-ring overflow is a finding, not an
invalidation; lost alerts mean the layout lied); trickle: some but not all —
achieved rate ≥ 95% of target (waived when the feed was genuinely
back-pressured: saturation is itself a finding), and RSS and profiles
present and non-empty. Failures mark the run INVALID with the reason in
summary.json; the orchestrator continues, the report lists them.

## Testing

- `internal/bench` generator tests: determinism, alert counts, symbol
  universe sizes; oracle check that GapCluster(k) + gap tick fires exactly k
  against the real engine (oracle-test pattern).
- End-to-end smoke scenario (1k alerts, 5k tps, 10 s) exercising
  run.sh → chronod → benchfeed → collection → gates, CI-runnable (~30 s).

## Execution

Subagent-driven: implementer + reviewer per build task (bench package →
benchengine → benchfeed → orchestrator), then one subagent per scenario run
(service-layer runs serialized), each producing per-run analysis; a reviewer
subagent re-checks validity gates against raw logs; final REPORT.md
consolidated and reviewed. Total wall-clock ≈ 3–4 h for the 16-run spine on
a quiet box.

## Error handling

- chronod crash or feed error mid-run → INVALID, one retry, then reported.
- pprof fetch failure → INVALID (no silent profile gaps).
- Seed failure → hard stop for that scenario only.
- RSS would exceed the 8 GB cap → run aborts cleanly, marked INVALID.

## Untouched

`engine/` (frozen — drivers only import it), proto/wire, the pending
dashboard-metrics work and the dirty `internal/server/web/` files.

## Risks

- The box is shared (only ~2 GB free at design time): a mistimed parallel
  run could OOM. Mitigated by the RSS cap, serial service runs, and a
  headroom check before launch.
- 200k ticks/s from a single feeder process on 12 cores may be feeder-bound
  (gRPC encode on the driver side); benchfeed reports its own CPU so
  saturation is distinguishable from service saturation.
- Burst drain of 500k triggers through the store's write path may exceed
  the measurement window; the drain timer simply reports its actual value —
  that IS the measurement.
- Committed profiles bloat the repo over repeated campaigns; shrink policy
  above.
