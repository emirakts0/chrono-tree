# Performance Campaign Report — 2026-09-06

## Setup

- Host: 12 logical cores, 13 GB RAM, Linux. Runs strictly serial; box otherwise quiet.
- Software: Go 1.27, commit `aa1a774` + harness fixes through `run.sh`/`benchfeed` fixes on 2026-09-06 (see git log). `engine/` frozen; chronod ran broker-less (`-nats-url ""` → noop publisher).
- Method: OFAT spine. Baseline = 1M alerts / 500 symbols / 20k ticks/s / zero-fire parked ladder. Each scenario moves one factor. 4 min steady load per run; 30 s CPU profile mid-run (service) or whole-run (engine); 1 Hz `/stats` sampling (service); RAM from VmRSS/VmHWM + Go heap.
- Layers: **engine** = in-process driver (`scripts/benchengine`); **service** = real chronod + gRPC feeder (`scripts/benchfeed`), results via 1 Hz stats windowed to the load phase.
- Validity gates: all 16 runs VALID — sent == accepted, zero tick drops, firing matches layout (parked: 0; trickle: 138,298 on both layers; bursts: conservation `fired + ring_dropped == cluster` exact on both layers).

## Engine layer

| scenario | achieved tps | CPU % 1 core | RSS peak MiB | fired | ring drops | drain |
|---|---|---|---|---|---|---|
| baseline (20k tps, 500 sym) | 20,000 | 6.3 | 1,000 | 0 | 0 | — |
| rate-100k | 100,000 | 13.3 | 1,000 | 0 | 0 | — |
| rate-200k | 200,000 | 19.2 | 995 | 0 | 0 | — |
| sym-1000 | 20,000 | 7.1 | 998 | 0 | 0 | — |
| sym-2000 | 20,000 | 7.8 | 1,017 | 0 | 0 | — |
| trickle | 20,000 | 7.2 | 1,050 | 138,298 | 0 | — |
| burst-100k | 20,000 | 7.6 | 1,013 | 100,000 | 0 | 12 ms |
| burst-500k | 20,000 | 7.5 | 1,022 | 500,000 | 0 | 44 ms |

- **Rate**: 10× the tick rate costs 3× the CPU (6.3 → 19.2% of one core); marginal ~0.1 µs/tick, sublinear as fixed per-slice costs amortize. No knee at 200k tps.
- **Symbols**: 4× symbols at fixed 1M alerts adds ~1.4pp of one core (6.3 → 7.8). Alert count, not tree depth, sets cost.
- **Firing**: +0.9pp of one core for ~575 fires/s sustained (trickle). Bursts are nearly free: 500k simultaneous triggers pushed and drained in 44 ms with zero ring drops — the engine's dedicated consumer keeps the 65,536-slot ring from overflowing even at 7.5× capacity inflow.
- **RAM is rate-, symbol- and burst-invariant**: peak RSS ≈ 1.0 GB in every scenario, set by the 1M-alert index (COW B-tree; ~87–89% of all allocation is `btype newNode` during seed). Matches the earlier structural profile: ~630 B/alert engine-side.

## Service layer (chronod)

| scenario | achieved tps | CPU % 1 core | RSS peak MiB | db MB | fired | ring drops | drain |
|---|---|---|---|---|---|---|---|
| baseline | 20,000 | 24.6 | 1,441 | 815 | 0 | 0 | — |
| rate-100k | 100,000 | 35.9 | 1,464 | 815 | 0 | 0 | — |
| rate-200k | 200,000 | 50.3 | 1,454 | 815 | 0 | 0 | — |
| sym-1000 | 20,000 | 25.4 | 1,450 | 801 | 0 | 0 | — |
| sym-2000 | 20,000 | 25.6 | 1,484 | 817 | 0 | 0 | — |
| trickle | 20,000 | **83.3** | 1,522 | 815 | 138,298 | 0 | — |
| burst-100k | 20,000 | 25.6 | 1,499 | 815 | 65,600 | 34,400 | 15.1 s |
| burst-500k | 20,000 | 24.2 | 1,494 | 817 | 65,664 | 434,336 | 14.9 s |

- **Wrapper cost** (service − engine, same logical work): +18pp of one core at 20k tps (24.6 vs 6.3) — gRPC decode, `price.Parse` ×2, catalog lookups, stats. At 200k tps the wrapper share shrinks relatively (50.3 vs 19.2): engine Match is ~2/3 of per-tick cost, wrapper ~1/3.
- **Rate**: linear-ish to 200k tps (24.6 → 35.9 → 50.3%), zero drops, not feeder-bound (driver hit target). Single-process ceiling lies well above 200k tps.
- **Symbols**: flat (24.6 → 25.4 → 25.6 across 4× symbols); the live registry's ~2,000 learned entries are invisible (~tens of KB).
- **Sustained firing is the service's dominant cost**: 575 fires/s costs +58pp of one core over parked (83.3 vs 24.6) — the pump's enrich→publish→store-flip pipeline (~1.7 ms CPU per trigger). The engine's own firing cost for the same rate was +0.9pp. Delivery, not matching, caps service capacity under sustained firing.
- **RAM**: peak ≈ 1.45–1.52 GB across all scenarios = engine index (~1.0 GB) + store-backed structures; db ≈ 815 MB. Still alert-count-bound (RSS ≈ 2× db), consistent with the 1M-alert memory work.

## Burst capacity finding

The production pump cannot absorb bursts larger than the engine ring (65,536):

| burst | delivered | ring-lost | loss | drain (deliver+flip) |
|---|---|---|---|---|
| 100k | 65,600 | 34,400 | 34.4% | 15.1 s |
| 500k | 65,664 | 434,336 | 86.9% | 14.9 s |

Delivered volume is flat at ~65.6k regardless of burst size (ring capacity), and flip throughput is ~4.4k triggers/s. The engine layer loses nothing on the same bursts (its consumer drains during the push), so the loss is specific to the service pump's batch-store-flip pace. Mitigation directions: larger ring (`Config.RingSize`), a streaming enrichment path (BatchGet while draining), or backpressure into the feed. Note the engine's fire-once CAS means ring-lost triggers are gone for the boot; they re-arm only via replay after restart (records stay active).

## Attribution (what dominates)

- **CPU, engine layer**: comparator + B-tree search in `Match` (~30–47% of samples); runtime clock/scheduler visible only because the engine idles below 20k tps pacing. GC negligible (20–45 cycles/run, no GC frames in top lists).
- **CPU, service layer**: `ingestTick` cum ~61% (engine Match 2/3 of it; `price.Parse` ~3–5%, protobuf decode ~4–5%); trigger delivery dominates under sustained firing (trickle: 83% of a core).
- **RAM, both layers**: COW B-tree node churn at seed (87–89% of cumulative allocation). Post-seed steady state allocates almost nothing — matching is allocation-free.
- **Cross-layer parity checks**: identical fired counts (trickle 138,298 on both layers), identical walk (77/77 in the smoke), sent == accepted everywhere, zero malformed drops.

## Caveats

- Engine CPU% denominator excludes the burst drain; service CPU% window includes it (gap scenarios) — burst service CPU is mildly overstated; ts-stamped `stats.jsonl` allows post-hoc correction.
- Trickle ladder depletes front-loaded (~70% of fires in the first quarter) — inherent to a uniform ladder under a mean-reverting walk.
- Engine GCCount includes seeding; service GC count not collected.
- Feeder CPU not reported separately; 200k service tps was verified non-saturated by the achieved-rate gate only.
- burst-100k (service) INVALID on first attempt: chronod's gRPC MaxConnectionAge (5 min) killed the stream and results were lost — fixed by a fresh-stream burst (`beaa421`) and conservation-based drain (`HEAD`); the re-run is the reported one.

## Recommendations

1. **Capacity envelope (this box)**: 1M alerts + 20k tps + ~575 fires/s sustains on ~83% of one core; ingest alone would allow ≫200k tps. Plan capacity per alert (≈630 B engine + ~0.9 KB persisted, RSS ≈ 2× db) — rate and symbol count are weak axes.
2. **Burst absorption is the one real gap**: raise `RingSize` beyond 65,536 or make the pump stream; today a 100k burst silently drops 34% at the service boundary while the engine side loses none.
3. **Trigger delivery is the scaling frontier**: an order of magnitude more fire-rate requires a faster enrich/flip path (batching is already there; concurrency or larger batches next), not a faster matcher.
4. Re-run this campaign after any engine change: `for s in baseline rate-100k rate-200k sym-1000 sym-2000 trickle burst-100k burst-500k; do bash scripts/perf/run.sh engine $s docs/perf/<date>-campaign && bash scripts/perf/run.sh service $s docs/perf/<date>-campaign; done`

## After the burst-absorption fix (postopt)

Three service scenarios re-run same-day, same box, same gates after two levers
landed: the trigger ring default grew **65,536 → 1,048,576 slots**
(`f1eeee0`) and the pump became a **pipelined batch pipeline** — 4096-wide
enrich overlapping the store flip (`2b072f7`). Full runs: `docs/perf/2026-09-06-campaign-postopt/`.

| scenario | | CPU % 1 core | RSS peak MiB | delivered | ring-dropped | drain |
|---|---|---|---|---|---|---|
| trickle | before → after | 83.3 → **75.7** | 1,522 → 1,485 | 138,298 → 138,298 | 0 → 0 | — |
| burst-100k | before → after | 25.6 → 25.4 | 1,499 → 1,590 | 65,600 → **100,000** | 34,400 → **0** | 15.1 s → **2.8 s** |
| burst-500k | before → after | 24.2 → 23.6 | 1,494 → 1,770 | 65,664 → **500,000** | 434,336 → **0** | 14.9 s → **9.1 s** |

**Targets were met on both burst scenarios and partially on trickle.** The
burst-loss finding is gone: `fired + ring_dropped == cluster` now holds with
`ring_dropped = 0` on both bursts — 100k and 500k clusters delivered whole,
drains down 5.4× and 1.6× (effective flip throughput ~35–55k triggers/s (100k vs 500k burst) vs
~4.4k/s before), at the cost of +91/+276 MiB peak RSS (ring slots plus
delivered/flip buffers; well under the 8 GiB cap). Trickle CPU fell 7.6pp of
one core (83.3 → 75.7) with identical fire counts — the pump pipeline hides
part of the per-trigger store cost, but delivery remains the service's
dominant expense (~51pp over parked), so the sustained-firing frontier stands,
at a better slope. Caveat from before still applies: burst CPU% windows
include drain, so burst CPU figures are mildly overstated either way.
