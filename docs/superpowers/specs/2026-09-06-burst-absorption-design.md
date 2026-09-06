# Burst Absorption: bigger ring + pipelined trigger pump

Date: 2026-09-06
Status: approved (design), pending implementation plan

## Context

The 2026-09-06 performance campaign measured the service layer's trigger
delivery as its scaling frontier: sustained firing (~575 fires/s) costs 83.3%
of one core (engine side: +0.9pp), and simultaneous bursts lose everything
above the engine ring's 65,536 capacity to overflow — 34.4% of a 100k burst,
86.9% of a 500k burst — while the engine layer loses zero on the same bursts
(its consumer drains concurrently). Root cause on both counts: the pump drains
64 triggers per batch and runs enrich (BatchGet) → publish → flip
(MarkTriggeredBatch) strictly sequentially per batch, so small-batch store
transaction overhead dominates (~1.7 ms CPU per trigger; ~4.4k flips/s).

## Decisions

- **Ring capacity becomes operational, not compiled-in**: chronod gains a
  `-ring` flag (default 1,048,576 slots ≈ 32 MB), applied to
  `engine.Config.RingSize` before `NewCore`. `NewCore`'s signature is
  unchanged; the engine (frozen) rounds to a power of two. Default covers the
  largest tested burst (500k) with 2× headroom.
- **The pump becomes a two-stage pipeline** (`internal/service/core.go`):
  - Stage A (existing pump goroutine, enrich): pop up to 4096 triggers from
    the ring, BatchGet-enrich, publish, update `TriggersFired/Published`
    stats at drain time (unchanged timing); send the resulting
    `fired []alertstore.Fired` slice over a buffered channel (capacity 2).
  - Stage B (new flipper goroutine): one `MarkTriggeredBatch` per received
    batch; `active`/`triggered` gauges move to this stage (they track store
    truth). bbolt View (read) txs run concurrently with write txs, so A's
    BatchGet overlaps B's flip naturally; writes were always serialized.
  - Channel capacity 2 provides backpressure: if flipping lags enriching,
    draining blocks and the ring absorbs — the ring's new job.
- **Shutdown contract preserved**: pumpCancel stops the drain loop; the pump
  closes the channel without taking another batch; the flipper flushes what
  is in flight and exits; `pumpDone` closes only after the flipper exits, so
  `Close` still guarantees the pump is fully stopped before `eng.Close`.
- **Error semantics unchanged**: BatchGet failure → batch counted as fired,
    records stay active (replay re-arms them at restart); flip failure →
    logged, records stay active until restart. Publish order is preserved
    (single enrich goroutine); flip order is irrelevant (idempotent writes).

## Changes

1. `cmd/chronod/main.go`: `-ring` int flag (default 1<<20) → `cfg.RingSize`.
2. `internal/service/core.go`: `deliverBatch` splits into `enrichBatch`
   (drain → enrich → publish → return fired slice) and `flipBatch` (store
   write + gauge updates); `pump()` gains the 4096 drain buffer, the channel,
   and the flipper goroutine; `pumpDone` semantics extended as above.

## Out of scope

GOMEMLIMIT tuning (deferred), any `engine/` change, wire/proto changes,
pump concurrency beyond the two stages (writes serialize in bbolt anyway).

## Testing

- Pipeline flush on Close: after a trigger stream, `Close()` flushes every
  in-flight flip — store counts and the `triggered` gauge lose nothing.
- Gauge truth: N fires → `active`/`triggered` match `store.Counts()` exactly.
- Flip failure: records stay active, the "until restart" warning logs.
- Existing stress/oracle/replay suites must stay green (external pump
  contract is unchanged).
- End-to-end proof: re-run `service trickle`, `service burst-100k`,
  `service burst-500k` into `docs/perf/2026-09-06-campaign-postopt/` and add
  a before/after table to the campaign REPORT.md. Targets from the campaign
  baseline: trickle CPU 83.3% → substantially lower; burst-500k ring loss
  434,336 → 0; burst drains ~15 s → low single-digit seconds or below.

## Risks

- Gauge timing: `active` now decrements at flip time, slightly later than
  fire time; `/stats` may briefly show both a fired-but-unflipped trigger in
  `active` and the fire in `triggers_fired`. Accepted — gauges track store
  truth; the campaign's drain criterion (fired+ring ≥ cluster AND
  triggered ≥ fired) already models this.
- 32 MB default ring RAM is paid by every deployment. Accepted (invisible
  next to ~630 B/alert); operators can lower it.
- A pathological store that flips slower than the ring fills still drops —
  the pipeline widens the window, it cannot remove the laws of physics.
