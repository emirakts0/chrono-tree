# Remove Payload Metadata from the Engine

Date: 2026-09-06
Status: approved (design), pending implementation

## Context

A 1M-alert profiling run (pprof + smaps, 2026-09-05) attributed ~160 MB of the
engine's ~630 B/alert live heap to `meta map[AlertID]*AlertMeta` — a map the
engine maintains on every Upsert and removal. Code inspection showed the map
has **no readers anywhere**: no getter, no trigger payload carries it, and the
trigger pump enriches from the alertstore (`Core.deliverBatch` →
`Store.BatchGet`) instead. The struct's `UserID`/`Segment`/`Channels`/`Notes`
fields are additionally unused end-to-end in this product.

The engine therefore pays memory and code noise for write-only state.

## Decision

- The engine evaluates alerts; it does not carry notification payload.
  `AlertMeta`, `AlertSpec.Meta`, and the `Engine.meta` map are deleted.
- The package freeze on `engine/` is lifted for exactly this change; the
  engine's behavioral contracts are unchanged.
- **Standing decision for future features:** if metadata is ever wanted at
  fire time, its home is the alertstore (the source of truth, already fetched
  in bulk by the pump) — not a revived engine-side map.

## Changes

1. `engine/engine.go` — delete the `AlertMeta` type, the `AlertSpec.Meta`
   field, the `Engine.meta` field, and its init in `New`. The mutex still
   guards `refs` and `live`.
2. `engine/index.go` — drop `meta := a.Meta` / `e.meta[a.ID] = &meta`
   (around `index.go:233-236`) and both `delete(e.meta, …)` sites (around
   `index.go:134,207`). These lines only maintained the unread map; no
   behavioral difference is possible.
3. `internal/service/core.go` — delete the two `Meta: engine.AlertMeta{…}`
   fills (`specFrom` around `core.go:216`, `UpsertAlert` around
   `core.go:344`). Every field in those literals is already present in the
   store record the pump fetches for enrichment.
4. `engine/engine_test.go` — drop the `Meta:` fill around line 384.
5. No wire (proto), store, or UI changes: nothing downstream observes the
   metadata today.

## Verification

1. `go vet ./...` clean.
2. `go test ./... -race` — the engine's conformance, oracle, and parity
   suites prove firing behavior is unchanged.
3. Re-run the 1M-alert seed and profile (same procedure as the 2026-09-05
   run): expect engine live heap to drop from ~630 to ~470 B/alert
   (~160 MB less at 1M), with the `engine.Upsert` meta allocations gone
   from the inuse_space profile.

## Risks

Low. The map has no readers, so no consumer can miss it. The only semantic
footprint is the recorded decision above: future fire-time metadata belongs
in the store, not the engine.
