# chrono-tree Alert Store — bbolt Persistence Design

**Date:** 2026-09-05
**Status:** Approved (brainstorming complete)
**Module:** `github.com/emir/chrono-tree`
**Depends on:** `2026-09-04-service-design.md` (service layer, merged); reverses that spec's "Persistence: None" decision for the service layer — the engine stays volatile by design.

## 1. Purpose

Move the service-side alert catalog out of RAM into a persistent, fast-querying store (bbolt), so that:

- **Triggered-alarm inquiries are fast.** Today `ListAlerts` full-scans `map[AlertID]*Alert`, filters, and O(N log N)-sorts on every `/api/alerts` request — unusable at a million alerts.
- **The service keeps no alert log in RAM.** Only the engine holds alert state in memory; the service catalog lives in bbolt.
- **Alerts survive restarts.** bbolt is the source of truth; on boot, chronod replays active alerts into the engine.
- **The trigger pump enriches in bulk.** Engine triggers are batch-fetched from the store by alarm ID when the ring batch is drained — not held in memory.

## 2. Decisions

| Question | Decision |
|---|---|
| Restart semantics | **Full recovery** — bbolt is source of truth; boot replays active alerts into the engine |
| Stored data | **Alerts only** — one record per alert, state updated in place, with `fired_price`/`fired_at` fields written onto the record at fire time |
| Query strategy | **Per-field secondary indexes** (`state`, `symbol`, `venue`, `tier`, `direction`), ID-set intersection — fast for every UI filter combination |
| Durability | **Synced writes** — default bbolt fsync per commit; no `NoSync` |
| Retention | **Keep everything** — permanent audit log; offline `chronoctl compact` command, no deletion, no background sweeps |
| Integration | **Dedicated `internal/alertstore` package** behind its own API; bbolt embedded nowhere else |
| Fire/cancel race | Check-state-and-flip **inside bolt's single-writer write tx** — no service-side lock choreography |
| Store-write failure on pump | Alert stays `active` and may re-fire (at-least-once, visible duplication) — never silently marked triggered |
| API/UI surface | Unchanged — same HTTP contracts, same gRPC, same web UI |
| Engine/price | Zero diffs (still frozen) |

## 3. Layout

```
internal/alertstore/   — NEW: bbolt-backed alert catalog
    store.go           — Open/Close, Put, Get, BatchGet, Cancel, MarkTriggeredBatch
    query.go           — Query(filters, limit, offset) via index intersection
    keys.go            — record encode/decode + index key encode/parse
    replay.go          — IterActive: boot-time replay cursor
internal/service/      — Core swaps map[AlertID]*Alert for *alertstore.Store
cmd/chronod/           — new -db flag (path to the .bbolt file)
cmd/chronoctl/         — new "compact" subcommand (offline bbolt.Compact)
```

`go.mod` gains `go.etcd.io/bbolt`.

## 4. Data model & key encoding

**Record** (value of the `alerts` bucket; hand-rolled fixed layout in `keys.go` — no proto changes, no reflection):

| field | encoding |
|---|---|
| ID | 16 bytes |
| Symbol, Venue, Tier | length-prefixed UTF-8 |
| Decimals, PriceType, Direction, State | 1 byte each |
| TargetPrice, ValidFrom, Expires, CreatedAtUnixNanos, FiredPrice, FiredAtUnixNanos | 8 bytes each (big-endian) |

`FiredPrice`/`FiredAtUnixNanos` are zero until the pump flips state to `triggered`; both are written in the same tx. They surface through `AlertView` as a decimal string (formatting needs the record's `Decimals`) and nanos.

**Buckets:**

- `alerts` — key: 16-byte `AlertID` → value: record.
- `idx_state` — key: `state(1B) | ~createdAt(8B) | id(16B)`, empty value.
- `idx_symbol` / `idx_venue` / `idx_tier` / `idx_direction` — key: `valueUTF8 | ~createdAt(8B) | id(16B)`, empty value.

`~createdAt` is the big-endian nanos with all bits flipped, so ascending byte order is newest-first — a `Seek` to the value prefix yields newest entries directly; no reverse cursors. Index keys are self-describing: the querying side knows the value prefix it sought with, so `~createdAt|id` parses at a known offset. Symbol/venue/tier values come from the catalog vocabulary — bounded, ASCII, no separator ambiguity. The `id` suffix makes ordering deterministic on equal timestamps (UUIDv7 IDs are time-ordered anyway).

**Index maintenance on every write tx:**

- `Put` (upsert): if replacing, delete the old record's 5 index entries first (state may differ on re-registration of a triggered/cancelled ID); then write the record + 5 fresh entries.
- `MarkTriggeredBatch`: per fired alert, delete the `active` entry and insert the `triggered` entry in `idx_state`; the other four indexes are untouched (symbol/venue/tier/direction don't change at fire time). Batched: one tx per ring batch.
- `Cancel`: same shape, `cancelled` entry.

## 5. Service integration

`service.Core` changes:

- `alerts map[engine.AlertID]*Alert` and its `sync.RWMutex` are **deleted**. The engine remains the only hot-path in-RAM alert state.
- `stateCounts` becomes three `atomic.Int64` gauges, updated alongside store writes — they feed `/stats` and Prometheus (O(1) snapshots), never queries.
- `UpsertAlert`, `CancelAlert`, `ListAlerts`, `GetAlert`, `deliver` are rewritten against the store; validation logic is unchanged.
- Ticks never touch the store — the ingest path is unchanged.

## 6. Write paths & the fire/cancel race

**UpsertAlert:**

1. Validate exactly as today (catalog, price, dims).
2. `store.Put(record{State: active, CreatedAt: now})` — **before** `engine.Upsert`, preserving the "catalog first so the pump never misses" ordering. Bolt fsyncs; the RPC returns only after the record is durable.
3. `engine.Upsert(spec)`; on failure restore the previous record (or delete if new), then return the mapped engine error.
4. `engine.Sync()`; bump the active gauge.

**Pump (`pump`/`deliver` rewrite):**

1. `PopBatch(buf)` as today — the engine ring is untouched.
2. One `BatchGet(ids)` read tx → records for the batch. Unknown or non-active IDs are skipped and counted (drop-and-count, as today).
3. Enrich and publish per trigger (NATS semantics, health-transition logging, metrics — unchanged).
4. One `MarkTriggeredBatch` write tx: for each successfully published trigger, re-read state inside the tx and flip to `triggered` only if still `active`, writing `fired_price`/`fired_at`.

**CancelAlert:** `store.Cancel(id)` (write tx that no-ops unless state is `active`), then `engine.Cancel`. If a trigger fires between the two, the pump's `MarkTriggered` sees state already `cancelled` and skips; the engine may still publish one in-flight trigger — same observable behavior as today.

The map's RWMutex choreography disappears: bolt serializes writers, so "re-read state inside the write tx" is the atomic check-and-flip. No lock ordering between service and store.

**Failure semantics:** if `MarkTriggeredBatch` fails, the flip is skipped and logged (health-transition style, like the NATS publisher). The trigger was already published; the record stays `active` and may re-fire on a later tick — at-least-once duplication is visible, silent loss is not.

## 7. Read paths

**Inquiry (`Query`, backing `/api/alerts`):**

1. For each set filter, open a cursor over that index's prefix range — entries arrive newest-first.
2. Intersect: walk the smallest range; for each candidate `id`, verify membership by seeking the other indexes' key suffix. `total` = full intersection count; pagination takes `[offset, offset+limit)`.
3. Materialize only the page's records in one read pass — the intersection phase reads index keys only, never record values.

The `AlertFilter` struct, response shape (`{total, items}`), and the web UI's pagination stay byte-compatible. Page sorting is free (entries emerge newest-first).

**GetAlert:** direct `alerts` bucket get by 16-byte ID.

**`/stats`:** `AlertsByState`/`AlertCount` read the atomics — no store round-trip on the 1 Hz snapshot path.

**Concurrency:** readers (inquiry handlers, pump BatchGet, replay) run concurrently under bolt MVCC read txs; all writes funnel through bolt's single writer (upserts, cancels, pump state flips).

## 8. Boot replay, lifecycle & ops

**Open:** new `-db` flag (default `chrono.bbolt`), opened before the engine is built. A corrupt/unopenable file is a hard startup failure — source of truth or nothing.

**Replay:** before gRPC/HTTP listeners start (readiness stays construction-order), iterate `idx_state` prefix `active`:

- `expires != 0 && expires <= now` → flip to `cancelled` in a batched tx. No new state value; `/stats` and the UI vocabulary are unchanged. Alerts whose `valid_from` is still in the future replay too — the engine already gates matching on `valid_from`.
- Otherwise `engine.Upsert(spec)` reconstructed from the record; one `engine.Sync()` after the loop.

Startup logs report restored / expired / total counts.

**Shutdown:** existing order stands; `store.Close()` after the pump has drained (`Close` waits on `pumpDone`) and after `engine.Close()`.

**Compaction:** `chronoctl compact <db-path> [-out path]` — read-only open, `bbolt.Compact` into a fresh file, atomic rename, before/after sizes. Offline tool; never run against a live file.

## 9. Error handling

- Corrupt DB at open → startup failure (fail fast).
- Store error on upsert/cancel path → `codes.Internal` via the existing `internal/service/errors.go` mapping; engine left untouched when the store write failed first.
- Store error on pump write path → logged with health transition; trigger already published; record stays active (see §6).

## 10. Testing

**`internal/alertstore`:**
- Round-trip: encode/decode every record field; index-key encode/parse at known offsets.
- CRUD + index integrity: after Put/MarkTriggered/Cancel/re-Put, every index bucket contains exactly the expected entries (full-scan compare) — no orphans.
- Query: table tests — each single filter, all pairings, full conjunction, empty-result filters, limit/offset windows, `total` correctness against a brute-force oracle (the repo's oracle pattern).
- Concurrency: parallel readers querying while a writer churns Put/MarkTriggered under `-race`.
- Replay: populate, close, reopen — active set intact; expired-while-closed alerts come back cancelled.
- Sync semantics: `NoSync` stays false in production config (config test).

**`internal/service`:** existing suites swap the map for a temp-file store (helper in `testenv_test.go`); bufconn end-to-end, oracle property, stress and race tests run unchanged. New cases: upsert → restart → replay → tick fires; cancel-vs-fire race under stress; `fired_price`/`fired_at` surfaced in `AlertView`.

**Perf gates:**
- Seed 1M alerts through UpsertAlert: sustained rate within an order of magnitude of today's; boot replay from that DB in seconds, not minutes.
- `/api/alerts` worst case (state=triggered among 1M, page 1): **< 10 ms** — the acceptance test for the whole change.
- `go build ./...`, `go vet ./...`, full suite green under `-race`, goleak clean, engine/price zero diffs.

## 11. Out of scope

- New alert states or API/UI changes (surface unchanged).
- Compound indexes beyond the five per-field indexes.
- Backup tooling beyond `chronoctl compact`.
- Engine persistence (WAL/snapshot) — the engine stays volatile; replay rebuilds it from the store.
- Retention/TTL/deletion — the store is a permanent audit log.
