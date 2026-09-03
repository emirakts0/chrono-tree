# crypto-precision retrofit — Design Spec

**Date:** 2026-09-03
**Status:** Approved (brainstorming complete)
**Module:** `github.com/emir/chrono-tree` (Go 1.27)
**Amends:** `2026-09-02-chrono-tree-design.md` (price representation only)

## 1. Purpose

Adapt the merged Phase A engine for cryptocurrency market precision. Crypto targets
exchange-quoted pairs: prices up to ~$1M with 8–10 fractional decimals (sub-penny
tokens), where binary floating point is unacceptable — `0.1` is not representable in
float64, so GTE/LTE comparisons at decimal boundaries can mis-fire by 1 ulp. The fix:
the engine's price representation becomes **exact fixed-point `int64` in base units**
(smallest quoted tick), with all decimal↔binary conversion confined to one audited
boundary package.

## 2. Requirements Summary

| Requirement | Decision |
|---|---|
| Price universe | Exchange-quoted pairs: ≤ ~$1M, ≤ 8–10 decimals → `int64` base units (≤ ~10¹⁶, ample headroom) |
| Engine price repr | `type Price int64`, scale-agnostic — engine never knows the decimal point |
| Scale ownership | Ingestion boundary (daemon config, Phase B); engine is a pure integer-matching kernel |
| Input formats | Mixed: decimal strings and float64, normalized before the engine (this spec ships the tooling) |
| Float policy | Strict round-trip: shortest-repr format → exact digit parse; any loss rejects with `ErrPrecisionLoss` — never silent rounding |
| Scope | Engine retrofit + `price/` package only; Phase B (gRPC, daemon, bench) specs separately |
| API break | Accepted — no external consumers |

## 3. Engine Changes

```go
// engine/alert.go
type Price int64 // price in base units of the instrument's smallest quoted tick.
                 // Scale-agnostic: the engine never knows where the decimal point is.
```

- `entry.price` becomes `Price` — layout-neutral (float64 → int64, entry stays 48 bytes).
- `compareEntry` compares `Price` as int64: true total order, no NaN/−0 ordering
  subtleties, marginally faster than float compares. Probes (`entryKey`, `entryKeyMax`)
  take `Price`.
- `Tick.Bid/Ask/Mid/Last` become `Price`; `priceOf` returns `Price`.
- `Trigger.Price` becomes `Price` — downstream consumers receive the exact base-unit
  value and format for humans via `price.Format` at their leisure.
- `AlertSpec.TargetPrice` becomes `Price`. **No new validation** — scale is unknowable
  to the engine by design. `Price(0)` is a legitimate target (GTE-0 fires on any
  positive tick; meaningful for sub-penny breakouts).
- Untouched: COW publication, snapshot retirement, slot generations, reaper, trigger
  ring, `Config`, all existing sentinels. This is a representation swap on the compare
  path, not a structural change.

## 4. The `price/` Package

The only place decimal↔binary conversion happens.

```go
package price

func Parse(s string, decimals uint8) (int64, error)     // exact, no float math
func FromFloat(f float64, decimals uint8) (int64, error) // strict round-trip
func Format(v int64, decimals uint8) string              // exact inverse of Parse
```

Sentinels: `ErrSyntax`, `ErrOverflow`, `ErrPrecisionLoss`, `ErrNotFinite`, `ErrScale`.

### Parse

Grammar: `[+-]? digits [ '.' digits ]` — at least one digit overall; `1.` and `.5`
accepted (strconv-like). Exponent notation rejected (`ErrSyntax`) — exchange feeds and
`FormatFloat('f')` output never use it. Cap: `decimals ≤ 18` (`10¹⁸ < MaxInt64`), else
`ErrScale`.

Exactness rules — this is where shift and rounding bugs would live, so the semantics
are pinned by table:

| Input @ 8 decimals | Result |
|---|---|
| `"0.1"` | `10000000` — exact digit parse, no float ever involved |
| `"0.100000000"` | `10000000` — digits beyond scale that are zeros are accepted (value is exact) |
| `"0.000000005"` | `ErrPrecisionLoss` — nonzero digit beyond scale; never silently rounded |
| `"9223372036854775808"` | `ErrOverflow` — MaxInt64+1; checked multiply-add (`v > (MaxInt64−d)/10` pattern) |
| `"-0"`, `""`, `"1e5"`, `"0x1"` | `0`, `ErrSyntax`, `ErrSyntax`, `ErrSyntax` |

Signs honored; negatives are representable — the engine is agnostic, and rejecting
negative price targets is a Phase B service-layer business rule.

### FromFloat

Rejects NaN/±Inf (`ErrNotFinite`); negative zero normalizes to `0`. Formats with
`strconv.FormatFloat(f, 'f', -1, 64)` — the shortest string that round-trips the
float — then feeds that string to `Parse`. So `0.1` → `"0.1"` → exactly `10000000` at
8 decimals, while a float that is genuinely inexact at the given scale fails loudly
with `ErrPrecisionLoss`.

### Format

Pure integer math (powers-of-ten table); no float round-trip. Invariants:
`Format(Parse(s, d), d)` normalizes `s`, and `Parse(Format(v, d), d) == v` for all
in-range `v`.

## 5. Testing & Verification

**`price/` package:**
- Table tests over the nasty-value matrix (leading zeros, `+`/`-`, zero-tail
  fractions, `MaxInt64` boundary, `decimals` 0 and 18 extremes).
- Property test against `math/big` (stdlib, test-only): for random decimal strings
  and random scales 0–18, `Parse` agrees exactly with big-Integer scaled arithmetic —
  the same oracle discipline as the engine's brute-force oracle.
- Round-trip invariant tests (both directions, as pinned in §4).
- `FromFloat` exactness table: every float whose shortest representation fits the
  scale converts; known-inexact ones reject. Negative tests must demonstrably fail if
  the rejection logic is removed (negative validation, as practiced in Phase A).

**Engine rebase (tests re-target, not re-thought — no semantics change):**
- Existing price literals convert to base units (e.g., `100.5` @ 8 → `10050000000`)
  via small test helpers built on `price.Parse`.
- The brute-force oracle generates random integer prices and random scales, and gains
  a new duty: triggers must be identical whether prices arrived via strings → `Parse`
  or as direct int64 — proving no hidden conversion layer exists.
- Zero-alloc guarantee re-verified: existing `testing.AllocsPerRun` assertions must
  still pass; the swap must not add allocations.
- Benchmarks re-run at 1M (`CHRONO_BENCH_10M` still env-gated): expectation is
  equal-or-better ns/op from integer compares; any regression beyond noise is
  investigated before merge.

## 6. Out of Scope

- Phase B service layer: daemon scale/decimals config format, ingestion endpoints,
  negative-price business rules, gRPC schema price types.
- Wider-than-int64 representations (18 decimals at any magnitude) — revisit only if
  on-chain/DeFi instruments with mandatory 18-decimal quoting enter scope.
- Persisted scale metadata; cross-service scale negotiation.
