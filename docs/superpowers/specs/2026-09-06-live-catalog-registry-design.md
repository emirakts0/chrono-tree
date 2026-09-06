# Live Catalog Registry: the service learns reference data from the feed

Date: 2026-09-06
Status: approved (design), pending implementation

## Context

`internal/catalog` is compiled-in reference data (~500 symbols with decimals
and reference prices, plus the venue/tier dim vocabulary). The service
enforces it as law at five points: tick ingest rejects whole batches for
unknown symbols/venues/tiers (`errUnknownRefdata`), `UpsertAlert` rejects
unknown reference data, replay skips records that drifted from the catalog,
`venueTicks` fixes its keys at construction, and `GetCatalog`/the dashboard
serve the static table. Consequence: growing 500 → 1000 rates, or feeding a
newly listed pair at all, requires a recompile.

The catalog is doing double duty — *simulator dataset* (chronofeed walks
prices from it) and *validation gate* — and only the second role is wanted.
The service should record and match whatever the feed names.

## Decisions

- **Scope: symbols and dim values both learn at runtime.** New venues and
  tiers intern on first sight, same as symbols. Dim *names* stay fixed and
  positional (`venue` slot 0, `tier` slot 1); only values multiply.
- **Decimals derive from the wire string's fractional digits** —
  `"0.5200"` → 4, `"65000"` → 0 — for both ticks and alert targets. First
  data naming a symbol pins its scale for the boot.
- **Registry is pure RAM** (option A): no persistence. The feed is trusted
  to keep a symbol's precision consistent across runs. Replay needs no
  trust: `alertstore.Alert` already persists `Decimals` per record
  (`keys.go:42`), so the boot re-derivation is exact.
- `catalog.Default()` survives untouched as the simulator dataset;
  the service runs on `catalog.Empty()`.

## Changes

### 1. Registry — `internal/catalog`

`Catalog` gains an `RWMutex`; existing lookups (`Symbol`, `Value`, `Name`,
`DimValues`, `Symbols`, `Dims`) keep their signatures, now `RLock`-guarded.
New:

```go
func Empty() *Catalog // dims vocabulary only: zero symbols, zero dim values

// First sight interns; later sights are lookups.
EnsureSymbol(name string, decimals uint8) (Symbol, bool)
    // ok=false ⇔ already registered at different decimals (scale conflict)
EnsureValue(dim, name string) (uint16, bool)
    // ok=false ⇔ dim exhausted (65,534 values; DimSentinel 0xFFFF reserved)
```

- First-wins pinning; never rescale. `Reference` is empty for learned
  symbols (simulator-only field).
- Hot path stays three map hits + RLock per tick; writes only on first
  sight.

### 2. Scale derivation — `internal/price`

```go
func ScaleOf(s string) (uint8, error) // fractional-digit count; >MaxDecimals → error
```

Lives in `price` (the decimal-conversion package) with table tests.

### 3. Service — reject flips to intern (`internal/service/core.go`)

- `ingestTick`: unknown symbol → `ScaleOf(bid)` + `EnsureSymbol`; unknown
  venue/tier → `EnsureValue`. `errUnknownRefdata` and the batch-reject class
  are deleted; the only remaining tick errors are price-parse drops.
- `UpsertAlert`: unknown symbol → `ScaleOf(target)` + `EnsureSymbol`;
  unknown venue/tier → intern. The "unknown venue %q (valid: …)" errors
  disappear.
- `specFrom`/`replay`: `EnsureSymbol(a.Symbol, a.Decimals)`,
  `EnsureValue` for venue/tier. Never skips; the `skipped` counter and
  "catalog drifted" path are deleted.
- `venueTicks` becomes a `sync.Map` (`Load` → `Add`; miss → `LoadOrStore`)
  so first-seen venues count immediately.

### 4. Surfaces

- `GetCatalog` serves the live registry; learned symbols carry
  `Reference: ""` (legal proto value). `DimInfo.Values` reflect interned
  values.
- Dashboard `/stats` (`server.go:403-405`): `Venues`, `Tiers`,
  `SymbolCount` are live values.

### 5. Untouched

Engine (frozen), proto/wire, alertstore, chronofeed, `catalog.Default()`.

## Error handling

1. **Scale conflict** (string demands more precision than pinned):
   `price.Parse` already fails (`ErrScale`/`ErrPrecisionLoss`). Ticks ride
   the existing drop-and-count path; alert upserts map it to
   `InvalidArgument`. Never rescale silently — an alert at 2 decimals vs
   ticks at 4 would misprice invisibly.
2. **Dim exhaustion**: `EnsureValue` returns false; tick dropped-counted,
   alert rejected. No wraparound past `DimSentinel`.
3. Garbage input is still caught by the existing protovalidate wire
   patterns, unchanged.

## Testing

- `catalog`: first-sight pinning wins over conflicting `EnsureSymbol`;
  concurrent `Ensure*` under `-race` (N goroutines, one name → exactly one
  registration); dim-overflow false; `Empty()` round-trips `Value`/`Name`.
- `price`: `ScaleOf` table tests (no dot, trailing zeros, >18 digits).
- `service`: alert for a never-seen symbol + its first tick → triggers
  (the headline scenario); tick with more decimals than pinned → dropped,
  counted; `UpsertAlert` with unknown venue/tier succeeds; replay after
  restart re-registers from the store (`skipped` path gone).
- Engine conformance/oracle/parity suites untouched, must stay green.

## Cost

One map entry (~40 B) per symbol; at 1,000 symbols ~40 KB — invisible next
to the engine's ~470 B/alert.

## Risks

- A feed that changes a symbol's precision between runs descales its live
  alerts against new ticks (accepted: option A, feed trusted; alerts
  themselves replay exactly via persisted `Decimals`).
- Unbounded registry growth is possible from a chatty feed (accepted: feed
  trusted; protovalidate patterns bound the worst shapes).
