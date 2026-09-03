# chrono-tree

In-memory price alert engine for crypto markets. You register alerts like
"BTCUSDT ask >= 65000" and feed it market ticks; when an alert's condition is
met it fires exactly once and delivers a trigger through a lock-free queue.

Prices are `int64` base units, not floats. A price of `"12.34"` at 2 decimals
is the integer `1234`. This is a hard requirement for crypto: sub-penny tokens
quote with 8-18 decimals and float64 cannot represent them exactly. All
decimal-to-binary conversion happens in one place, the `price` package, and it
never rounds - a value that does not fit exactly is rejected.

## Packages

### `engine`

The matching core. Pure integer arithmetic, scale-agnostic: it never knows
where the decimal point is.

- Alerts are indexed in B-trees keyed by `(price, alert id)` - one tree per
  symbol, price type (bid/ask/mid/last) and direction (>= / <=).
- `Match` runs lock-free on the hot path: a tick descends/ascends the two
  trees for its symbol and touches only entries in range. No full scans, no
  allocations.
- Alert mutations (upsert/cancel/pause) go through a bounded queue and are
  applied to copy-on-write snapshots, so ticks never block on writers.
  Old snapshots are retired once readers drain (RCU style).
- Exactly-once firing: each alert has a state slot; the ACTIVE to TRIGGERED
  transition is a single CAS. Two ticks hitting the same alert fire it once.
- Triggers are delivered through a bounded Vyukov MPMC ring buffer
  (`Triggers()`); if the consumer is slow, drops are counted, not blocked.
- A background reaper handles expiries, recycles slots of removed alerts and
  runs an integrity sweep. `Close` shuts everything down (goleak-verified).

### `price`

The conversion boundary. Stdlib only, exact or error:

- `Parse(s, decimals)` - decimal string to int64 base units. Digit-by-digit,
  checked multiply-add, no floating point. A nonzero digit beyond the scale
  returns `ErrPrecisionLoss`; overflow returns `ErrOverflow`.
- `FromFloat(f, decimals)` - strict float input. Converts through the
  shortest round-trip string representation and never rounds: an inexact
  value is rejected. NaN/Inf rejected.
- `Format(v, decimals)` - int64 back to a decimal string, pure integer math.

Scale (how many decimals a symbol quotes with) is the caller's contract; a
service layer would own a symbol-to-decimals table.

## Usage

```go
e := engine.New(engine.DefaultConfig())
defer e.Close()

id := engine.AlertID{...} // 16-byte id, e.g. UUIDv7

err := e.Upsert(engine.AlertSpec{
    ID:          id,
    Symbol:      "BTCUSDT",
    PriceType:   engine.PriceAsk,
    Direction:   engine.DirGTE,
    TargetPrice: 65000_00000000, // 65000 at 8 decimals
    ValidFrom:   time.Now().UnixNano(),
    AutoDeactivate: true,
})
e.Sync() // wait until the alert is visible to Match

e.Match(&engine.Tick{
    Symbol: "BTCUSDT", Ask: 65001_00000000,
    Present: engine.TickAllPresent(), TS: time.Now().UnixNano(),
})

for {
    tr, ok := e.Triggers().Pop()
    if !ok {
        break
    }
    // tr.ID, tr.Price (base units), tr.TS
}
```

## Tests

```
go test ./... -race -count=1
ok  github.com/emir/chrono-tree/engine  3.482s
ok  github.com/emir/chrono-tree/price   1.552s
```

Coverage beyond unit tests:

- `price` has a 50,000-case property test against `math/big` as an independent
  oracle: `Parse` must agree exactly with unlimited-precision arithmetic, and
  must reject what big.Int says is not exactly representable. Round-trip
  invariants are pinned in both directions (`Parse(Format(v,d),d) == v`).
- `engine` has a randomized oracle test comparing triggers against a naive
  brute-force evaluator, plus a string-price parity test: the same scenario
  run with direct int64 prices and with prices routed through
  `price.Format`/`price.Parse` must produce identical trigger sets. This
  proves the engine has no hidden conversion layer.
- Zero-allocation hot path is asserted with `testing.AllocsPerRun == 0`;
  concurrency is exercised under the race detector.

## Benchmarks

```
go test ./engine -bench . -benchmem -run '^$'
goos: linux
goarch: amd64
cpu: AMD Ryzen 5 5600H with Radeon Graphics

BenchmarkMatchSparse1M-12     2277883    526.8 ns/op    0 B/op    0 allocs/op
BenchmarkMatchDenseSkip-12       6387    183943 ns/op   0 B/op    0 allocs/op
```

- `Sparse1M`: 1,000,000 alerts over 1000 symbols; a tick that fires nothing.
  This is the dominant shape under sustained load: ~527 ns per tick,
  independent of total alert count (only the symbol's trees are touched).
- `DenseSkip`: 20,000 alerts already triggered on one symbol; measures the
  per-tick cost of skipping out-of-range entries between price gaps.

Both paths are allocation-free. `BenchmarkMatchSparse10M` exists behind
`CHRONO_BENCH_10M=1`.

## Status

Engine and price boundary are complete. Not yet built: the service layer
(gRPC daemon, symbol-to-decimals config, input validation). Design docs live
in `docs/superpowers/specs/`.
