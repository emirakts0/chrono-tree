<h1 align="center">chrono-tree</h1>

<p align="center">
  An in-memory price-alert engine for markets. Register "BTCUSDT ask ≥ 65000",
  feed it ticks, and matching alerts fire exactly once — sub-microsecond,
  allocation-free, at millions of concurrent alerts.
</p>

<p align="center">
  Built for cryptocurrency markets: high tick rates, sub-cent precision
  (8–18 decimals), venues and book tiers as match dimensions.
</p>

<div align="center">

[![Go](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![btype](https://img.shields.io/badge/B--Tree-tidwall%2Fbtype-CA2159)](https://github.com/tidwall/btype)

</div>

## Overview

> chrono-tree is a pure Go engine library that holds millions live price alerts
> and evaluates them against a continuous tick stream. The hot path never
> allocates and never takes a lock: a tick descends two B-trees for its symbol,
> touches only entries in range, and fires winners through a single CAS.
> Alerts are terminal once triggered — one alert, one trigger.

## Features

- **Zero-Allocation Matching** — a tick touches only its symbol's trees: ~0.6 µs at 1M alerts, `0 B/op` asserted in tests. Per-tick cost is O(qualifying entries), independent of total alert count.
- **Exact Integer Prices** — no floats anywhere. Prices are `int64` base units (`"12.34"` at 2 decimals is `1234`); one conversion boundary (`price`) accepts only values representable exactly, which sub-cent tokens with 8–18 decimals require.
- **Partitioned B-Tree Index** — 8 trees per symbol (4 price types × 2 directions), keyed by `(dims, price, id)`. Every entry a scan visits qualifies by construction — there are no per-entry filters.
- **Copy-on-Write Snapshots** — mutations are applied to tree copies and published with one atomic store (RCU style). Ticks never block writers; writers never block ticks.
- **Exactly-Once Firing** — the ACTIVE → TRIGGERED transition is a single CAS on a per-alert slot, so concurrent ticks and stale snapshots cannot double-fire.
- **Up to 8 Match Dimensions** — caller-owned dimension values (e.g. venue, book tier) fold into the tree key; a fire requires exact equality across all of them.
- **Bounded Trigger Ring** — a Vyukov MPMC ring delivers triggers; overflow drops-and-counts instead of blocking the matching path.
- **Background Reaper** — sweeps expiries, recycles alert slots, and runs an integrity pass; shutdown is goleak-verified.

## Architecture

```mermaid
flowchart LR
    T["Market Tick"]

    subgraph DP["DATA PLANE · lock-free"]
        direction TB
        IN["Symbol Interning<br/>string → uint32, 0 alloc"]
        SNAP["Snapshot Load<br/>atomic.Pointer"]
        TREES["8 B-Trees per symbol<br/>4 price types × 2 directions<br/>key: (dims, price, id)"]
        CAS["Exactly-Once Gate<br/>CAS ACTIVE → TRIGGERED"]
        RING["Vyukov MPMC Ring<br/>65,536 triggers"]
        IN --> SNAP --> TREES --> CAS --> RING
    end

    subgraph CP["CONTROL PLANE · single writer"]
        direction TB
        API["Upsert / Cancel /<br/>Pause"]
        MQ["Bounded Mutation<br/>Queue"]
        FL["Flusher · COW publish"]
        API --> MQ --> FL
    end

    subgraph HK["HOUSEKEEPING"]
        RP["Reaper<br/>expiry · slot recycle"]
    end

    OUT["Consumer<br/>Pop / PopBatch"]

    T -- "Match()" --> IN
    RING --> OUT
    FL -. "atomic publish" .-> SNAP
    RP -. "removals" .-> MQ

    classDef input fill:#FFDE17,stroke:#111,stroke-width:2px,color:#111
    classDef data fill:#69D2E7,stroke:#111,stroke-width:2px,color:#111
    classDef control fill:#FF6B6B,stroke:#111,stroke-width:2px,color:#111
    classDef house fill:#BDE399,stroke:#111,stroke-width:2px,color:#111
    classDef comp fill:#ffffff,stroke:#111,stroke-width:1px,color:#111

    class T input
    class IN,SNAP,TREES,CAS,RING data
    class API,MQ,FL control
    class RP house
    class OUT comp

    style DP fill:#E8F7FB,stroke:#111,stroke-width:2px
    style CP fill:#FFE9E7,stroke:#111,stroke-width:2px
    style HK fill:#F0FAE9,stroke:#111,stroke-width:2px
```

The engine is one package with no network and no I/O. Everything is built
around a single decision: **readers never mutate shared state.**

**Index.** Each alert is a ~64-byte, pointer-free record stored *by value*
inside a [`tidwall/btype`](https://github.com/tidwall/btype) table, sorted by
`(dims, price, id)` — the id breaks ties so equal prices coexist. Each symbol
owns 8 tables, one per price type (bid/ask/mid/last) × direction (≥/≤). That
partitioning is what keeps the scan honest: a GTE scan `Descend`s from the tick
price, an LTE scan `Ascend`s from it, and every entry visited is a candidate —
symbol, type, direction and dims were all resolved by the tree key, not by
filtering.

**Publication.** Upserts and cancels go through a bounded mutation queue to a
single flusher goroutine, which copies the affected symbol's tables, applies
the batch, and publishes via `atomic.Pointer`; `btype`'s ref-counting reclaims
retired snapshots once readers drain. Because firing cannot mutate a snapshot,
"fire" and "remove" are split: the CAS flips the alert's status slot, and the
removal is enqueued back to the flusher. A fired entry lingering in the tree is
simply re-skipped.

**Delivery.** Winners land on a bounded MPMC ring (`Pop` / `PopBatch`). A slow
consumer means counted drops, never backpressure into `Match` — for a feed
ingestion path, a stale trigger beats a stalled matcher. A background reaper
handles expiries (lazy inline checks remain the correctness backstop) and
recycles slots so the slot arena stays dense and cache-friendly.

Correctness is checked against independent oracles: a randomized brute-force
evaluator must produce identical trigger sets, the same scenario run through
`price.Format`/`Parse` must match direct integer prices exactly, and `price`
is property-tested against `math/big` over 50,000 cases.

Note: developed with GLM 5.3 (Z.ai).

## A demo on top

To validate the engine under realistic conditions, i built a demo daemon on
top of it — `chrono.v1` gRPC ingestion, trigger publishing to NATS, a bbolt
alert store, and an embedded monitoring dashboard, with a synthetic market
feeder — and ran it against 1M live alerts at up to 200k ticks/s. The demo is
not part of this repository; the engine ships as a pure library with no
network and no I/O.

## Benchmarks

(AMD Ryzen 5 5600H, Linux/amd64):

| benchmark | ns/op | allocs/op | |
|---|---:|---:|---|
| MatchSparse1M | 623 | 0 | 1M alerts across 1k symbols, non-firing tick |
| MatchSparse5M | 679 | 0 | same tick at 5M alerts — per-tick cost stays ~flat |
| MatchHotSymbol1M | 1,190 | 0 | all 1M alerts on one symbol — deeper-tree seeks |
| MatchDenseSkip | 209,715 | 0 | tick price crosses 20k already-fired entries |
| MatchFire1k | 75,316 | 5 | 1k fresh fires per tick (~75 ns/fire); the allocs are the concurrent COW removal flush, not the match path |
| MatchDimsSparse1M | 874 | 0 | same as Sparse1M with 2 match dimensions |
| MatchDimsSparse5M | 1,050 | 0 | same as DimsSparse1M at 5M alerts |
| MatchDimsDenseSkip | 201,614 | 0 | same as DenseSkip with 2 match dimensions |

---

<p align="center">
  Developed with GLM 5.3 (Z.ai).<br>
  <a href="mailto:emirakts0@gmail.com">emirakts0@gmail.com</a>
</p>
