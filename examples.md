# Examples

Working examples for using chrono-tree. For the architecture and design
rationale, see the [README](README.md).

- [Full cycle](#full-cycle) — register, feed, consume, shut down
- [Control plane](#control-plane) — cancel, pause/resume, sync
- [Match contract](#match-contract) — concurrency, no-op cases, exactly-once
- [Delivery](#delivery) — consuming triggers, sizing the queue
- [Dimensions](#dimensions-optional) — venue/tier style match dims

## Full cycle

```go
package main

import (
	"fmt"
	"time"

	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/price"
)

func main() {
	cfg := engine.DefaultConfig()
	cfg.TriggerQueueSize = 1 << 20 // size for bursts: ≥ max alerts on one symbol + stall budget
	e := engine.New(cfg)

	// Consumer: the engine never closes C(), so pair it with your own stop signal.
	stop := make(chan struct{})
	go func() {
		c := e.Triggers().C()
		for {
			select {
			case tg := <-c:
				fmt.Printf("fired %x at %d (ts %d)\n", tg.ID, tg.Price, tg.TS)
			case <-stop:
				return
			}
		}
	}()

	// Register: "BTCUSDT last ≥ 101.50".
	target, _ := price.Parse("101.50", 2) // 10150 — int64 base units, no floats
	id := engine.AlertID{0x01}            // 16 bytes; use a UUIDv7 in production
	err := e.Upsert(engine.AlertSpec{
		ID:          id,
		Symbol:      "BTCUSDT",
		PriceType:   engine.PriceLast, // Bid | Ask | Mid | Last
		Direction:   engine.DirGTE,    // GTE (market ≥ target) | LTE (market ≤ target)
		TargetPrice: engine.Price(target),
		ValidFrom:   time.Now().UnixNano(),
	})
	if err != nil {
		panic(err)
	}
	e.Sync() // optional: wait until the control plane is published

	// Feed a tick: never blocks, never allocates, never errors.
	e.Match(&engine.Tick{
		Symbol:  "BTCUSDT",
		Last:    10151, // 101.51 in base units
		Present: 1 << uint(engine.PriceLast),
		TS:      time.Now().UnixNano(),
	})

	// Shutdown order: stop submitting, stop the consumer, then Close.
	close(stop)
	e.Close()
}
```

## Control plane

```go
e.Cancel(id)                         // permanent removal (ErrNotFound if absent)
e.SetStatus(id, engine.StatusPaused) // ACTIVE → PAUSED: instant, tree untouched
e.SetStatus(id, engine.StatusActive) // PAUSED → ACTIVE: idempotent
st, ok := e.Status(id)               // read-only lookup, no mutation
e.Sync()                             // everything submitted so far is published
```

- `Upsert` with an existing ID **replaces** the live alert (old entry removed,
  new inserted in queue order).
- Control-plane calls enqueue on the unbounded, lossless mutation queue —
  they never block on a full queue and never drop. Keep them off the tick
  hot path.
- Zero values are meaningful: `ValidFrom: 0` is valid immediately, `Expires: 0`
  means never.
- Alerts are one-shot: firing transitions the slot to `Triggered` and removes
  the entry from the tree — there is no re-arm path.
- Terminal alerts (`Triggered`, `Cancelled`, `Expired`) reject transitions with
  `ErrInvalidTransition`; pause/resume only flips the slot between `Active` and
  `Paused`.
- `Status` reflects the ledger: an `Upsert` is visible immediately (the ledger
  entry is updated synchronously, no `Sync` needed), a fired/cancelled alert
  reports its terminal status right away, and the entry reports `false` once
  the flusher has applied its removal — terminal states are observable in the
  window before that cleanup.
- `Stats()` gauges — `Live`, `Symbols` (interned since `New`; the interner
  never evicts), `MutQDepth` (mutations awaiting the flusher), `ExpiryLen`
  (reaper registrations), `DroppedTriggers` — are independent point-in-time
  reads, not one consistent snapshot.

## Match contract

- Call from any number of goroutines concurrently; symbols proceed
  independently.
- Silent no-op on unknown symbols, `Present: 0`, malformed dims, or a closed
  engine.
- Exactly-once: when two ticks cross the same alert, the CAS winner fires and
  the loser observes `TRIGGERED`.

## Delivery

`Trigger` is deliberately minimal — `{ID, Price, TS}`; look up your own
metadata by ID. Three consumption styles:

```go
q := e.Triggers()
tg := <-q.C()        // blocking receive
n := q.PopBatch(dst) // batched drain: up to len(dst), 0 when empty, never blocks
```

The queue is bounded and drop-and-count: an overflow is counted in
`Dropped()` / `Stats().DroppedTriggers`, never backpressured into `Match`. A
drop is a lost notification (the alert is already consumed), so size
`TriggerQueueSize` for the peak: *max alerts on one symbol + consumer stall
time × fire rate*. A bare receiving loop drains far faster than `Match` can
produce — keep heavy work (persist, notify) in a downstream pool.

## Dimensions (optional)

```go
cfg.Dims = []string{"venue", "tier"} // fixed for the engine's lifetime, max 8

// On alerts and ticks alike — a fire requires exact equality on all dims:
Dims: engine.Dims(venueID, tierID)
```
