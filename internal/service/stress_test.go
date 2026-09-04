package service

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/price"
)

// TestStressFeedAndPublish: parallel upserts and a 5k/s tick stream for
// ~1.5s against a real in-process NATS server. Under -race this
// exercises the pump, the publisher, and catalog mutation together.
// Assertions: triggers fired > 0, received over NATS > 0, zero dropped
// publishes at this modest rate.
func TestStressFeedAndPublish(t *testing.T) {
	e := newNATSEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nc := e.natsClient(t)
	var received atomic.Uint64
	if _, err := nc.Subscribe("chrono.triggers.>", func(*nats.Msg) { received.Add(1) }); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	syms := catalog.Default().Symbols()
	// math/rand/v2's *Rand must be used by one goroutine at a time, so each
	// goroutine gets its own source.
	newRng := func(seed byte) *rand.Rand { return rand.New(rand.NewChaCha8([32]byte{seed})) }
	// 500 alerts within ±1% of reference: many will fire under the stream.
	upserts := make(chan int, 500)
	for i := 0; i < 500; i++ {
		upserts <- i
	}
	close(upserts)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := newRng(byte(w))
			for range upserts {
				sym := syms[rng.IntN(len(syms))]
				ref := parseRefForTest(t, sym)
				target := int64(float64(ref) * (0.99 + 0.02*rng.Float64()))
				_, err := e.alerts().UpsertAlert(ctx, &chronov1.UpsertAlertRequest{
					Symbol: sym.Name, PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
					Direction:   chronov1.Direction_DIRECTION_ABOVE,
					TargetPrice: formatForTest(t, target, sym.Decimals),
					Venue:       catalog.Venues[rng.IntN(3)], Tier: catalog.Tiers[rng.IntN(2)],
				})
				if err != nil {
					return // ctx expired mid-seed
				}
			}
		}(w)
	}

	// Feed: random symbols, prices above reference — fires the ABOVE alerts.
	feedDone := make(chan struct{})
	go func() {
		defer close(feedDone)
		rng := newRng(255)
		stream, err := e.feed().StreamTicks(ctx)
		if err != nil {
			return
		}
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				_, _ = stream.CloseAndRecv()
				return
			case <-tick.C:
				for i := 0; i < 50; i++ { // 5k/s
					sym := syms[rng.IntN(len(syms))]
					ref := parseRefForTest(t, sym)
					ask := ref * 105 / 100
					if err := stream.Send(&chronov1.TickBatch{Ticks: []*chronov1.Tick{{
						Symbol: sym.Name,
						Bid:    formatForTest(t, ref*104/100, sym.Decimals),
						Ask:    formatForTest(t, ask, sym.Decimals),
						Venue:  catalog.Venues[rng.IntN(3)], Tier: catalog.Tiers[rng.IntN(2)],
						TsUnixNanos: time.Now().UnixNano(),
					}}}); err != nil {
						return
					}
				}
			}
		}
	}()

	wg.Wait()
	<-feedDone
	cancel()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if e.core.stats.TriggersFired.Load() > 0 && received.Load() > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if e.core.stats.TriggersFired.Load() == 0 {
		t.Fatal("nothing fired under stress")
	}
	if received.Load() == 0 {
		t.Fatal("no triggers received over NATS")
	}
	if got := e.core.stats.TriggersPublishDropped.Load(); got != 0 {
		t.Fatalf("dropped publishes at 5k ticks/s: %d", got)
	}
}

func parseRefForTest(t *testing.T, s catalog.Symbol) int64 {
	t.Helper()
	v, err := price.Parse(s.Reference, s.Decimals)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func formatForTest(t *testing.T, base int64, dec uint8) string {
	t.Helper()
	return price.Format(base, dec)
}
