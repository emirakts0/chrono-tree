package service

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/price"
)

// TestStressWatchersAndFeed: 4 watchers, parallel upserts and cancels, and
// a 5k/s tick stream for ~1.5s. Under -race this exercises the pump,
// broadcast eviction, and catalog mutation together. Assertions are the
// documented invariants: triggers fired > 0, deliveries observed, no
// watcher drops at this modest rate.
func TestStressWatchersAndFeed(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var delivered = make([]uint64, 4)
	var wwg sync.WaitGroup
	for i := range delivered {
		stream, err := e.alerts().WatchTriggers(ctx, &chronov1.WatchTriggersRequest{})
		if err != nil {
			t.Fatal(err)
		}
		wwg.Add(1)
		go func(i int, s chronov1.AlertService_WatchTriggersClient) {
			defer wwg.Done()
			for {
				if _, err := s.Recv(); err != nil {
					return
				}
				delivered[i]++
			}
		}(i, stream)
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
	wwg.Wait() // writers of delivered are done; now reads are race-free

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if e.core.stats.TriggersFired.Load() > 0 && sum(delivered) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if e.core.stats.TriggersFired.Load() == 0 {
		t.Fatal("nothing fired under stress")
	}
	if sum(delivered) == 0 {
		t.Fatal("watchers observed no deliveries")
	}
	if got := e.core.stats.WatcherDrops.Load(); got != 0 {
		t.Fatalf("watcher drops at 5k/s: %d (watchers should keep up)", got)
	}
}

func sum(v []uint64) uint64 {
	var s uint64
	for _, x := range v {
		s += x
	}
	return s
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

// TestWatcherEviction drives the eviction branch deterministically: fill a
// watcher's 256-slot channel, broadcast once more — the watcher must be
// evicted (channel closed, counted), and broadcast must not block.
func TestWatcherEviction(t *testing.T) {
	e := newEnv(t)
	w := newWatcher()
	e.core.hubMu.Lock()
	e.core.watchers[w] = struct{}{}
	e.core.hubMu.Unlock()
	tr := &chronov1.Trigger{AlertId: "eviction-probe"}
	for i := 0; i < 256; i++ {
		select {
		case w.ch <- tr:
		default:
			t.Fatal("watcher buffer smaller than 256")
		}
	}
	e.core.broadcast(tr)
	if got := e.core.WatcherCount(); got != 0 {
		t.Fatalf("watcher not evicted, count = %d", got)
	}
	// A closed buffered channel still delivers its queued items; drain the
	// 256 probes, then the channel must report closed.
	for i := 0; i < 256; i++ {
		if _, ok := <-w.ch; !ok {
			t.Fatalf("channel closed after %d of 256 buffered items", i)
		}
	}
	if _, ok := <-w.ch; ok {
		t.Fatal("evicted watcher channel should be closed")
	}
	if got := e.core.stats.WatcherDrops.Load(); got != 1 {
		t.Fatalf("WatcherDrops = %d, want 1", got)
	}
}
