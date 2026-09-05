package service

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/alertstore"
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

// TestCancelVsFireRace pins the cancel-vs-fire contract under genuine
// concurrency: a fixed ladder of BELOW alerts is driven to fire over
// several pump batches while another goroutine cancels half of them by
// id. The per-alert outcome (triggered vs cancelled) is racy by design;
// the invariants are not: every alert reaches exactly one terminal
// state, the Core's atomic gauges match the store's index counts for
// all three states, and every published trigger's record is terminal.
// The atomic check-and-flip inside bbolt's single writer is the
// arbiter — whichever of CancelBatch and MarkTriggeredBatch lands
// first wins; the loser sees a terminal record and no-ops.
func TestCancelVsFireRace(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	const m = 40
	sym, ok := catalog.Default().Symbol("BTCUSDT")
	if !ok {
		t.Fatal("BTCUSDT not in catalog")
	}

	// Fixed target ladder 64900.00 .. 64997.50 (deterministic), all BELOW
	// on the ask so a descending price path fires them across batches.
	ids := make([]string, m)
	for i := range m {
		resp, err := e.alerts().UpsertAlert(ctx, &chronov1.UpsertAlertRequest{
			Symbol: "BTCUSDT", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
			Direction:   chronov1.Direction_DIRECTION_BELOW,
			TargetPrice: formatForTest(t, 6_490_000+int64(i)*250, sym.Decimals),
			Venue:       "ATLAS", Tier: "TOP",
		})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = resp.GetAlertId()
	}

	// Prebuilt descending asks 64990.00 .. 64860.00: the final ask sits
	// below the whole ladder, so every uncancelled alert must fire.
	batches := make([]*chronov1.TickBatch, 0, 14)
	for step := 1; step <= 14; step++ {
		ask := 6_500_000 - int64(step)*10_000
		batches = append(batches, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
			tick("BTCUSDT",
				formatForTest(t, ask-50, sym.Decimals),
				formatForTest(t, ask, sym.Decimals),
				"ATLAS", "TOP"),
		}})
	}

	var wg sync.WaitGroup
	cancelErrs := make(chan error, m/2)
	wg.Add(2)
	go func() { // feed: drive the asks down through the ladder
		defer wg.Done()
		for _, b := range batches {
			if _, err := runTicks(t, e, b); err != nil {
				cancelErrs <- err
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	go func() { // canceller: half the ladder, by id
		defer wg.Done()
		for i := 0; i < m; i += 2 {
			if _, err := e.alerts().CancelAlert(ctx, &chronov1.CancelAlertRequest{AlertId: ids[i]}); err != nil &&
				status.Code(err) != codes.NotFound { // NotFound: the trigger won the race
				cancelErrs <- err
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	close(cancelErrs)
	for err := range cancelErrs {
		t.Fatal(err)
	}

	// Quiesce: every alert is either cancelled or crossed by the final
	// ask, so the terminal total must reach m.
	deadline := time.Now().Add(5 * time.Second)
	terminal := 0
	for {
		st := e.core.AlertsByState()
		terminal = st["triggered"] + st["cancelled"]
		if terminal == m || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if terminal != m {
		a, tr, cn, _ := e.store.Counts()
		t.Fatalf("quiesce: %d of %d alerts terminal, gauges = %v, store = %d/%d/%d, fired=%d published=%d live=%d",
			terminal, m, e.core.AlertsByState(), a, tr, cn,
			e.core.stats.TriggersFired.Load(), e.core.stats.TriggersPublished.Load(),
			e.core.Engine().Stats().Live)
	}

	// (a) Per-id terminal state: exactly one of triggered/cancelled,
	// never active, never missing.
	for _, id := range ids {
		v, ok := e.core.GetAlert(id)
		if !ok {
			t.Fatalf("alert %s vanished from the store", id)
		}
		if v.State != "triggered" && v.State != "cancelled" {
			t.Fatalf("alert %s state = %q, want triggered or cancelled", id, v.State)
		}
	}

	// (b) The atomic gauges match the store, per state (scan pattern
	// from TestAlertsByStateIncremental).
	for name, st := range map[string]alertstore.State{
		"active":    alertstore.StateActive,
		"triggered": alertstore.StateTriggered,
		"cancelled": alertstore.StateCancelled,
	} {
		_, total, err := e.core.store.Query(alertstore.Filter{State: st, HasState: true})
		if err != nil {
			t.Fatal(err)
		}
		if got := e.core.AlertsByState()[name]; got != total {
			t.Fatalf("gauge %q = %d, store says %d", name, got, total)
		}
	}

	// (c) Every published trigger belongs to a terminal record; nothing
	// was published for an id that does not exist.
	published := e.rec.triggers()
	terminalByState := e.core.AlertsByState()
	if len(published) > terminalByState["triggered"]+terminalByState["cancelled"] {
		t.Fatalf("published %d triggers, only %d terminal records",
			len(published), terminalByState["triggered"]+terminalByState["cancelled"])
	}
	for _, tr := range published {
		v, ok := e.core.GetAlert(tr.AlertID)
		if !ok || (v.State != "triggered" && v.State != "cancelled") {
			t.Fatalf("published trigger %s resolves to (%v, %v), want a terminal record", tr.AlertID, v.State, ok)
		}
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
