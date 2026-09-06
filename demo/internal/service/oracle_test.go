package service

import (
	"context"
	"encoding/json/v2"
	"math/rand/v2"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"

	chronov1 "github.com/emir/chrono-tree/demo/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/demo/internal/catalog"
	"github.com/emir/chrono-tree/demo/internal/pub"
	"github.com/emir/chrono-tree/internal/price"
)

func TestServiceOracle(t *testing.T) {
	e := newNATSEnv(t)
	cat := catalog.Default()
	all := cat.Symbols()
	syms := all[:25]
	rng := rand.New(rand.NewChaCha8([32]byte{1, 2, 3}))
	venues, tiers := catalog.Venues, catalog.Tiers

	type oracleAlert struct {
		id     string
		symbol string
		venue  string
		tier   string
		dir    chronov1.Direction
		target int64 // base units
		fired  bool
	}
	alerts := make([]oracleAlert, 200)
	for i := range alerts {
		sym := syms[rng.IntN(len(syms))]
		venue := venues[rng.IntN(len(venues))]
		tier := tiers[rng.IntN(len(tiers))]
		dir := chronov1.Direction_DIRECTION_ABOVE
		if rng.IntN(2) == 0 {
			dir = chronov1.Direction_DIRECTION_BELOW
		}
		ref, _ := price.Parse(sym.Reference, sym.Decimals)
		target := int64(float64(ref)*(0.9+0.2*rng.Float64()) + 0.5)
		resp, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
			Symbol: sym.Name, PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
			Direction: dir, TargetPrice: price.Format(target, sym.Decimals),
			Venue: venue, Tier: tier,
		})
		if err != nil {
			t.Fatalf("seed upsert %d: %v", i, err)
		}
		alerts[i] = oracleAlert{id: resp.GetAlertId(), symbol: sym.Name, venue: venue, tier: tier, dir: dir, target: target}
	}

	// Subscribe in the background, collecting trigger IDs from the wire —
	// the same path real consumers use.
	nc := e.natsClient(t)
	raw := make(chan *nats.Msg, 4096)
	if _, err := nc.ChanSubscribe("chrono.triggers.>", raw); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	gotIDs := make(chan string, 4096)
	go func() {
		for m := range raw {
			var tr pub.Trigger
			if err := json.Unmarshal(m.Data, &tr); err == nil {
				gotIDs <- tr.AlertID
			}
		}
		close(gotIDs)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Random tick stream; the naive evaluator applies each tick in order.
	type wireTick struct {
		symbol string
		ask    int64
		venue  string
		tier   string
	}
	stream, err := e.feed().StreamTicks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var expected []string
	for i := 0; i < 3000; i++ {
		sym := syms[rng.IntN(len(syms))]
		ref, _ := price.Parse(sym.Reference, sym.Decimals)
		ask := int64(float64(ref)*(0.85+0.3*rng.Float64()) + 0.5)
		wt := wireTick{symbol: sym.Name, ask: ask, venue: venues[rng.IntN(len(venues))], tier: tiers[rng.IntN(len(tiers))]}
		for j := range alerts {
			a := &alerts[j]
			if a.fired || a.symbol != wt.symbol || a.venue != wt.venue || a.tier != wt.tier {
				continue
			}
			above := a.dir == chronov1.Direction_DIRECTION_ABOVE
			if (above && ask >= a.target) || (!above && ask <= a.target) {
				a.fired = true
				expected = append(expected, a.id)
			}
		}
		if err := stream.Send(&chronov1.TickBatch{Ticks: []*chronov1.Tick{{
			Symbol: wt.symbol,
			Bid:    price.Format(ask*99/100, sym.Decimals),
			Ask:    price.Format(ask, sym.Decimals),
			Venue:  wt.venue, Tier: wt.tier, TsUnixNanos: time.Now().UnixNano(),
		}}}); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}

	// Collect until we have the expected count or time out.
	want := map[string]bool{}
	for _, id := range expected {
		want[id] = true
	}
	have := map[string]bool{}
	timeout := time.After(10 * time.Second)
	for uint64(len(have)) < uint64(len(want)) && len(want) > 0 {
		select {
		case id, ok := <-gotIDs:
			if !ok {
				t.Fatalf("watch ended early: have %d of %d", len(have), len(want))
			}
			if !want[id] {
				t.Fatalf("unexpected trigger %s (not in oracle's fired set)", id)
			}
			have[id] = true
		case <-timeout:
			t.Fatalf("timed out: have %d of %d expected triggers", len(have), len(want))
		}
	}
	// Drain briefly for strays — none may appear.
	strayDeadline := time.After(1500 * time.Millisecond)
	for {
		select {
		case id, ok := <-gotIDs:
			if !ok {
				return
			}
			if !want[id] {
				t.Fatalf("unexpected trigger %s after completion", id)
			}
		case <-strayDeadline:
			return
		}
	}
}
