package feed

import (
	"testing"
	"time"

	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/price"
)

var testSyms = []catalog.Symbol{
	{Name: "BTCUSDT", Decimals: 2, Reference: "65000.00"},
	{Name: "SOLUSDT", Decimals: 4, Reference: "150.0000"},
	{Name: "PEPEUSDT", Decimals: 8, Reference: "0.00001234"},
}

func TestMarketDeterministic(t *testing.T) {
	ts := time.Unix(1_700_000_000, 0)
	a := NewMarket(testSyms, catalog.Venues, catalog.Tiers, 42)
	b := NewMarket(testSyms, catalog.Venues, catalog.Tiers, 42)
	for i := 0; i < 1000; i++ {
		ta, tb := a.Tick(ts), b.Tick(ts)
		if ta != tb {
			t.Fatalf("divergence at tick %d: %v vs %v", i, ta, tb)
		}
	}
}

func TestTicksWellFormed(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	m := NewMarket(testSyms, catalog.Venues, catalog.Tiers, 7)
	seen := map[string]bool{}
	for i := 0; i < 5000; i++ {
		msg := m.Tick(now)
		seen[msg.Venue+"/"+msg.Tier] = true

		sym := catalog.Symbol{}
		for _, s := range testSyms {
			if s.Name == msg.Symbol {
				sym = s
			}
		}
		bid, err := price.Parse(msg.Bid, sym.Decimals)
		if err != nil {
			t.Fatalf("bid %q: %v", msg.Bid, err)
		}
		ask, err := price.Parse(msg.Ask, sym.Decimals)
		if err != nil {
			t.Fatalf("ask %q: %v", msg.Ask, err)
		}
		if bid <= 0 || ask <= bid {
			t.Fatalf("bad quote: bid=%d ask=%d (%v)", bid, ask, msg)
		}
		if got := price.Format(bid, sym.Decimals); got != msg.Bid {
			t.Fatalf("bid %q does not round-trip: %q", msg.Bid, got)
		}
		// Sanity: prices stay within two orders of magnitude of the anchor.
		ref, _ := price.Parse(sym.Reference, sym.Decimals)
		if bid > ref*100 || bid < ref/100 {
			t.Fatalf("%s drifted: bid=%d ref=%d", msg.Symbol, bid, ref)
		}
	}
	// All six dim combos must interleave over 5000 ticks.
	for _, v := range catalog.Venues {
		for _, ti := range catalog.Tiers {
			if !seen[v+"/"+ti] {
				t.Fatalf("combo %s/%s never emitted", v, ti)
			}
		}
	}
}

func TestEmitterRate(t *testing.T) {
	e := NewEmitter(100) // 100/s
	interval := 10 * time.Millisecond
	total := 0
	for i := 0; i < 100; i++ { // 1 simulated second
		total += e.Take(interval)
	}
	if total < 95 || total > 105 {
		t.Fatalf("emitted %d over 1s at rate 100, want ~100", total)
	}
}
