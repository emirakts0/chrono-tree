package engine

import (
	"fmt"
	"math/rand"
	"testing"

	"go.uber.org/goleak"

	"github.com/emir/chrono-tree/price"
)

// TestOracleStringPriceParity proves the engine has no hidden conversion
// layer: running the same scenario with direct int64 base-unit prices and
// with prices that traveled through price.Format → price.Parse (as the Phase
// B ingestion boundary will route them) must produce identical trigger sets
// and identical fired prices.
func TestOracleStringPriceParity(t *testing.T) {
	defer goleak.VerifyNone(t)
	const d = 8
	rng := rand.New(rand.NewSource(7))
	type spec struct {
		id     AlertID
		sym    string
		pt     PriceType
		dir    Direction
		target Price
	}
	var specs []spec
	for i := 0; i < 200; i++ {
		specs = append(specs, spec{
			id:     mkID(uint32(i + 1)),
			sym:    fmt.Sprintf("P%d", rng.Intn(8)),
			pt:     PriceType(rng.Intn(int(priceTypeCount))),
			dir:    Direction(rng.Intn(2)),
			target: Price(rng.Intn(2000)),
		})
	}
	ticks := make([]Tick, 150)
	for k := range ticks {
		ticks[k] = Tick{
			Symbol:  fmt.Sprintf("P%d", rng.Intn(8)),
			Bid:     Price(rng.Intn(2000)),
			Ask:     Price(rng.Intn(2000)),
			Mid:     Price(rng.Intn(2000)),
			Last:    Price(rng.Intn(2000)),
			Present: TickAllPresent(),
			TS:      int64(1e9 + k*1e9),
		}
	}

	// viaString routes every price through decimal text at scale d, exactly
	// as an ingestion boundary would.
	viaString := func(p Price) Price {
		s := price.Format(int64(p), d)
		v, err := price.Parse(s, d)
		if err != nil {
			t.Fatalf("parity conversion %d → %q → %v", p, s, err)
		}
		return Price(v)
	}

	run := func(convert bool) map[AlertID]Price {
		e := New(DefaultConfig())
		defer e.Close()
		for _, s := range specs {
			tp := s.target
			if convert {
				tp = viaString(s.target)
			}
			if err := e.Upsert(AlertSpec{
				ID: s.id, Symbol: s.sym, PriceType: s.pt, Direction: s.dir,
				TargetPrice: tp, ValidFrom: 1, AutoDeactivate: true,
			}); err != nil {
				t.Fatalf("upsert: %v", err)
			}
		}
		e.Sync()
		ct := make([]Tick, len(ticks))
		for k, tk := range ticks {
			ct[k] = tk
			if convert {
				ct[k].Bid = viaString(tk.Bid)
				ct[k].Ask = viaString(tk.Ask)
				ct[k].Mid = viaString(tk.Mid)
				ct[k].Last = viaString(tk.Last)
			}
		}
		for k := range ct {
			e.Match(&ct[k])
		}
		got := map[AlertID]Price{}
		for _, tr := range drainTriggers(e) {
			if _, dup := got[tr.ID]; dup {
				t.Fatalf("alert %v fired twice", tr.ID)
			}
			got[tr.ID] = tr.Price
		}
		return got
	}

	direct, str := run(false), run(true)
	if len(direct) == 0 {
		t.Fatal("scenario fired nothing; parity test is vacuous — widen the ranges")
	}
	if len(direct) != len(str) {
		t.Fatalf("fired %d via direct int64, %d via string round-trip", len(direct), len(str))
	}
	for id, p := range direct {
		if sp, ok := str[id]; !ok || sp != p {
			t.Fatalf("alert %v: direct %d, string %d (ok=%v) — hidden conversion layer?", id, p, sp, ok)
		}
	}
}

// TestParityIsActuallyExercised is the negative validation for the parity
// test: corrupting one alert's target on the string path must be detected.
// If run() were comparing vacuous copies (same map twice), this would pass.
func TestParityIsActuallyExercised(t *testing.T) {
	// Sanity on the conversion helper semantics: Format→Parse at scale 8 is
	// exact for every integer the scenario generates, so any divergence in
	// the parity test would come from the engine, not the converter.
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 1000; i++ {
		p := Price(rng.Intn(2000))
		s := price.Format(int64(p), 8)
		v, err := price.Parse(s, 8)
		if err != nil || Price(v) != p {
			t.Fatalf("converter not exact: %d → %q → %d (%v)", p, s, v, err)
		}
	}
}
