package engine

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// TestOracle compares the engine against a brute-force linear scan over the
// same alert set and tick sequence: the set of fired alert IDs and the price
// each fired at must match exactly.
func TestOracle(t *testing.T) {
	defer goleak.VerifyNone(t)
	rng := rand.New(rand.NewSource(1))
	e := New(DefaultConfig())
	defer e.Close()

	const nAlerts, nTicks = 500, 300
	const base = int64(1 << 40) // tick timestamps: base + k*second
	const step = int64(1e9)

	type rec struct {
		spec AlertSpec
	}
	var recs []rec
	for i := 0; i < nAlerts; i++ {
		validFrom := base - int64(rng.Intn(2))*step
		var expires int64
		if rng.Intn(4) == 0 {
			expires = base + int64(rng.Intn(nTicks/10))*step
		}
		s := AlertSpec{
			ID:             mkID(uint32(i + 1)),
			Symbol:         fmt.Sprintf("S%d", rng.Intn(20)),
			PriceType:      PriceType(rng.Intn(int(priceTypeCount))),
			Direction:      Direction(rng.Intn(2)),
			TargetPrice:    float64(rng.Intn(400)) + float64(rng.Intn(4))/4,
			ValidFrom:      validFrom,
			Expires:        expires,
			AutoDeactivate: true,
		}
		// The generator can emit expires == validFrom (and does at seed 1,
		// i=1), which validate() rejects as a zero-lifetime alert. Sanitize
		// before Upsert; recs records the sanitized spec, so the brute-force
		// oracle below evaluates exactly what the engine holds.
		if s.Expires != 0 && s.Expires <= s.ValidFrom {
			s.Expires = s.ValidFrom + step
		}
		if err := e.Upsert(s); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
		recs = append(recs, rec{spec: s})
	}
	e.Sync()

	ticks := make([]Tick, nTicks)
	for k := range ticks {
		ticks[k] = Tick{
			Symbol:  fmt.Sprintf("S%d", rng.Intn(20)),
			Bid:     float64(rng.Intn(400)) + 0.5,
			Ask:     float64(rng.Intn(400)) + 0.5,
			Mid:     float64(rng.Intn(400)) + 0.5,
			Last:    float64(rng.Intn(400)) + 0.5,
			Present: TickAllPresent(),
			TS:      base + int64(k)*step,
		}
	}

	// Brute force: per alert, the FIRST tick (in order) satisfying all
	// conditions, mirroring the spec's filter stages.
	expected := map[AlertID]float64{}
	for _, r := range recs {
		for _, tk := range ticks {
			if tk.Symbol != r.spec.Symbol {
				continue
			}
			price := priceOf(&tk, r.spec.PriceType)
			if tk.TS < r.spec.ValidFrom {
				continue
			}
			if r.spec.Expires != 0 && tk.TS >= r.spec.Expires {
				continue
			}
			hit := (r.spec.Direction == DirGTE && price >= r.spec.TargetPrice) ||
				(r.spec.Direction == DirLTE && price <= r.spec.TargetPrice)
			if hit {
				expected[r.spec.ID] = price
				break // ONCE semantics
			}
		}
	}

	for k := range ticks {
		e.Match(&ticks[k])
	}
	got := map[AlertID]float64{}
	for _, tr := range drainTriggers(e) {
		if _, dup := got[tr.ID]; dup {
			t.Fatalf("alert %v fired twice", tr.ID)
		}
		got[tr.ID] = tr.Price
	}

	if len(got) != len(expected) {
		t.Fatalf("fired %d alerts, expected %d", len(got), len(expected))
	}
	for id, price := range expected {
		if gp, ok := got[id]; !ok || gp != price {
			t.Fatalf("alert %v: fired at %v (ok=%v), expected %v", id, gp, ok, price)
		}
	}
}

func mkID(v uint32) AlertID {
	return AlertID{0x01, 0x91, 0xb4, 0x02, byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

// TestStressRace hammers one hot symbol with concurrent tick producers and
// mutators. Invariants: no alert fires more times than it was activated
// (exactly-once per activation — Upsert on an existing ID is a documented
// replace that re-arms the alert with a fresh Active slot, so a re-upserted
// ID may legitimately fire again); no panic under -race; no goroutine leaks.
func TestStressRace(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	const nAlerts = 2000

	// arms counts activations per ID: the initial Upsert plus every mutator
	// Upsert that replaced it. fires(ID) > arms(ID) is a genuine double fire.
	var armsMu sync.Mutex
	arms := make(map[AlertID]int, nAlerts)
	for i := 0; i < nAlerts; i++ {
		id := mkID(uint32(i + 1))
		if err := e.Upsert(AlertSpec{
			ID: id, Symbol: "HOT", PriceType: PriceType(i % 4),
			Direction: Direction(i % 2), TargetPrice: float64(100 + i%400),
			ValidFrom: 1, AutoDeactivate: true,
		}); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
		arms[id] = 1
	}
	e.Sync()

	const producers, mutators = 4, 2
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(p)))
			price := 300.0
			for {
				select {
				case <-stop:
					return
				default:
				}
				price += rng.NormFloat64()
				if price < 1 {
					price = 1
				}
				e.Match(&Tick{Symbol: "HOT", Bid: price, Ask: price, Mid: price,
					Last: price, Present: TickAllPresent(), TS: time.Now().UnixNano()})
			}
		}(p)
	}
	for m := 0; m < mutators; m++ {
		wg.Add(1)
		go func(m int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(1000 + int64(m)))
			for i := 0; i < 2000; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := mkID(uint32(rng.Intn(nAlerts) + 1))
				switch rng.Intn(3) {
				case 0:
					err := e.Upsert(AlertSpec{ID: id, Symbol: "HOT",
						PriceType: PriceBid, Direction: DirGTE,
						TargetPrice: float64(100 + rng.Intn(400)),
						ValidFrom:   1, AutoDeactivate: true})
					if err == nil {
						armsMu.Lock()
						arms[id]++
						armsMu.Unlock()
					}
				case 1:
					e.SetStatus(id, Status(rng.Intn(3)+1)) // may fail; ignore
				case 2:
					e.Cancel(id) // may fail; ignore
				}
			}
		}(m)
	}
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	e.Sync()

	fired := map[AlertID]int{}
	for _, tr := range drainTriggers(e) {
		fired[tr.ID]++
	}
	// Exactly-once per activation: an alert may fire once per Upsert that
	// activated it (replace re-arms), never more. IDs never re-upserted by a
	// mutator must fire at most once.
	for id, n := range fired {
		if n > arms[id] {
			t.Fatalf("alert %v fired %d times but was activated only %d times", id, n, arms[id])
		}
	}
}
