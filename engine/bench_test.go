package engine

import (
	"fmt"
	"math/rand"
	"testing"
)

// Sweep campaign constants (docs/superpowers/specs/2026-09-07-sweep-benchmarks-design.md).
const (
	sweepSymbols = 500 // SYM000..SYM499
	sweepSeconds = 12  // virtual seconds per run
	sweepTPS     = 200 // ticks per symbol per virtual second; 1.2M total
	sweepBandLo  = 100 // LTE targets live in [0, sweepBandLo)
	sweepBandHi  = 200 // GTE targets live in (sweepBandLo, sweepBandHi]
)

// sweepSchedule is a fully precomputed sustained-load scenario: per-symbol
// alert specs, per-symbol tick timelines in firing order, and the oracle —
// the exact number of alerts the schedule fires, computed by frontier replay.
type sweepSchedule struct {
	alerts    [][]AlertSpec
	ticks     [][]Tick
	total     int // total ticks across all symbols
	wantFires int // oracle: alerts crossed by the tick frontiers
}

// comboDims returns the dim vector for permutation c (0..8): (c/3, c%3).
func comboDims(c int) [dimMax]uint16 { return Dims(uint16(c/3), uint16(c%3)) }

// genSweep builds the deterministic sweep scenario. alerts is spread evenly
// over sweepSymbols, half GTE (targets evenly spaced in (100,200]) and half
// LTE (evenly spaced in [0,100)) — disjoint bands, so a GTE-phase tick's
// Descend probe never reaches LTE targets and an LTE-phase tick's Ascend
// probe never reaches GTE targets. Each virtual second advances each
// frontier by 5% of its band: GTE-phase ticks are uniform in the second's
// sub-band (lo, hi], LTE-phase ticks mirrored below 100, shuffled together.
// After sweepSeconds the population is ~60% depleted and both trees stay
// live. The oracle replays the frontiers (running max per GTE dim combo,
// running min per LTE dim combo) and counts exactly which alerts fire.
func genSweep(seed int64, alerts int, dimmed bool) *sweepSchedule {
	rng := rand.New(rand.NewSource(seed))
	combos := 1
	if dimmed {
		combos = 9
	}
	perSym := alerts / sweepSymbols
	nGTE := perSym / 2
	nLTE := perSym - nGTE
	advance := (sweepBandHi - sweepBandLo) / 20 // 5% of a 100-wide band

	s := &sweepSchedule{
		alerts: make([][]AlertSpec, sweepSymbols),
		ticks:  make([][]Tick, sweepSymbols),
	}
	for sym := 0; sym < sweepSymbols; sym++ {
		symName := fmt.Sprintf("SYM%03d", sym)
		idBase := uint32(sym * perSym)

		for i := 0; i < nGTE; i++ {
			t := Price(sweepBandLo + (sweepBandHi-sweepBandLo)*(2*i+1)/(2*nGTE))
			s.alerts[sym] = append(s.alerts[sym], AlertSpec{
				ID: mkID(idBase + uint32(i)), Symbol: symName,
				PriceType: PriceLast, Direction: DirGTE, TargetPrice: t,
				ValidFrom: 1, AutoDeactivate: true, Dims: comboDims(i % combos),
			})
		}
		for i := 0; i < nLTE; i++ {
			t := Price(sweepBandLo - sweepBandLo*(2*i+1)/(2*nLTE))
			s.alerts[sym] = append(s.alerts[sym], AlertSpec{
				ID: mkID(idBase + uint32(nGTE+i)), Symbol: symName,
				PriceType: PriceLast, Direction: DirLTE, TargetPrice: t,
				ValidFrom: 1, AutoDeactivate: true, Dims: comboDims(i % combos),
			})
		}

		maxGTE := make([]Price, combos) // running frontier per dim combo
		for i := range maxGTE {
			maxGTE[i] = Price(sweepBandLo)
		}
		minLTE := make([]Price, combos)
		for i := range minLTE {
			minLTE[i] = Price(sweepBandLo)
		}

		tickIdx := 0
		for sec := 1; sec <= sweepSeconds; sec++ {
			gLo := Price(sweepBandLo + advance*(sec-1))
			gHi := Price(sweepBandLo + advance*sec) // GTE sub-band (gLo, gHi]
			lLo := Price(sweepBandLo - advance*sec) // LTE sub-band [lLo, lHi)
			lHi := Price(sweepBandLo - advance*(sec-1))
			secTicks := make([]Tick, 0, sweepTPS)
			for i := 0; i < sweepTPS/2; i++ { // GTE phase
				p := gLo + 1 + Price(rng.Int63n(int64(gHi-gLo)))
				c := tickIdx % combos
				if p > maxGTE[c] {
					maxGTE[c] = p
				}
				secTicks = append(secTicks, Tick{Symbol: symName, Last: p,
					Present: 1 << uint(PriceLast), TS: int64(1)<<40 + int64(tickIdx),
					Dims: comboDims(c)})
				tickIdx++
			}
			for i := 0; i < sweepTPS/2; i++ { // LTE phase
				p := lLo + Price(rng.Int63n(int64(lHi-lLo)))
				c := tickIdx % combos
				if p < minLTE[c] {
					minLTE[c] = p
				}
				secTicks = append(secTicks, Tick{Symbol: symName, Last: p,
					Present: 1 << uint(PriceLast), TS: int64(1)<<40 + int64(tickIdx),
					Dims: comboDims(c)})
				tickIdx++
			}
			rng.Shuffle(len(secTicks), func(a, b int) {
				secTicks[a], secTicks[b] = secTicks[b], secTicks[a]
			})
			s.ticks[sym] = append(s.ticks[sym], secTicks...)
		}
		s.total += len(s.ticks[sym])

		// Oracle: a GTE alert fires iff its combo's frontier reached its
		// target (some GTE-phase tick price >= target); LTE mirrored.
		for _, a := range s.alerts[sym] {
			c := 0
			if dimmed {
				c = int(a.Dims[0])*3 + int(a.Dims[1])
			}
			if (a.Direction == DirGTE && maxGTE[c] >= a.TargetPrice) ||
				(a.Direction == DirLTE && minLTE[c] <= a.TargetPrice) {
				s.wantFires++
			}
		}
	}
	return s
}

// TestSweepGeneratorDeterministic pins the schedule's determinism: the same
// seed must produce identical alerts, ticks, and oracle count.
func TestSweepGeneratorDeterministic(t *testing.T) {
	a := genSweep(1, 20_000, false)
	b := genSweep(1, 20_000, false)
	if a.total != b.total || a.wantFires != b.wantFires {
		t.Fatalf("totals diverge: (%d,%d) vs (%d,%d)", a.total, a.wantFires, b.total, b.wantFires)
	}
	for sym := range a.ticks {
		if len(a.alerts[sym]) != len(b.alerts[sym]) || len(a.ticks[sym]) != len(b.ticks[sym]) {
			t.Fatalf("symbol %d: length mismatch", sym)
		}
		for i := range a.ticks[sym] {
			if a.ticks[sym][i] != b.ticks[sym][i] {
				t.Fatalf("symbol %d tick %d diverged", sym, i)
			}
		}
		for i := range a.alerts[sym] {
			if a.alerts[sym][i] != b.alerts[sym][i] {
				t.Fatalf("symbol %d alert %d diverged", sym, i)
			}
		}
	}
}

// TestSweepGeneratorCalibration checks the campaign property: every scenario
// fires 50–70% of its population (~60% target) and schedules exactly
// 500 × 200 × 12 = 1.2M ticks.
func TestSweepGeneratorCalibration(t *testing.T) {
	scenarios := []struct {
		alerts int
		dimmed bool
	}{
		{100_000, false}, {1_000_000, false}, {100_000, true}, {1_000_000, true},
	}
	for _, sc := range scenarios {
		if testing.Short() && sc.alerts > 200_000 {
			continue
		}
		s := genSweep(1, sc.alerts, sc.dimmed)
		if want := sweepSymbols * sweepTPS * sweepSeconds; s.total != want {
			t.Errorf("alerts=%d dimmed=%v: total ticks %d, want %d", sc.alerts, sc.dimmed, s.total, want)
		}
		frac := float64(s.wantFires) / float64(sc.alerts)
		if frac < 0.50 || frac > 0.70 {
			t.Errorf("alerts=%d dimmed=%v: fire fraction %.3f, want 0.50–0.70", sc.alerts, sc.dimmed, frac)
		}
	}
}

// TestSweepOracleMatchesEngine is the harness's ground-truth check: feeding
// the schedule to a real engine must fire exactly the alerts the oracle
// predicts, with zero drops. If this fails, either the generator or the
// oracle is wrong — the benchmark numbers would be meaningless.
func TestSweepOracleMatchesEngine(t *testing.T) {
	for _, dimmed := range []bool{false, true} {
		s := genSweep(1, 5_000, dimmed)
		frac := float64(s.wantFires) / 5_000
		if frac < 0.50 || frac > 0.70 {
			t.Fatalf("dimmed=%v: fire fraction %.3f, want 0.50–0.70", dimmed, frac)
		}
		cfg := DefaultConfig()
		if dimmed {
			cfg.Dims = []string{"segment", "tier"}
		}
		e := New(cfg)
		for sym := range s.alerts {
			for _, a := range s.alerts[sym] {
				if err := e.Upsert(a); err != nil {
					t.Fatal(err)
				}
			}
		}
		e.Sync()
		for sym := range s.ticks {
			for i := range s.ticks[sym] {
				e.Match(&s.ticks[sym][i])
			}
		}
		q := e.Triggers()
		n := 0
		for {
			if _, ok := q.Pop(); !ok {
				break
			}
			n++
		}
		if n != s.wantFires {
			t.Fatalf("dimmed=%v: engine fired %d, oracle predicted %d", dimmed, n, s.wantFires)
		}
		if d := q.Dropped(); d != 0 {
			t.Fatalf("dimmed=%v: %d triggers dropped", dimmed, d)
		}
		e.Close()
	}
}
