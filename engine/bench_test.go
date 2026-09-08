package engine

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// Sweep campaign constants.
const (
	sweepSymbols = 500 // SYM000..SYM499
	sweepSeconds = 12  // virtual seconds per run
	sweepTPS     = 200 // ticks per symbol per virtual second; 1.2M total
	sweepBandLo  = 100 // LTE targets live in [0, sweepBandLo)
	sweepBandHi  = 200 // GTE targets live in (sweepBandLo, sweepBandHi]

	// sweepHoldFires pins total fires in hold scenarios (genSweep
	// fixedFires > 0), so ns/op varies with the resident population only.
	sweepHoldFires = 50_000
)

// sweepSchedule is a precomputed sustained-load scenario: per-symbol alert
// specs, per-symbol tick timelines in firing order, and the oracle — the
// exact number of alerts the schedule fires.
type sweepSchedule struct {
	alerts    [][]AlertSpec
	ticks     [][]Tick
	total     int // total ticks across all symbols
	wantFires int // oracle: alerts crossed by the tick frontiers
}

// comboDims returns the dim vector for permutation c (0..8): (c/3, c%3).
func comboDims(c int) [dimMax]uint16 { return Dims(uint16(c/3), uint16(c%3)) }

// gteTarget places GTE target i of n. Standard mode (fireN 0) spreads all
// targets over the full band. Hold mode puts the first fireN targets inside
// the frontier span (they all fire) and the rest one unit beyond it — the +1
// keeps integer truncation from landing a non-firing target on the span's
// top price, which the frontier does reach.
func gteTarget(i, n, fireN, span int) Price {
	if fireN == 0 {
		return Price(sweepBandLo + (sweepBandHi-sweepBandLo)*(2*i+1)/(2*n))
	}
	if i < fireN {
		return Price(sweepBandLo + span*(2*i+1)/(2*fireN))
	}
	j, rest := i-fireN, n-fireN
	return Price(sweepBandLo + span + 1 + (sweepBandHi-sweepBandLo-span-1)*(2*j+1)/(2*rest))
}

// lteTarget mirrors gteTarget below the band midpoint.
func lteTarget(i, n, fireN, span int) Price {
	if fireN == 0 {
		return Price(sweepBandLo - sweepBandLo*(2*i+1)/(2*n))
	}
	if i < fireN {
		return Price(sweepBandLo - span*(2*i+1)/(2*fireN))
	}
	j, rest := i-fireN, n-fireN
	return Price((sweepBandLo - span - 1) * (2*j + 1) / (2 * rest))
}

// genSweep builds the deterministic sweep scenario. Alerts are spread evenly
// over sweepSymbols, half GTE (evenly spaced in (100,200]) and half LTE
// (evenly spaced in [0,100)) — disjoint bands, so a GTE-phase tick's Descend
// probe never reaches LTE targets and an LTE-phase tick's Ascend probe never
// reaches GTE targets. Each virtual second advances each frontier by 5% of
// its band, with the second's ticks uniform in the sub-band and shuffled.
// After sweepSeconds the population is ~60% depleted and both trees stay
// live. The oracle replays the frontiers (running max per GTE dim combo,
// running min per LTE dim combo) and counts exactly which alerts fire.
// With fixedFires > 0 (hold mode) the fire count is pinned instead: targets
// inside the frontier span all fire and the rest sit beyond it, so the
// schedule always fires fixedFires regardless of population.
func genSweep(seed int64, alerts, fixedFires int, dimmed bool) *sweepSchedule {
	rng := rand.New(rand.NewSource(seed))
	combos := 1
	if dimmed {
		combos = 9
	}
	perSym := alerts / sweepSymbols
	nGTE := perSym / 2
	nLTE := perSym - nGTE
	advance := (sweepBandHi - sweepBandLo) / 20 // 5% of a 100-wide band
	// Hold mode (fixedFires > 0) pins the fire count: fireGTE/fireLTE
	// targets per symbol sit inside the frontier span and all fire; the
	// rest sit beyond the final frontier, where no scan ever reaches.
	fireGTE, fireLTE := 0, 0
	if fixedFires > 0 {
		perSymFires := fixedFires / sweepSymbols
		if perSymFires%2 != 0 || perSymFires/2 > nGTE || perSymFires/2 > nLTE {
			panic(fmt.Sprintf("genSweep: fixedFires %d not realizable for %d alerts", fixedFires, alerts))
		}
		fireGTE, fireLTE = perSymFires/2, perSymFires/2
	}
	// span is the frontier's total travel: every GTE target in
	// (100, 100+span] and LTE target in [100-span, 100) is crossed.
	span := advance * sweepSeconds

	s := &sweepSchedule{
		alerts: make([][]AlertSpec, sweepSymbols),
		ticks:  make([][]Tick, sweepSymbols),
	}
	for sym := 0; sym < sweepSymbols; sym++ {
		symName := fmt.Sprintf("SYM%03d", sym)
		idBase := uint32(sym * perSym)

		for i := 0; i < nGTE; i++ {
			t := gteTarget(i, nGTE, fireGTE, span)
			s.alerts[sym] = append(s.alerts[sym], AlertSpec{
				ID: mkID(idBase + uint32(i)), Symbol: symName,
				PriceType: PriceLast, Direction: DirGTE, TargetPrice: t,
				ValidFrom: 1, AutoDeactivate: true, Dims: comboDims(i % combos),
			})
		}
		for i := 0; i < nLTE; i++ {
			t := lteTarget(i, nLTE, fireLTE, span)
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
	for _, tc := range []struct {
		alerts     int
		fixedFires int
	}{{20_000, 0}, {100_000, sweepHoldFires}} {
		a := genSweep(1, tc.alerts, tc.fixedFires, false)
		b := genSweep(1, tc.alerts, tc.fixedFires, false)
		if a.total != b.total || a.wantFires != b.wantFires {
			t.Fatalf("fires=%d: totals diverge: (%d,%d) vs (%d,%d)",
				tc.fixedFires, a.total, a.wantFires, b.total, b.wantFires)
		}
		for sym := range a.ticks {
			if len(a.alerts[sym]) != len(b.alerts[sym]) || len(a.ticks[sym]) != len(b.ticks[sym]) {
				t.Fatalf("fires=%d: symbol %d: length mismatch", tc.fixedFires, sym)
			}
			for i := range a.ticks[sym] {
				if a.ticks[sym][i] != b.ticks[sym][i] {
					t.Fatalf("fires=%d: symbol %d tick %d diverged", tc.fixedFires, sym, i)
				}
			}
			for i := range a.alerts[sym] {
				if a.alerts[sym][i] != b.alerts[sym][i] {
					t.Fatalf("fires=%d: symbol %d alert %d diverged", tc.fixedFires, sym, i)
				}
			}
		}
	}
}

// TestSweepGeneratorCalibration checks that every standard scenario fires
// 50–70% of its population, every hold scenario fires 50k ± 1%, and all
// schedule exactly 500 × 200 × 12 = 1.2M ticks.
func TestSweepGeneratorCalibration(t *testing.T) {
	scenarios := []struct {
		alerts     int
		fixedFires int
		dimmed     bool
	}{
		{100_000, 0, false}, {1_000_000, 0, false}, {5_000_000, 0, false},
		{100_000, 0, true}, {1_000_000, 0, true}, {5_000_000, 0, true},
		{100_000, sweepHoldFires, false}, {1_000_000, sweepHoldFires, false}, {5_000_000, sweepHoldFires, false},
		{100_000, sweepHoldFires, true}, {1_000_000, sweepHoldFires, true}, {5_000_000, sweepHoldFires, true},
	}
	for _, sc := range scenarios {
		if testing.Short() && sc.alerts > 200_000 {
			continue
		}
		s := genSweep(1, sc.alerts, sc.fixedFires, sc.dimmed)
		if want := sweepSymbols * sweepTPS * sweepSeconds; s.total != want {
			t.Errorf("alerts=%d fires=%d dimmed=%v: total ticks %d, want %d",
				sc.alerts, sc.fixedFires, sc.dimmed, s.total, want)
		}
		switch {
		case sc.fixedFires > 0:
			if d := s.wantFires - sc.fixedFires; d < -sc.fixedFires/100 || d > sc.fixedFires/100 {
				t.Errorf("alerts=%d dimmed=%v: fires %d, want %d ±1%%",
					sc.alerts, sc.dimmed, s.wantFires, sc.fixedFires)
			}
		default:
			frac := float64(s.wantFires) / float64(sc.alerts)
			if frac < 0.50 || frac > 0.70 {
				t.Errorf("alerts=%d dimmed=%v: fire fraction %.3f, want 0.50–0.70",
					sc.alerts, sc.dimmed, frac)
			}
		}
	}
}

// checkEngineMatchesOracle feeds a schedule to a fresh engine and verifies
// the engine fires exactly the oracle's count with zero drops.
func checkEngineMatchesOracle(t *testing.T, s *sweepSchedule, dimmed bool) {
	t.Helper()
	cfg := DefaultConfig()
	if dimmed {
		cfg.Dims = []string{"segment", "tier"}
	}
	e := New(cfg)
	defer e.Close()
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
}

// TestSweepOracleMatchesEngine is the ground-truth check: feeding the
// schedule to a real engine must fire exactly the alerts the oracle
// predicts, with zero drops.
func TestSweepOracleMatchesEngine(t *testing.T) {
	for _, dimmed := range []bool{false, true} {
		s := genSweep(1, 5_000, 0, dimmed)
		frac := float64(s.wantFires) / 5_000
		if frac < 0.50 || frac > 0.70 {
			t.Fatalf("dimmed=%v: fire fraction %.3f, want 0.50–0.70", dimmed, frac)
		}
		checkEngineMatchesOracle(t, s, dimmed)
	}
}

// TestSweepHoldOracleMatchesEngine is the hold-mode ground truth: the 100k
// hold scenario (50% fire fraction, the densest hold case) must fire exactly
// what the oracle predicts. Smaller populations can't realize 50k fires
// (need ≥ 100 firing targets per symbol), so 100k is the floor.
func TestSweepHoldOracleMatchesEngine(t *testing.T) {
	for _, dimmed := range []bool{false, true} {
		s := genSweep(1, 100_000, sweepHoldFires, dimmed)
		if d := s.wantFires - sweepHoldFires; d < -sweepHoldFires/100 || d > sweepHoldFires/100 {
			t.Fatalf("dimmed=%v: hold generator out of calibration: fires %d, want %d ±1%%",
				dimmed, s.wantFires, sweepHoldFires)
		}
		checkEngineMatchesOracle(t, s, dimmed)
	}
}

// benchSweep runs one full sustained-load sweep as the benchmark body. The
// schedule is precomputed, the engine preloaded and synced, and a trigger
// consumer drains out of band; the timed op is a striped worker fan-out
// replaying the per-symbol tick arrays. b.N is pinned to the schedule
// length, so run with -benchtime=1x; ns/op is per tick.
func benchSweep(b *testing.B, alerts, fixedFires int, dimmed bool) {
	s := genSweep(1, alerts, fixedFires, dimmed)
	switch {
	case fixedFires > 0:
		if d := s.wantFires - fixedFires; d < -fixedFires/100 || d > fixedFires/100 {
			b.Fatalf("hold generator out of calibration: fires %d, want %d ±1%%", s.wantFires, fixedFires)
		}
	default:
		if frac := float64(s.wantFires) / float64(alerts); frac < 0.50 || frac > 0.70 {
			b.Fatalf("sweep generator out of calibration: fire fraction %.3f (want 0.50–0.70)", frac)
		}
	}
	cfg := DefaultConfig()
	// Queue capacity above any scenario's total possible fires — hold fires
	// are bounded by fixedFires; standard fires are bounded by alerts —
	// rounded up to the next power of two.
	qcap := 1 << 20
	if fixedFires == 0 {
		for qcap < alerts {
			qcap <<= 1
		}
	}
	cfg.TriggerQueueSize = qcap
	if dimmed {
		cfg.Dims = []string{"segment", "tier"}
	}
	e := New(cfg)
	defer e.Close()
	for sym := range s.alerts {
		for _, a := range s.alerts[sym] {
			if err := e.Upsert(a); err != nil {
				b.Fatal(err)
			}
		}
	}
	e.Sync()

	// run drives the striped fan-out under the benchmark timer: worker w
	// replays symbols w, w+workers, ..., so work is perfectly balanced and
	// no worker shares a cursor.
	workers := runtime.GOMAXPROCS(0)
	run := func(match func(*Tick)) {
		b.ResetTimer()
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for sym := w; sym < sweepSymbols; sym += workers {
					ticks := s.ticks[sym]
					for i := range ticks {
						match(&ticks[i])
					}
				}
			}(w)
		}
		wg.Wait()
		b.StopTimer()
	}

	b.N = s.total
	b.ReportAllocs()

	done := make(chan struct{})
	finished := make(chan struct{})
	var fired atomic.Int64
	go func() {
		c := e.Triggers().C()
		for {
			select {
			case <-c:
				fired.Add(1)
			case <-done:
				for { // drain the tail non-blocking, then signal
					select {
					case <-c:
						fired.Add(1)
					default:
						close(finished)
						return
					}
				}
			}
		}
	}()

	run(func(t *Tick) { e.Match(t) })
	close(done)
	<-finished

	if got := fired.Load(); got != int64(s.wantFires) {
		b.Fatalf("consumer counted %d fires, oracle says %d — harness mismatch", got, s.wantFires)
	}
	if d := e.Triggers().Dropped(); d != 0 {
		b.Fatalf("%d triggers dropped — fire calibration invalid", d)
	}
}

// benchSweepPinned runs the sweep scenario at GOMAXPROCS 1, 4, and 12 as
// named sub-benchmarks, pinning parallelism inside each sub-benchmark before
// the fan-out reads GOMAXPROCS(0). Do not pass -cpu; the gN suffix carries
// the value.
func benchSweepPinned(b *testing.B, alerts, fixedFires int, dimmed bool) {
	for _, g := range []int{1, 4, 12} {
		b.Run(fmt.Sprintf("g%d", g), func(b *testing.B) {
			old := runtime.GOMAXPROCS(g)
			defer runtime.GOMAXPROCS(old)
			benchSweep(b, alerts, fixedFires, dimmed)
		})
	}
}

// BenchmarkSweep100k: 100k alerts across 500 symbols, no match dims.
func BenchmarkSweep100k(b *testing.B) { benchSweepPinned(b, 100_000, 0, false) }

// BenchmarkSweep1M: 1M alerts across 500 symbols.
func BenchmarkSweep1M(b *testing.B) { benchSweepPinned(b, 1_000_000, 0, false) }

// BenchmarkSweep100kDims: the 100k scenario with 2 match dims (9
// combinations).
func BenchmarkSweep100kDims(b *testing.B) { benchSweepPinned(b, 100_000, 0, true) }

// BenchmarkSweep1MDims: the 1M scenario with 2 match dims (9 combinations).
func BenchmarkSweep1MDims(b *testing.B) { benchSweepPinned(b, 1_000_000, 0, true) }

// BenchmarkSweep5M: 5M alerts across the same 500 symbols.
func BenchmarkSweep5M(b *testing.B) { benchSweepPinned(b, 5_000_000, 0, false) }

// BenchmarkSweep5MDims: the 5M scenario with 2 match dims (9 combinations).
func BenchmarkSweep5MDims(b *testing.B) { benchSweepPinned(b, 5_000_000, 0, true) }

// BenchmarkSweepHold100k: 100k alerts, total fires pinned at 50k. The hold
// family isolates resident-population cost (tree depth, cache/heap pressure,
// slot-arena footprint) from fire/density cost: fires and scan work are
// constant across the family while population varies.
func BenchmarkSweepHold100k(b *testing.B) {
	benchSweepPinned(b, 100_000, sweepHoldFires, false)
}

// BenchmarkSweepHold1M: 1M alerts, total fires pinned at 50k.
func BenchmarkSweepHold1M(b *testing.B) {
	benchSweepPinned(b, 1_000_000, sweepHoldFires, false)
}

// BenchmarkSweepHold5M: 5M alerts, total fires pinned at 50k.
func BenchmarkSweepHold5M(b *testing.B) {
	benchSweepPinned(b, 5_000_000, sweepHoldFires, false)
}

// BenchmarkSweepHold100kDims: the 100k hold scenario with 2 match dims.
func BenchmarkSweepHold100kDims(b *testing.B) {
	benchSweepPinned(b, 100_000, sweepHoldFires, true)
}

// BenchmarkSweepHold1MDims: the 1M hold scenario with 2 match dims.
func BenchmarkSweepHold1MDims(b *testing.B) {
	benchSweepPinned(b, 1_000_000, sweepHoldFires, true)
}

// BenchmarkSweepHold5MDims: the 5M hold scenario with 2 match dims.
func BenchmarkSweepHold5MDims(b *testing.B) {
	benchSweepPinned(b, 5_000_000, sweepHoldFires, true)
}
