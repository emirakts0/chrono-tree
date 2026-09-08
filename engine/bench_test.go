package engine

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
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
}

// benchSink keeps the baseline probe's loop from being optimized away.
var benchSink Price

// benchSweep runs one full sustained-load sweep as the benchmark body. The
// schedule is precomputed (Task 1), the engine preloaded and synced, and a
// trigger consumer drains out of band; the timed op is a striped worker
// fan-out replaying the per-symbol tick arrays. b.N is pinned to the
// schedule length, so run with -benchtime=1x. ns/op is per tick;
// ticks/s = 1e9 / ns/op.
//
// Protocol: run via benchSweepPinned with `-benchtime=1x -count=3` and NO
// -cpu flag; the gN sub-benchmark suffix is the pinned GOMAXPROCS. Discard
// count 1 of every cell — the framework's discovery pass, which under
// go1.27.0 runs at whatever ambient GOMAXPROCS the process has (with a
// multi-value -cpu list that is the last value, and a b.N-overriding body
// gets that run printed as its first row) — and take the median of counts
// 2–3. GOMAXPROCS is pinned inside each sub-benchmark so parallelism is
// correct by construction, discovery pass included.
//
// Workers are spawned manually rather than via b.RunParallel: RunParallel's
// contract requires the body to exhaust pb.Next() (Go 1.27 fatals otherwise),
// and draining 1.2M iterations through the framework's shared atomic cursor
// would put a contended counter in the timed path — exactly the
// generator-overhead contamination this benchmark exists to avoid. Manual
// fan-out keyed on GOMAXPROCS(0) matches the pin with zero shared state.
func benchSweep(b *testing.B, alerts int, dimmed bool) {
	s := genSweep(1, alerts, dimmed)
	if frac := float64(s.wantFires) / float64(alerts); frac < 0.50 || frac > 0.70 {
		b.Fatalf("sweep generator out of calibration: fire fraction %.3f (want 0.50–0.70)", frac)
	}
	cfg := DefaultConfig()
	// Queue capacity above any scenario's total possible fires (1M alerts):
	// the zero-drop assertion must measure engine/harness bugs, not whether
	// the single consumer kept pace in this process (the 64k default
	// overflowed deterministically on Sweep1MDims in the full campaign).
	cfg.TriggerQueueSize = 1 << 20
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
	reportMutationDrops(b) // cumulative; row's own drops = end − start

	// run drives the striped fan-out under the benchmark timer: worker w
	// replays symbols w, w+workers, ... — identical per-symbol timelines, so
	// work is perfectly balanced and no worker shares a cursor.
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

	// Generator-overhead probe (debug, not part of the campaign): the same
	// striped fan-out with the engine call removed. Its ns/op is the floor
	// every real number must dominate.
	if os.Getenv("CHRONO_BENCH_BASELINE") == "1" {
		run(func(t *Tick) { benchSink = t.Last })
		return
	}

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
	reportMutationDrops(b)
}

// benchSweepPinned runs the sweep scenario at GOMAXPROCS 1, 4, and 12 as
// named sub-benchmarks. Parallelism is pinned inside each sub-benchmark
// (before benchSweep's fan-out reads GOMAXPROCS(0)) so the harness cannot
// be poisoned by ambient scheduler state: a multi-value -cpu list under
// go1.27.0 executes the framework's discovery pass at the last -cpu value's
// GOMAXPROCS, and a b.N-overriding body gets that run reported as a result.
// Do not pass -cpu to the campaign; the gN suffix carries the value.
func benchSweepPinned(b *testing.B, alerts int, dimmed bool) {
	for _, g := range []int{1, 4, 12} {
		b.Run(fmt.Sprintf("g%d", g), func(b *testing.B) {
			old := runtime.GOMAXPROCS(g)
			defer runtime.GOMAXPROCS(old)
			benchSweep(b, alerts, dimmed)
		})
	}
}

// BenchmarkSweep100k: 100k alerts across 500 symbols, no match dims.
func BenchmarkSweep100k(b *testing.B) { benchSweepPinned(b, 100_000, false) }

// BenchmarkSweep1M: 1M alerts across 500 symbols — 10× the alert density per
// symbol, so each tick fires ~10× more alerts than the 100k row.
func BenchmarkSweep1M(b *testing.B) { benchSweepPinned(b, 1_000_000, false) }

// BenchmarkSweep100kDims: the 100k scenario with 2 match dims (9
// combinations); alerts and tick load are split evenly across the cells.
func BenchmarkSweep100kDims(b *testing.B) { benchSweepPinned(b, 100_000, true) }

// BenchmarkSweep1MDims: the 1M scenario with 2 match dims (9 combinations).
func BenchmarkSweep1MDims(b *testing.B) { benchSweepPinned(b, 1_000_000, true) }
