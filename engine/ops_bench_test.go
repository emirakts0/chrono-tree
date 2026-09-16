package engine

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// benchCfg raises MaxAlerts so insert-style benchmarks can let the framework
// ramp b.N freely: capping b.N instead hangs the harness, which re-runs the
// benchmark with a larger N until one run reaches benchtime — a capped run
// never does. The raised limit costs nothing (slot chunks allocate lazily);
// any benchtime long enough to exhaust it would need a book far beyond RAM.
func benchCfg() Config {
	cfg := DefaultConfig()
	cfg.MaxAlerts = 1 << 27
	return cfg
}

// BenchmarkUpsert measures control-plane insert cost end to end (gate,
// intern, refs publish, mutation enqueue). Symbol strings are precomputed so
// the timed loop carries no fmt noise; ns/op is per upsert; Sync is outside
// the timer.
func BenchmarkUpsert(b *testing.B) {
	syms := make([]string, 500)
	for i := range syms {
		syms[i] = fmt.Sprintf("S%03d", i)
	}
	e := New(benchCfg())
	defer e.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a := AlertSpec{ID: mkID(uint32(i)), Symbol: syms[i%500],
			PriceType: PriceLast, Direction: DirGTE, TargetPrice: Price(100 + i%97),
			ValidFrom: 1}
		if err := e.Upsert(a); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	e.Sync()
}

// BenchmarkUpsertReplace measures the same-ID replace path: terminal CAS on
// the old handout, its removal enqueue, and the new handout's insert. Live
// stays at 1, so no book cap is needed.
func BenchmarkUpsertReplace(b *testing.B) {
	e := New(DefaultConfig())
	defer e.Close()
	if err := e.Upsert(AlertSpec{ID: mkID(1), Symbol: "REPL", PriceType: PriceLast,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1}); err != nil {
		b.Fatal(err)
	}
	e.Sync()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a := AlertSpec{ID: mkID(1), Symbol: "REPL", PriceType: PriceLast,
			Direction: DirGTE, TargetPrice: Price(100 + i%97), ValidFrom: 1}
		if err := e.Upsert(a); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	e.Sync()
}

// BenchmarkUpsertParallel measures insert throughput under contention: many
// goroutines upserting distinct IDs through the single gate/publish critical
// section. ns/op is wall-clock per op across all goroutines.
func BenchmarkUpsertParallel(b *testing.B) {
	syms := make([]string, 500)
	for i := range syms {
		syms[i] = fmt.Sprintf("P%03d", i)
	}
	e := New(benchCfg())
	defer e.Close()
	var next atomic.Uint32
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := next.Add(1) - 1
			a := AlertSpec{ID: mkID(i), Symbol: syms[i%500],
				PriceType: PriceLast, Direction: DirGTE, TargetPrice: Price(100 + i%97),
				ValidFrom: 1}
			if err := e.Upsert(a); err != nil {
				b.Error(err) // FailNow is main-goroutine-only under RunParallel
				return
			}
		}
	})
	b.StopTimer()
	e.Sync()
}

// BenchmarkCancel measures Cancel's CAS + removal-enqueue + refs path.
func BenchmarkCancel(b *testing.B) {
	e := New(benchCfg())
	defer e.Close()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		a := AlertSpec{ID: mkID(uint32(i)), Symbol: fmt.Sprintf("C%03d", i%500),
			PriceType: PriceLast, Direction: DirGTE, TargetPrice: 100,
			ValidFrom: 1}
		if err := e.Upsert(a); err != nil {
			b.Fatal(err)
		}
	}
	e.Sync()
	b.ResetTimer()
	b.StartTimer() // ResetTimer does not resume a timer paused by StopTimer
	for i := 0; i < b.N; i++ {
		if err := e.Cancel(mkID(uint32(i))); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	e.Sync()
}

// BenchmarkFireRemoval measures the fire path at scale: one tick crosses the
// entire book, so ns/op ≈ per-fire cost (validity, CAS, trigger push,
// removal enqueue) plus amortized scan.
func BenchmarkFireRemoval(b *testing.B) {
	e := New(benchCfg())
	defer e.Close()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		a := AlertSpec{ID: mkID(uint32(i)), Symbol: "FIRE",
			PriceType: PriceLast, Direction: DirGTE, TargetPrice: 100,
			ValidFrom: 1}
		if err := e.Upsert(a); err != nil {
			b.Fatal(err)
		}
	}
	e.Sync()
	b.ResetTimer()
	b.StartTimer() // ResetTimer does not resume a timer paused by StopTimer
	e.Match(&Tick{Symbol: "FIRE", Last: 200, Present: 1 << uint(PriceLast),
		TS: time.Now().UnixNano()})
	b.StopTimer()
	e.Sync()
}

// BenchmarkSyncLatency measures the mutation queue round-trip: one upsert
// then a Sync barrier, per op.
func BenchmarkSyncLatency(b *testing.B) {
	e := New(DefaultConfig())
	defer e.Close()
	a := AlertSpec{ID: mkID(0), Symbol: "SYN", PriceType: PriceLast,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1}
	if err := e.Upsert(a); err != nil {
		b.Fatal(err)
	}
	e.Sync()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.ID = mkID(uint32(i + 1))
		a.TargetPrice = Price(100 + i%97)
		if err := e.Upsert(a); err != nil {
			b.Fatal(err)
		}
		e.Sync()
	}
}
