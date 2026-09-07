package engine

import (
	"fmt"
	"os"
	"testing"
)

// benchMatch builds an engine with `alerts` alerts spread over 1000 symbols
// and measures Match on a tick that fires nothing (sparse) — every tree
// lookup is a seek + immediate stop, the dominant shape under sustained load.
func benchSparse(b *testing.B, alerts int) {
	e := New(DefaultConfig())
	defer e.Close()
	const symbols = 1000
	perSym := alerts / symbols / 2
	for s := 0; s < symbols; s++ {
		sym := fmt.Sprintf("SYM%04d", s)
		for i := 0; i < perSym; i++ {
			// GTE targets far above the tick price; LTE targets far below.
			e.Upsert(AlertSpec{ID: mkID(uint32(s*perSym*2 + i*2)), Symbol: sym,
				PriceType: PriceType(i % 4), Direction: DirGTE,
				TargetPrice: Price(1_000_000 + i), ValidFrom: 1, AutoDeactivate: true})
			e.Upsert(AlertSpec{ID: mkID(uint32(s*perSym*2 + i*2 + 1)), Symbol: sym,
				PriceType: PriceType(i % 4), Direction: DirLTE,
				TargetPrice: Price(100_000 - i), ValidFrom: 1, AutoDeactivate: true})
		}
	}
	e.Sync()
	tick := Tick{Symbol: "SYM0500", Bid: 500_000, Ask: 500_000, Mid: 500_000, Last: 500_000,
		Present: TickAllPresent(), TS: 1 << 40}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Match(&tick)
	}
}

func BenchmarkMatchSparse1M(b *testing.B) { benchSparse(b, 1_000_000) }

func BenchmarkMatchSparse10M(b *testing.B) {
	if os.Getenv("CHRONO_BENCH_10M") == "" {
		b.Skip("set CHRONO_BENCH_10M=1 to run the 10M benchmark")
	}
	benchSparse(b, 10_000_000)
}

func BenchmarkMatchSparse5M(b *testing.B) { benchSparse(b, 5_000_000) }

// BenchmarkMatchHotSymbol1M is Sparse1M collapsed onto a single symbol: the
// same 1M alerts, all on one instrument. The tick still fires nothing, but
// every seek now descends trees 1000× larger than the spread case — the
// per-symbol concentration cost of a venue-wide alert book on one name.
func BenchmarkMatchHotSymbol1M(b *testing.B) {
	e := New(DefaultConfig())
	defer e.Close()
	const alerts = 1_000_000
	for i := 0; i < alerts/2; i++ {
		e.Upsert(AlertSpec{ID: mkID(uint32(i * 2)), Symbol: "HOT",
			PriceType: PriceType(i % 4), Direction: DirGTE,
			TargetPrice: Price(1_000_000 + i), ValidFrom: 1, AutoDeactivate: true})
		e.Upsert(AlertSpec{ID: mkID(uint32(i*2 + 1)), Symbol: "HOT",
			PriceType: PriceType(i % 4), Direction: DirLTE,
			TargetPrice: Price(100_000 - i), ValidFrom: 1, AutoDeactivate: true})
	}
	e.Sync()
	tick := Tick{Symbol: "HOT", Bid: 500_000, Ask: 500_000, Mid: 500_000, Last: 500_000,
		Present: TickAllPresent(), TS: 1 << 40}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Match(&tick)
	}
}

// BenchmarkMatchFire1k measures the firing path: each timed Match fires
// 1000 fresh ACTIVE alerts (scan + CAS win + queue push), then re-arms them
// with the timer stopped. DenseSkip shows the CAS-fail cost of stale
// entries; this is its CAS-win complement — the per-fire price of a real
// gap. The tick carries only the Last price, so it is also the partial-
// Present shape (3 of 4 price types pruned by the mask). The flusher runs
// concurrently, as in production: each fire schedules a tree removal, and
// the COW copies that removal triggers land in the reported B/op — the
// matching path itself allocates nothing (0 B/op is asserted in tests).
func BenchmarkMatchFire1k(b *testing.B) {
	e := New(DefaultConfig())
	defer e.Close()
	const fires = 1000
	specs := make([]AlertSpec, fires)
	for i := range specs {
		specs[i] = AlertSpec{ID: mkID(uint32(i)), Symbol: "FIRE",
			PriceType: PriceLast, Direction: DirGTE,
			TargetPrice: Price(100_000 + i), ValidFrom: 1, AutoDeactivate: true}
	}
	for i := range specs {
		e.Upsert(specs[i])
	}
	e.Sync()
	buf := make([]Trigger, fires)
	tick := Tick{Symbol: "FIRE", Last: 1_000_000,
		Present: 1 << uint(PriceLast), TS: 1 << 40}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Match(&tick)
		b.StopTimer()
		e.Triggers().PopBatch(buf) // drain: keep the queue from filling
		for j := range specs {
			// Re-arm: the old slots are terminal (TRIGGERED), so Upsert
			// replaces them cleanly without double-removal.
			e.Upsert(specs[j])
		}
		e.Sync()
		b.StartTimer()
	}
}

// BenchmarkMatchDenseSkip measures the sustained-load shape: alerts already
// TRIGGERED, so every scan touches entries whose slot CAS fails. This is the
// per-tick cost the engine pays between price gaps.
func BenchmarkMatchDenseSkip(b *testing.B) {
	e := New(DefaultConfig())
	defer e.Close()
	const perTree = 10_000
	for i := 0; i < perTree; i++ {
		e.Upsert(AlertSpec{ID: mkID(uint32(i * 2)), Symbol: "SYM0500",
			PriceType: PriceType(i % 4), Direction: DirGTE,
			TargetPrice: Price(100_000 + i), ValidFrom: 1, AutoDeactivate: true})
		e.Upsert(AlertSpec{ID: mkID(uint32(i*2 + 1)), Symbol: "SYM0500",
			PriceType: PriceType(i % 4), Direction: DirLTE,
			TargetPrice: Price(900_000 - i), ValidFrom: 1, AutoDeactivate: true})
	}
	e.Sync()
	tick := Tick{Symbol: "SYM0500", Bid: 500_000, Ask: 500_000, Mid: 500_000, Last: 500_000,
		Present: TickAllPresent(), TS: 1 << 40}
	e.Match(&tick) // fire everything once; removals flush out
	e.Sync()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Match(&tick)
	}
}

// benchDimsSparse is benchSparse with width-2 dims: alerts spread over
// 3×7 dim combinations; the tick probes one combination.
func benchDimsSparse(b *testing.B, alerts int) {
	cfg := DefaultConfig()
	cfg.Dims = []string{"segment", "tier"}
	e := New(cfg)
	defer e.Close()
	const symbols = 1000
	perSym := alerts / symbols / 2
	for s := 0; s < symbols; s++ {
		sym := fmt.Sprintf("SYM%04d", s)
		for i := 0; i < perSym; i++ {
			dims := Dims(uint16(i%3), uint16(i%7))
			e.Upsert(AlertSpec{ID: mkID(uint32(s*perSym*2 + i*2)), Symbol: sym,
				PriceType: PriceType(i % 4), Direction: DirGTE,
				TargetPrice: Price(1_000_000 + i), ValidFrom: 1, AutoDeactivate: true,
				Dims: dims})
			e.Upsert(AlertSpec{ID: mkID(uint32(s*perSym*2 + i*2 + 1)), Symbol: sym,
				PriceType: PriceType(i % 4), Direction: DirLTE,
				TargetPrice: Price(100_000 - i), ValidFrom: 1, AutoDeactivate: true,
				Dims: dims})
		}
	}
	e.Sync()
	tick := Tick{Symbol: "SYM0500", Bid: 500_000, Ask: 500_000, Mid: 500_000, Last: 500_000,
		Present: TickAllPresent(), TS: 1 << 40, Dims: Dims(1, 2)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Match(&tick)
	}
}

func BenchmarkMatchDimsSparse1M(b *testing.B) { benchDimsSparse(b, 1_000_000) }

func BenchmarkMatchDimsSparse5M(b *testing.B) { benchDimsSparse(b, 5_000_000) }

// BenchmarkMatchDimsDenseSkip is DenseSkip at width 2 with all alerts in the
// tick's dim cell: the pure byte-width cost of the 64B entry in dense scans.
func BenchmarkMatchDimsDenseSkip(b *testing.B) {
	cfg := DefaultConfig()
	cfg.Dims = []string{"segment", "tier"}
	e := New(cfg)
	defer e.Close()
	const perTree = 10_000
	for i := 0; i < perTree; i++ {
		e.Upsert(AlertSpec{ID: mkID(uint32(i * 2)), Symbol: "SYM0500",
			PriceType: PriceType(i % 4), Direction: DirGTE,
			TargetPrice: Price(100_000 + i), ValidFrom: 1, AutoDeactivate: true,
			Dims: Dims(1, 2)})
		e.Upsert(AlertSpec{ID: mkID(uint32(i*2 + 1)), Symbol: "SYM0500",
			PriceType: PriceType(i % 4), Direction: DirLTE,
			TargetPrice: Price(900_000 - i), ValidFrom: 1, AutoDeactivate: true,
			Dims: Dims(1, 2)})
	}
	e.Sync()
	tick := Tick{Symbol: "SYM0500", Bid: 500_000, Ask: 500_000, Mid: 500_000, Last: 500_000,
		Present: TickAllPresent(), TS: 1 << 40, Dims: Dims(1, 2)}
	e.Match(&tick) // fire everything once; removals flush out
	e.Sync()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Match(&tick)
	}
}

// BenchmarkTriggerQueueRoundtrip measures the delivery queue's combined
// push+pop cost under parallel producers — the channel-backed queue that
// replaced the Vyukov ring (see TriggerQueue doc for the measurement
// rationale). Push+pop per iteration keeps the buffer from filling, so both
// sides of the queue stay in the measured path.
func BenchmarkTriggerQueueRoundtrip(b *testing.B) {
	q := NewTriggerQueue(1024)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		t := Trigger{}
		for pb.Next() {
			q.TryPush(t)
			q.Pop()
		}
	})
}
