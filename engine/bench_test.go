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
