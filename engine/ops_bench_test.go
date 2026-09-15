package engine

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkUpsert measures control-plane insert cost end to end (spec build,
// refs publish, mutation enqueue). ns/op is per upsert; Sync is outside the
// timer.
func BenchmarkUpsert(b *testing.B) {
	e := New(DefaultConfig())
	defer e.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a := AlertSpec{ID: mkID(uint32(i)), Symbol: fmt.Sprintf("S%03d", i%500),
			PriceType: PriceLast, Direction: DirGTE, TargetPrice: Price(100 + i%97),
			ValidFrom: 1, AutoDeactivate: true}
		if err := e.Upsert(a); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	e.Sync()
}

// benchMaxBook caps b.N: the framework's calibration ramp can drive N past
// DefaultConfig's MaxAlerts (10M) when the timed region is far cheaper than
// the untimed upsert setup, tripping ErrAlertLimit. 1M matches the scale of
// the repo's sweep benchmarks.
const benchMaxBook = 1_000_000

// BenchmarkCancel measures Cancel's CAS + removal-enqueue + refs path.
func BenchmarkCancel(b *testing.B) {
	if b.N > benchMaxBook {
		b.N = benchMaxBook
	}
	e := New(DefaultConfig())
	defer e.Close()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		a := AlertSpec{ID: mkID(uint32(i)), Symbol: fmt.Sprintf("C%03d", i%500),
			PriceType: PriceLast, Direction: DirGTE, TargetPrice: 100,
			ValidFrom: 1, AutoDeactivate: true}
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
	if b.N > benchMaxBook {
		b.N = benchMaxBook
	}
	e := New(DefaultConfig())
	defer e.Close()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		a := AlertSpec{ID: mkID(uint32(i)), Symbol: "FIRE",
			PriceType: PriceLast, Direction: DirGTE, TargetPrice: 100,
			ValidFrom: 1, AutoDeactivate: true}
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
