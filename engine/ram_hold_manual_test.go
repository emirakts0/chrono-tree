package engine

// Manual RAM hold: insert RAM_ALERTS randomly distributed, never-expiring
// alerts, sync, and go idle — nothing fires, the population just sits in
// RAM. Measures steady-state heap cost per alert.
// Run: RAM_HOLD=1 RAM_ALERTS=100000 go test ./engine -run TestRAMHold -v -timeout 30m
// One process per population size (fresh heap baseline each run).
// Temporary analysis harness — not part of the suite.

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sync/atomic"
	"testing"
	"time"
)

// holdPopulate inserts alerts randomly distributed over Zipf-weighted symbols
// with uniform targets; never-expiring, nothing fires.
func holdPopulate(t *testing.T, e *Engine, alerts int) {
	t.Helper()
	const symbols = 500
	rng := rand.New(rand.NewSource(1))
	zipf := rand.NewZipf(rng, 1.1, 1, symbols-1)
	names := make([]string, symbols)
	for i := range names {
		names[i] = fmt.Sprintf("SYM%03d", i)
	}
	for i := 0; i < alerts; i++ {
		a := AlertSpec{
			ID:          mkID(uint32(i)),
			Symbol:      names[zipf.Uint64()],
			PriceType:   PriceLast,
			Direction:   Direction(rng.Intn(2)),
			TargetPrice: Price(1000 + rng.Int63n(999_000)),
			ValidFrom:   1,
		}
		if err := e.Upsert(a); err != nil {
			t.Fatal(err)
		}
	}
	e.Sync()
	for e.expQ.pending() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(2 * e.cfg.ReaperInterval)
}

func TestRAMHold(t *testing.T) {
	if os.Getenv("RAM_HOLD") == "" {
		t.Skip("set RAM_HOLD=1 to run")
	}
	alerts := 1_000_000
	if v := os.Getenv("RAM_ALERTS"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &alerts); err != nil {
			t.Fatalf("bad RAM_ALERTS %q: %v", v, err)
		}
	}
	gc := func() runtime.MemStats {
		var m runtime.MemStats
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&m)
		return m
	}
	base := gc()
	baseRSS := rssKB(t)

	cfg := DefaultConfig()
	e := New(cfg)
	holdPopulate(t, e, alerts)

	idle := gc()
	delta := idle.HeapAlloc - base.HeapAlloc
	t.Logf("alerts=%d: HeapAlloc=%d MiB RSS=%d MiB (ΔRSS=%d MiB) | engine delta=%d MiB (%.1f B/alert)",
		alerts, idle.HeapAlloc>>20, rssKB(t)>>10, (rssKB(t)-baseRSS)>>10,
		delta>>20, float64(delta)/float64(alerts))
	t.Logf("census: live=%d slots=%d parked=%d symbols=%d",
		e.Stats().Live, e.slots.next, len(e.parked), e.Stats().Symbols)

	// Heap profile to a stable path for inuse_space attribution.
	profPath := "/tmp/ram_hold.pb.gz"
	f, err := os.Create(profPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.WriteHeapProfile(f); err != nil {
		t.Fatal(err)
	}
	f.Close()
	t.Logf("heap profile: %s", profPath)
	e.Close()
}

// TestRAMHoldSettle observes whether RSS converges to live heap while the
// process sits fully idle after populate. Samples HeapAlloc / RSS /
// HeapReleased once per second for RAM_SETTLE_SECS (default 60), with no
// forced GC — only the runtime's own background scavenger at work. Ends
// with a debug.FreeOSMemory control: any immediate RSS drop there is
// unscavenged free pages, not live data.
// Run: RAM_SETTLE=1 RAM_ALERTS=1000000 go test ./engine -run TestRAMHoldSettle -v -count=1 -timeout 30m
func TestRAMHoldSettle(t *testing.T) {
	if os.Getenv("RAM_SETTLE") == "" {
		t.Skip("set RAM_SETTLE=1 to run")
	}
	alerts := 1_000_000
	if v := os.Getenv("RAM_ALERTS"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &alerts); err != nil {
			t.Fatalf("bad RAM_ALERTS %q: %v", v, err)
		}
	}
	secs := 60
	if v := os.Getenv("RAM_SETTLE_SECS"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &secs); err != nil {
			t.Fatalf("bad RAM_SETTLE_SECS %q: %v", v, err)
		}
	}

	e := New(DefaultConfig())
	holdPopulate(t, e, alerts)

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	start := time.Now()
	for i := 1; i <= secs; i++ {
		time.Sleep(time.Until(start.Add(time.Duration(i) * time.Second)))
		runtime.ReadMemStats(&m)
		t.Logf("t=%2ds HeapAlloc=%4d MiB RSS=%4d MiB HeapReleased=%4d MiB NumGC=%d",
			i, m.HeapAlloc>>20, rssKB(t)>>10, m.HeapReleased>>20, m.NumGC)
	}
	debug.FreeOSMemory()
	runtime.ReadMemStats(&m)
	t.Logf("after FreeOSMemory: HeapAlloc=%d MiB RSS=%d MiB HeapReleased=%d MiB",
		m.HeapAlloc>>20, rssKB(t)>>10, m.HeapReleased>>20)
	e.Close()
}

// TestRAMFlow asks whether the populate burst's free heap pages get returned
// to the OS once realistic traffic flows: after holdPopulate (RSS spiked),
// run steady churn — ticks driving a bounded random-walk price per symbol
// (gradual frontier crossing, sustained firing), cancels + fresh upserts
// keeping the population ~N, trigger queue drained — and sample once per
// second. No forced GC: only the allocation-driven GC cycles the traffic
// itself causes. Ends with a FreeOSMemory control.
//
//	Run: RAM_FLOW=1 RAM_ALERTS=1000000 RAM_FLOW_SECS=180 \
//	     go test ./engine -run TestRAMFlow -v -count=1 -timeout 30m
func TestRAMFlow(t *testing.T) {
	if os.Getenv("RAM_FLOW") == "" {
		t.Skip("set RAM_FLOW=1 to run")
	}
	alerts := 1_000_000
	if v := os.Getenv("RAM_ALERTS"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &alerts); err != nil {
			t.Fatalf("bad RAM_ALERTS %q: %v", v, err)
		}
	}
	secs := 180
	if v := os.Getenv("RAM_FLOW_SECS"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &secs); err != nil {
			t.Fatalf("bad RAM_FLOW_SECS %q: %v", v, err)
		}
	}
	const symbols = 500

	e := New(DefaultConfig())
	holdPopulate(t, e, alerts)

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	preRSS := rssKB(t)
	t.Logf("pre-flow: HeapAlloc=%d MiB RSS=%d MiB HeapReleased=%d MiB NumGC=%d",
		m.HeapAlloc>>20, preRSS>>10, m.HeapReleased>>20, m.NumGC)

	// Traffic: one goroutine, 10ms cadence, 20 mutations + 20 ticks per
	// round (2k/s each). Walks drift ±500/step within [400k,600k] so fires
	// trickle instead of wiping a symbol in one tick.
	rng := rand.New(rand.NewSource(2))
	names := make([]string, symbols)
	walk := make([]Price, symbols)
	for i := range names {
		names[i] = fmt.Sprintf("SYM%03d", i)
		walk[i] = 500_000
	}
	nextID := uint32(alerts)
	var fired atomic.Int64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]Trigger, 1024)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		ts := int64(1) << 40
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
			}
			for i := 0; i < 20; i++ { // cancel a random original alert
				_ = e.Cancel(mkID(uint32(rng.Intn(alerts))))
			}
			for i := 0; i < 20; i++ { // replace with a fresh alert
				s := rng.Intn(symbols)
				nextID++
				a := AlertSpec{
					ID:          mkID(nextID),
					Symbol:      names[s],
					PriceType:   PriceLast,
					Direction:   Direction(rng.Intn(2)),
					TargetPrice: Price(1000 + rng.Int63n(999_000)),
					ValidFrom:   1,
				}
				if err := e.Upsert(a); err != nil {
					t.Error(err)
					return
				}
			}
			for i := 0; i < 20; i++ { // ticks: advance random walks
				s := rng.Intn(symbols)
				walk[s] += Price(rng.Int63n(1001) - 500)
				if walk[s] < 400_000 {
					walk[s] = 400_000
				}
				if walk[s] > 600_000 {
					walk[s] = 600_000
				}
				ts++
				e.Match(&Tick{Symbol: names[s], Last: walk[s],
					Present: 1 << uint(PriceLast), TS: ts})
			}
			for { // drain triggers
				n := e.Triggers().PopBatch(buf)
				if n == 0 {
					break
				}
				fired.Add(int64(n))
			}
		}
	}()

	start := time.Now()
	for i := 1; i <= secs; i++ {
		time.Sleep(time.Until(start.Add(time.Duration(i) * time.Second)))
		runtime.ReadMemStats(&m)
		if i <= 5 || i%10 == 0 {
			t.Logf("t=%3ds HeapAlloc=%4d MiB RSS=%4d MiB HeapReleased=%4d MiB NumGC=%d live=%d fired=%d dropped=%d",
				i, m.HeapAlloc>>20, rssKB(t)>>10, m.HeapReleased>>20, m.NumGC,
				e.Stats().Live, fired.Load(), e.Triggers().Dropped())
		}
	}
	close(stop)
	<-done
	e.Sync()
	time.Sleep(2 * e.cfg.ReaperInterval)

	runtime.ReadMemStats(&m)
	t.Logf("post-flow: HeapAlloc=%d MiB RSS=%d MiB (pre-flow %d) HeapReleased=%d MiB NumGC=%d live=%d fired=%d",
		m.HeapAlloc>>20, rssKB(t)>>10, preRSS>>10, m.HeapReleased>>20, m.NumGC,
		e.Stats().Live, fired.Load())
	debug.FreeOSMemory()
	runtime.ReadMemStats(&m)
	t.Logf("after FreeOSMemory: HeapAlloc=%d MiB RSS=%d MiB HeapReleased=%d MiB",
		m.HeapAlloc>>20, rssKB(t)>>10, m.HeapReleased>>20)
	e.Close()
}
