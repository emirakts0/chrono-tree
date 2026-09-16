package engine

// Manual RAM baseline: insert N alerts, sync, go idle, attribute the heap.
// Run: RAM_BASELINE=1 go test ./engine -run TestRAMBaseline -v -timeout 30m
// Temporary analysis harness — not part of the suite.

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

func rssKB(t *testing.T) int64 {
	t.Helper()
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return -1
	}
	var kb int64
	for i := 0; i+8 <= len(b); i++ {
		if string(b[i:i+6]) == "VmRSS:" {
			n, _ := fmt.Sscanf(string(b[i+6:]), "%d", &kb)
			if n == 1 {
				return kb
			}
		}
	}
	return -1
}

func TestRAMBaseline(t *testing.T) {
	const alerts = 1_000_000
	const symbols = 500
	if os.Getenv("RAM_BASELINE") == "" {
		t.Skip("set RAM_BASELINE=1 to run")
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
	t.Logf("baseline: HeapAlloc=%d MiB RSS=%d MiB", base.HeapAlloc>>20, baseRSS>>10)

	cfg := DefaultConfig()
	e := New(cfg)
	pre := gc()
	t.Logf("engine created (defaults): HeapAlloc=%d MiB Δ=%d MiB", pre.HeapAlloc>>20, (pre.HeapAlloc-base.HeapAlloc)>>20)

	per := alerts / symbols
	for sym := 0; sym < symbols; sym++ {
		name := fmt.Sprintf("SYM%03d", sym)
		for i := 0; i < per; i++ {
			a := AlertSpec{
				ID:          mkID(uint32(sym*per + i)),
				Symbol:      name,
				PriceType:   PriceLast,
				Direction:   Direction(i & 1),
				TargetPrice: Price(100 + i%100),
				ValidFrom:   1,
			}
			if err := e.Upsert(a); err != nil {
				t.Fatal(err)
			}
		}
	}
	e.Sync()
	// Idle: let the reaper drain expQ, sweep, and recycle once.
	for e.expQ.pending() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(2 * cfg.ReaperInterval)

	idle := gc()
	idleRSS := rssKB(t)
	delta := idle.HeapAlloc - base.HeapAlloc
	t.Logf("idle: HeapAlloc=%d MiB RSS=%d MiB | engine delta=%d MiB (%.1f B/alert)",
		idle.HeapAlloc>>20, idleRSS>>10, delta>>20, float64(delta)/alerts)

	// Direct component census.
	var treeEntries int
	occupied := 0
	for i := range e.states {
		s := e.states[i].snap.Load()
		if s == nil {
			continue
		}
		occupied++
		for j := range s.trees {
			treeEntries += s.trees[j].Len()
		}
	}
	e.mu.Lock()
	refsLen := len(e.refs)
	e.mu.Unlock()
	// e.expiry / e.slots reads below race the reaper; safe only because this
	// env-gated manual harness (RAM_BASELINE=1) is never run under -race.
	t.Logf("census: symbols=%d treeEntries=%d refs=%d expiry=%d slotsAllocated=%d parked=%d live=%d",
		occupied, treeEntries, refsLen, e.expiry.Len(), e.slots.next, len(e.parked), e.Stats().Live)

	// Heap profile for inuse_space attribution.
	profPath := filepath.Join(t.TempDir(), "ram_idle.pb.gz")
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
	after := gc()
	t.Logf("after Close: HeapAlloc=%d MiB RSS=%d MiB", after.HeapAlloc>>20, rssKB(t)>>10)
}

// Manual RAM churn: 1M alerts across mixed TTL horizons (5m/1h/1d/never in
// equal quarters), then cancel ~1/3 of the steady population and fire the
// entire victim fifth, settle, and census. The expiry table census is the
// Gate 4 decision input: lingering stale registrations vs live expiring.
// Run: RAM_BASELINE=1 go test ./engine -run TestRAMChurn -v -timeout 30m
func TestRAMChurn(t *testing.T) {
	const alerts = 1_000_000
	const symbols = 500
	const perSym = alerts / symbols // 2000
	if os.Getenv("RAM_BASELINE") == "" {
		t.Skip("set RAM_BASELINE=1 to run")
	}

	gc := func() runtime.MemStats {
		var m runtime.MemStats
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&m)
		return m
	}
	base := gc()

	cfg := DefaultConfig()
	e := New(cfg)
	now := time.Now()
	ttls := []time.Duration{5 * time.Minute, time.Hour, 24 * time.Hour}
	for sym := 0; sym < symbols; sym++ {
		name := fmt.Sprintf("SYM%03d", sym)
		for i := 0; i < perSym; i++ {
			var exp int64
			switch i % 4 {
			case 3:
				exp = 0 // never
			default:
				exp = now.Add(ttls[i%3]).UnixNano()
			}
			a := AlertSpec{ID: mkID(uint32(sym*perSym + i)), Symbol: name,
				PriceType: PriceLast, Direction: Direction(i & 1),
				TargetPrice: Price(100 + i%100), ValidFrom: 1,
				Expires: exp}
			if err := e.Upsert(a); err != nil {
				t.Fatal(err)
			}
		}
	}
	e.Sync()
	for e.expQ.pending() > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	// Churn: cancel every third steady alert (symbols 0-399), fire all of
	// symbols 400-499 (both directions: two ticks bracket the target band).
	for sym := 0; sym < 400; sym++ {
		for i := 0; i < perSym; i += 3 {
			if err := e.Cancel(mkID(uint32(sym*perSym + i))); err != nil {
				t.Fatal(err)
			}
		}
	}
	for sym := 400; sym < symbols; sym++ {
		name := fmt.Sprintf("SYM%03d", sym)
		ts := time.Now().UnixNano()
		e.Match(&Tick{Symbol: name, Last: 500, Present: 1 << uint(PriceLast), TS: ts})
		e.Match(&Tick{Symbol: name, Last: 0, Present: 1 << uint(PriceLast), TS: ts})
	}
	e.Sync()
	time.Sleep(2 * cfg.ReaperInterval)

	idle := gc()
	delta := idle.HeapAlloc - base.HeapAlloc
	e.mu.Lock()
	refsLen := len(e.refs)
	live := e.live
	liveExpiring := 0
	for _, ref := range e.refs {
		if ref.e.expires != 0 {
			liveExpiring++
		}
	}
	e.mu.Unlock()
	t.Logf("idle: HeapAlloc=%d MiB | engine delta=%d MiB (%.1f B/alert) live=%d refs=%d",
		idle.HeapAlloc>>20, delta>>20, float64(delta)/alerts, live, refsLen)
	t.Logf("census: expiry=%d liveExpiring=%d ratio=%.2f slotsAllocated=%d parked=%d",
		e.expiry.Len(), liveExpiring,
		float64(e.expiry.Len())/float64(liveExpiring), e.slots.next, len(e.parked))

	profPath := filepath.Join(t.TempDir(), "ram_churn.pb.gz")
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
