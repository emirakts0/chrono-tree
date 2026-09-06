package bench

import (
	"os"
	"testing"
	"time"
)

func TestSymbolsDeterministic(t *testing.T) {
	a, b := Symbols(500), Symbols(500)
	if len(a) != 500 {
		t.Fatalf("len = %d, want 500", len(a))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("symbol %d differs between runs", i)
		}
		if a[i].Ref < 1 {
			t.Fatalf("%s: Ref = %d, want >= 1", a[i].Name, a[i].Ref)
		}
		if len(a[i].Name) > 20 {
			t.Fatalf("%s exceeds the 20-byte tick-name bound", a[i].Name)
		}
	}
}

func TestSymbolsDecimalsMix(t *testing.T) {
	// The universe must exercise all three precision classes (2/4/8).
	syms := Symbols(2000)
	seen := map[uint8]int{}
	for _, s := range syms {
		seen[s.Decimals]++
	}
	for _, dec := range []uint8{2, 4, 8} {
		if seen[dec] == 0 {
			t.Fatalf("decimals %d never appears: %v", dec, seen)
		}
	}
}

func TestMarketRoundRobinAndHold(t *testing.T) {
	syms := Symbols(4)
	m := NewMarket(syms, 1, false)
	for i := 0; i < 8; i++ {
		q := m.Step()
		if q.SymIdx != i%4 {
			t.Fatalf("step %d: SymIdx = %d, want %d", i, q.SymIdx, i%4)
		}
		if q.Bid >= q.Ask {
			t.Fatalf("step %d: bid %d >= ask %d", i, q.Bid, q.Ask)
		}
	}
	h := NewMarket(syms, 1, true)
	q := h.Step()
	if q.SymIdx != 0 {
		t.Fatal("first step is not symbol 0")
	}
	if q.Bid != syms[0].Ref { // held at reference, zero noise
		t.Fatalf("held symbol bid = %d, want ref %d", q.Bid, syms[0].Ref)
	}
}

func TestMarketDeterministic(t *testing.T) {
	syms := Symbols(32)
	a, b := NewMarket(syms, 7, false), NewMarket(syms, 7, false)
	for i := 0; i < 1000; i++ {
		qa, qb := a.Step(), b.Step()
		if qa != qb {
			t.Fatalf("step %d diverged: %+v vs %+v", i, qa, qb)
		}
	}
}

func TestEmitterBudget(t *testing.T) {
	e := &Emitter{Target: 20000}
	var n int
	for i := 0; i < 100; i++ { // 100 × 10ms = 1s
		n += e.Take(10 * time.Millisecond)
	}
	if n < 19800 || n > 20200 {
		t.Fatalf("emitter gave %d over 1s at 20k/s, want ~20000", n)
	}
}

func TestProcReads(t *testing.T) {
	pid := os.Getpid()
	if ReadRSS(pid) == 0 && ReadPeakRSS(pid) == 0 {
		t.Fatal("self has neither RSS nor peak")
	}
	// Burn a few CPU ticks first: a fresh test binary can legitimately
	// still be at utime+stime == 0 at USER_HZ=100 (10ms) resolution.
	for i := 0; i < 50_000_000; i++ {
		_ = i * i
	}
	if ReadCPUSeconds(pid) <= 0 {
		t.Fatal("self CPU seconds <= 0")
	}
}
