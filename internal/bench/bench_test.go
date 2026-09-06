package bench

import (
	"os"
	"testing"
	"time"

	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/alertstore"
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

// Tier parity: every layout assigns alert tier i%2 with symbol i%n, so the
// walk's tick for symbol s must carry tier s%2 — otherwise an engine
// configured with a tier dim never matches (dims must be exactly equal).
func TestMarketTierMatchesLayouts(t *testing.T) {
	for _, n := range []int{50, 200, 500} {
		syms := Symbols(n)
		for _, hold := range []bool{false, true} {
			m := NewMarket(syms, 1, hold)
			seen := map[int]int{}
			for i := 0; len(seen) < n; i++ {
				q := m.Step()
				if prev, ok := seen[q.SymIdx]; ok && prev != q.TierIdx {
					t.Fatalf("n=%d hold=%v: symbol %d tier flipped %d -> %d", n, hold, q.SymIdx, prev, q.TierIdx)
				}
				seen[q.SymIdx] = q.TierIdx
			}
			for s, tier := range seen {
				if tier != s%2 {
					t.Fatalf("n=%d hold=%v: symbol %d tier %d, want %d", n, hold, s, tier, s%2)
				}
			}
		}
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

// drain pops the engine's trigger ring until want triggers arrive or the
// timeout hits; returns how many arrived.
func drain(t *testing.T, e *engine.Engine, want int, timeout time.Duration) int {
	t.Helper()
	buf := make([]engine.Trigger, 64)
	deadline := time.Now().Add(timeout)
	var n int
	for n < want && time.Now().Before(deadline) {
		if k := e.Triggers().PopBatch(buf); k > 0 {
			n += k
			continue
		}
		time.Sleep(500 * time.Microsecond)
	}
	return n
}

func tickOf(syms []Symbol, q Quote, ts int64) engine.Tick { return EngineTick(syms, q, ts) }

func TestParkedNeverFires(t *testing.T) {
	syms := Symbols(50)
	specs := Parked(2000, syms)
	e := engine.New(engine.DefaultConfig())
	defer e.Close()
	for _, s := range specs {
		if err := e.Upsert(EngineSpec(s)); err != nil {
			t.Fatal(err)
		}
	}
	e.Sync()
	m := NewMarket(syms, 1, false)
	now := int64(1 << 40)
	for i := 0; i < 20000; i++ {
		q := m.Step()
		tk := tickOf(syms, q, now+int64(i))
		e.Match(&tk)
	}
	if n := drain(t, e, 1, 50*time.Millisecond); n != 0 {
		t.Fatalf("parked layout fired %d triggers", n)
	}
}

func TestTrickleFiresSome(t *testing.T) {
	syms := Symbols(50)
	specs := Trickle(2000, syms, 0.002)
	e := engine.New(engine.DefaultConfig())
	defer e.Close()
	for _, s := range specs {
		if err := e.Upsert(EngineSpec(s)); err != nil {
			t.Fatal(err)
		}
	}
	e.Sync()
	m := NewMarket(syms, 1, false)
	now := int64(1 << 40)
	for i := 0; i < 20000; i++ {
		q := m.Step()
		tk := tickOf(syms, q, now+int64(i))
		e.Match(&tk)
	}
	n := drain(t, e, 1, 100*time.Millisecond)
	if n == 0 {
		t.Fatal("trickle layout never fired")
	}
	rest := drain(t, e, 2000, 100*time.Millisecond)
	if n+rest >= 2000 {
		t.Fatalf("trickle burned its whole ladder: %d of 2000", n+rest)
	}
}

func TestGapClusterFiresExactlyK(t *testing.T) {
	syms := Symbols(50)
	specs, gap := GapCluster(2000, 400, syms)
	if len(specs) != 2000 {
		t.Fatalf("len(specs) = %d, want 2000", len(specs))
	}
	e := engine.New(engine.DefaultConfig())
	defer e.Close()
	for _, s := range specs {
		if err := e.Upsert(EngineSpec(s)); err != nil {
			t.Fatal(err)
		}
	}
	e.Sync()
	// Soak: held symbol 0 walks nothing, parked rest stays parked.
	m := NewMarket(syms, 1, true)
	now := int64(1 << 40)
	for i := 0; i < 5000; i++ {
		q := m.Step()
		tk := tickOf(syms, q, now+int64(i))
		e.Match(&tk)
	}
	if n := drain(t, e, 1, 50*time.Millisecond); n != 0 {
		t.Fatalf("cluster fired %d during soak", n)
	}
	// Gap tick: fires all 400.
	tk := tickOf(syms, gap, now+99999)
	e.Match(&tk)
	if n := drain(t, e, 400, 5*time.Second); n != 400 {
		t.Fatalf("gap tick fired %d, want exactly 400", n)
	}
}

func TestMkIDUniqueNonZero(t *testing.T) {
	seen := map[engine.AlertID]bool{}
	for i := uint64(0); i < 10000; i++ {
		id := MkID(i)
		if id == (engine.AlertID{}) {
			t.Fatal("zero id")
		}
		if seen[id] {
			t.Fatalf("duplicate id at %d", i)
		}
		seen[id] = true
	}
}

func TestStoreAlertRoundTrip(t *testing.T) {
	syms := Symbols(4)
	specs := Parked(8, syms)
	a := StoreAlert(specs[3], 12345)
	if a.Symbol != specs[3].Symbol || a.Decimals != specs[3].Decimals ||
		a.Venue != VenueName || a.Tier != Tier(specs[3].TierIdx) ||
		int64(a.TargetPrice) != specs[3].Target || a.State != alertstore.StateActive {
		t.Fatalf("round trip mismatch: %+v", a)
	}
}
