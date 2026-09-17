package engine

import (
	"errors"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestDimsHelper(t *testing.T) {
	allSentinel := [dimMax]uint16{
		DimSentinel, DimSentinel, DimSentinel, DimSentinel,
		DimSentinel, DimSentinel, DimSentinel, DimSentinel,
	}
	if Dims() != allSentinel {
		t.Fatalf("Dims() = %v, want all-sentinel", Dims())
	}
	d := Dims(3, 1, 42)
	want := allSentinel
	want[0], want[1], want[2] = 3, 1, 42
	if d != want {
		t.Fatalf("Dims(3,1,42) = %v, want %v", d, want)
	}
	// More than dimMax values is programmer error and must panic, not
	// silently index out of range.
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Dims with more than dimMax values must panic")
			}
		}()
		Dims(1, 2, 3, 4, 5, 6, 7, 8, 9)
	}()
}

func TestNormalizeDims(t *testing.T) {
	// Zero-value array at width 0 normalizes to all-sentinel (this is what
	// keeps every existing zero-value AlertSpec/Tick valid).
	if d, ok := normalizeDims([dimMax]uint16{}, 0); !ok || d != Dims() {
		t.Fatalf("width 0: got (%v, %v)", d, ok)
	}
	// Trailing real values are overwritten with the sentinel.
	if d, ok := normalizeDims([dimMax]uint16{1, 2, 7, 7, 7, 7, 7, 7}, 2); !ok || d != Dims(1, 2) {
		t.Fatalf("width 2: got (%v, %v)", d, ok)
	}
	// Sentinel inside width is malformed.
	if _, ok := normalizeDims([dimMax]uint16{1, DimSentinel}, 2); ok {
		t.Fatal("sentinel inside width must not be ok")
	}
	// Width 8 with real values in all slots is the maximal valid input.
	if d, ok := normalizeDims(Dims(1, 2, 3, 4, 5, 6, 7, 8), 8); !ok || d != Dims(1, 2, 3, 4, 5, 6, 7, 8) {
		t.Fatalf("width 8: got (%v, %v)", d, ok)
	}
}

func TestDimsValidation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DimCount = 2
	e := New(cfg)
	defer e.Close()
	base := AlertSpec{ID: mkID(1), Symbol: "S", PriceType: PriceAsk,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1}

	bad := base // slot 1 (tier) left at sentinel
	bad.Dims = Dims(1)
	if err := e.Upsert(bad); !errors.Is(err, ErrDims) {
		t.Fatalf("sentinel inside width: err=%v, want ErrDims", err)
	}

	good := base // trailing junk past width is normalized away
	good.ID = mkID(2)
	good.Dims = [dimMax]uint16{1, 2, 7, 7, 7, 7, 7, 7}
	if err := e.Upsert(good); err != nil {
		t.Fatalf("trailing junk should be normalized: %v", err)
	}

	// Behavioral proof of normalization: a width-2 match must fire the
	// trailing-junk alert exactly once. If Upsert stopped at validation and
	// stored the junk, the alert would be unreachable and never fire.
	e.Sync()
	e.Match(&Tick{Symbol: "S", Ask: 150, Present: 1 << uint(PriceAsk),
		TS: 1 << 40, Dims: Dims(1, 2)})
	trs := drainTriggers(e)
	if len(trs) != 1 || trs[0].ID != mkID(2) {
		t.Fatalf("trailing-junk alert: got %v fires, want exactly one fire of %v",
			trs, mkID(2))
	}

	// A zero-value Dims array carries real zeros (not sentinels) below
	// width: at width 2 it means dims (0, 0), so it must store and fire.
	zero := base
	zero.ID = mkID(3)
	if err := e.Upsert(zero); err != nil {
		t.Fatalf("zero-value Dims: %v", err)
	}
	e.Sync()
	e.Match(&Tick{Symbol: "S", Ask: 150, Present: 1 << uint(PriceAsk),
		TS: 1 << 40, Dims: Dims(0, 0)})
	trs = drainTriggers(e)
	if len(trs) != 1 || trs[0].ID != mkID(3) {
		t.Fatalf("zero-value Dims alert: got %v fires, want exactly one fire of %v",
			trs, mkID(3))
	}
}

func TestNewDimsConfig(t *testing.T) {
	mustPanic := func(name string, cfg Config) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: New must panic", name)
			}
		}()
		New(cfg)
	}
	nine := DefaultConfig()
	nine.DimCount = dimMax + 1
	mustPanic("nine dims", nine)
	neg := DefaultConfig()
	neg.DimCount = -1
	mustPanic("negative count", neg)
}

func TestDimsMatching(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DimCount = 2
	e := New(cfg)
	defer e.Close()
	up := func(v uint32, seg, tier uint16, target Price) {
		t.Helper()
		if err := e.Upsert(AlertSpec{ID: mkID(v), Symbol: "S", PriceType: PriceAsk,
			Direction: DirGTE, TargetPrice: target, ValidFrom: 1,
			Dims: Dims(seg, tier)}); err != nil {
			t.Fatalf("upsert %d: %v", v, err)
		}
	}
	up(1, 10, 20, 100) // exact match, well below tick
	up(2, 10, 21, 100) // tier differs
	up(3, 11, 20, 100) // segment differs
	up(4, 12, 22, 100) // both differ
	up(5, 10, 20, 150) // exact match at the boundary price (max-id probe)
	e.Sync()

	tick := func(seg, tier uint16, price Price) {
		e.Match(&Tick{Symbol: "S", Ask: price, Present: 1 << uint(PriceAsk),
			TS: 1 << 40, Dims: Dims(seg, tier)})
	}
	tick(10, 20, 150)

	fired := map[AlertID]bool{}
	for _, tr := range drainTriggers(e) {
		fired[tr.ID] = true
	}
	if !fired[mkID(1)] || !fired[mkID(5)] {
		t.Fatal("exact-dim alerts (incl. boundary price) must fire")
	}
	for _, v := range []uint32{2, 3, 4} {
		if fired[mkID(v)] {
			t.Fatalf("alert %d fired despite dim mismatch", v)
		}
	}

	// Fan-out shape: a second tick matching only alert 3's dims fires it.
	tick(11, 20, 150)
	for _, tr := range drainTriggers(e) {
		if tr.ID != mkID(3) {
			t.Fatalf("unexpected fire %v", tr.ID)
		}
	}
}

func TestMatchDimsZeroAllocs(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DimCount = 2
	e := New(cfg)
	defer e.Close()
	for i := 0; i < 50; i++ {
		if err := e.Upsert(AlertSpec{ID: mkID(uint32(i + 1)), Symbol: "S",
			PriceType: PriceAsk, Direction: DirGTE,
			TargetPrice: Price(1000 + i*10), ValidFrom: 1,
			Dims: Dims(uint16(i%3), uint16(i%5))}); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}
	e.Sync()
	tick := Tick{Symbol: "S", Ask: 500, Present: 1 << uint(PriceAsk),
		TS: 1 << 40, Dims: Dims(1, 2)}
	if allocs := testing.AllocsPerRun(200, func() { e.Match(&tick) }); allocs != 0 {
		t.Fatalf("Match allocates: %.0f allocs/op, want 0", allocs)
	}
}

func TestDimsMatchingLTE(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DimCount = 2
	e := New(cfg)
	defer e.Close()
	up := func(v uint32, tier uint16, target Price) {
		t.Helper()
		if err := e.Upsert(AlertSpec{ID: mkID(v), Symbol: "S", PriceType: PriceAsk,
			Direction: DirLTE, TargetPrice: target, ValidFrom: 1,
			Dims: Dims(10, tier)}); err != nil {
			t.Fatalf("upsert %d: %v", v, err)
		}
	}
	up(1, 21, 700)  // own dim block, above the tick price: must fire
	up(2, 22, 1000) // larger dim block: Ascend reaches it, guard must stop there
	e.Sync()
	e.Match(&Tick{Symbol: "S", Ask: 500, Present: 1 << uint(PriceAsk),
		TS: 1 << 40, Dims: Dims(10, 21)})
	fired := map[AlertID]bool{}
	for _, tr := range drainTriggers(e) {
		fired[tr.ID] = true
	}
	if !fired[mkID(1)] {
		t.Fatal("own-dim LTE alert must fire")
	}
	if fired[mkID(2)] {
		t.Fatal("LTE Ascend crossed the dim block")
	}
}

// TestUpsertErrDimsReplaceKeepsLiveHandout pins that an ErrDims-rejected
// replacement leaves the existing alert untouched: ref, slot status, expiry
// registration, and fire behavior of the live handout all survive. ErrDims
// upserts were previously only ever tested against fresh IDs.
func TestUpsertErrDimsReplaceKeepsLiveHandout(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.DimCount = 2
	cfg.ReaperInterval = time.Hour // no sweep interference
	e := New(cfg)
	defer e.Close()

	x := mkID(21)
	live := AlertSpec{ID: x, Symbol: "ERR", PriceType: PriceAsk, Direction: DirGTE,
		TargetPrice: 100, ValidFrom: 1, Dims: Dims(1, 2),
		Expires: time.Now().Add(time.Hour).UnixNano()}
	if err := e.Upsert(live); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	waitForExpLen(t, e, 1)

	// Sentinel inside width, first as a lone hole then as the all-sentinel
	// zero form: both must be rejected and neither may disturb the handout.
	bad := live
	bad.Dims = Dims(1) // slot 1 left at sentinel
	if err := e.Upsert(bad); !errors.Is(err, ErrDims) {
		t.Fatalf("hole variant: err=%v, want ErrDims", err)
	}
	bad.Dims = Dims()
	if err := e.Upsert(bad); !errors.Is(err, ErrDims) {
		t.Fatalf("all-sentinel variant: err=%v, want ErrDims", err)
	}

	if s := e.Stats(); s.Live != 1 {
		t.Fatalf("Live = %d after rejected replaces, want 1", s.Live)
	}
	e.mu.Lock()
	ref, ok := e.refs[x]
	e.mu.Unlock()
	if !ok {
		t.Fatal("rejected replace dropped the live ref")
	}
	if ref.e.dims != Dims(1, 2) {
		t.Fatalf("ref dims = %v, want the live handout's Dims(1, 2)", ref.e.dims)
	}
	if st := e.slots.status(entryIdx(ref.e)); st != StatusActive {
		t.Fatalf("slot status = %v, want StatusActive", st)
	}
	waitForExpLen(t, e, 1) // the live handout's registration is still there

	e.Match(&Tick{Symbol: "ERR", Ask: 150, Present: 1 << uint(PriceAsk),
		TS: 1 << 40, Dims: Dims(1, 2)})
	trs := drainTriggers(e)
	if len(trs) != 1 || trs[0].ID != x {
		t.Fatalf("triggers = %+v, want exactly one fire of the live handout", trs)
	}
}

// TestMatchTrailingJunkTickDimsFires pins Match-side trailing-junk
// normalization behaviorally (previously proven only for the Upsert side):
// ticks whose Dims carry real junk past the width must still reach the alert.
// If Match scanned with the raw t.Dims, these ticks would silently never fire.
func TestMatchTrailingJunkTickDimsFires(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.DimCount = 2
	e := New(cfg)
	defer e.Close()

	if err := e.Upsert(AlertSpec{ID: mkID(1), Symbol: "JUNK", PriceType: PriceAsk,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1, Dims: Dims(1, 2)}); err != nil {
		t.Fatal(err)
	}
	if err := e.Upsert(AlertSpec{ID: mkID(2), Symbol: "JUNK", PriceType: PriceAsk,
		Direction: DirLTE, TargetPrice: 200, ValidFrom: 1, Dims: Dims(1, 3)}); err != nil {
		t.Fatal(err)
	}
	e.Sync()

	tick := func(d [dimMax]uint16) {
		e.Match(&Tick{Symbol: "JUNK", Ask: 150, Present: 1 << uint(PriceAsk),
			TS: 1 << 40, Dims: d})
	}
	tick([dimMax]uint16{1, 2, 7, 7, 7, 7, 7, 7})
	if trs := drainTriggers(e); len(trs) != 1 || trs[0].ID != mkID(1) {
		t.Fatalf("junk-past-width tick 1: triggers = %+v, want exactly one fire of alert 1", trs)
	}
	tick([dimMax]uint16{1, 3, 0xfffe, 0, 0, 0, 0, 0})
	if trs := drainTriggers(e); len(trs) != 1 || trs[0].ID != mkID(2) {
		t.Fatalf("junk-past-width tick 2: triggers = %+v, want exactly one fire of alert 2", trs)
	}
	// A repeat of the first junk tick fires nothing: alert 1 is terminal.
	tick([dimMax]uint16{1, 2, 7, 7, 7, 7, 7, 7})
	if trs := drainTriggers(e); len(trs) != 0 {
		t.Fatalf("repeat junk tick re-fired: %+v", trs)
	}

	// Width-0 subtest: a tick with real values in all 8 slots must fire a
	// zero-Dims alert — normalization protects the width-0 contract.
	t.Run("Width0", func(t *testing.T) {
		e0 := New(DefaultConfig()) // DimCount 0
		defer e0.Close()
		if err := e0.Upsert(AlertSpec{ID: mkID(1), Symbol: "W0", PriceType: PriceAsk,
			Direction: DirGTE, TargetPrice: 100, ValidFrom: 1, Dims: Dims()}); err != nil {
			t.Fatal(err)
		}
		e0.Sync()
		e0.Match(&Tick{Symbol: "W0", Ask: 150, Present: 1 << uint(PriceAsk),
			TS: 1 << 40, Dims: Dims(1, 2, 3, 4, 5, 6, 7, 8)})
		if trs := drainTriggers(e0); len(trs) != 1 || trs[0].ID != mkID(1) {
			t.Fatalf("width-0 junk tick: triggers = %+v, want exactly one fire", trs)
		}
	})
}

func TestMatchMalformedDimsDropped(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DimCount = 1
	e := New(cfg)
	defer e.Close()
	if err := e.Upsert(AlertSpec{ID: mkID(1), Symbol: "S", PriceType: PriceAsk,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1,
		Dims: Dims(5)}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	// Sentinel inside width: malformed tick, silently dropped — no fire, no panic.
	e.Match(&Tick{Symbol: "S", Ask: 200, Present: 1 << uint(PriceAsk),
		TS: 1 << 40, Dims: [dimMax]uint16{DimSentinel, 9, DimSentinel,
			DimSentinel, DimSentinel, DimSentinel, DimSentinel, DimSentinel}})
	for _, tr := range drainTriggers(e) {
		t.Fatalf("malformed tick fired %v", tr.ID)
	}
	// The same alert fires from a well-formed tick, proving the drop was the
	// dims guard and not a broken tree.
	e.Match(&Tick{Symbol: "S", Ask: 200, Present: 1 << uint(PriceAsk),
		TS: 1 << 40, Dims: Dims(5)})
	n := 0
	for range drainTriggers(e) {
		n++
	}
	if n != 1 {
		t.Fatalf("well-formed tick fired %d times, want 1", n)
	}
}
