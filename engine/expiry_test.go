package engine

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

// waitForExpLen polls the reaper-written expiry gauge until it reaches want.
// The table itself is reaper-owned; the atomic gauge is the race-free view.
func waitForExpLen(t *testing.T, e *Engine, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if int(e.expLen.Load()) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expiry table length = %d, want %d", e.expLen.Load(), want)
}

// TestExpiryQueueUnbounded pins the lossless expQ: a burst of expiring
// upserts far deeper than the old channel capacity must not block Upsert,
// and every registration must land in the reaper's table.
func TestExpiryQueueUnbounded(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = time.Hour // sweeps never interfere
	e := New(cfg)
	defer e.Close()

	const n = 3*expChunk + 17 // crosses several chunk boundaries
	exp := time.Now().Add(time.Hour).UnixNano()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			if err := e.Upsert(AlertSpec{ID: mkID(uint32(i)), Symbol: "UB",
				PriceType: PriceLast, Direction: DirGTE, TargetPrice: 100,
				ValidFrom: 1, Expires: exp}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Upsert blocked on a full expiry queue")
	}
	waitForExpLen(t, e, n)
}

// TestExpiryDeregOnFire pins the flusher-pass dereg: firing an alert must
// leave no expiry registration behind. fire itself never touches expQ —
// the dereg rides the mutRemove into the flusher's refs pass.
func TestExpiryDeregOnFire(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = time.Hour // no sweep interference
	e := New(cfg)
	defer e.Close()

	id := mkID(1)
	if err := e.Upsert(AlertSpec{ID: id, Symbol: "FIRE", PriceType: PriceLast,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1,
		Expires: time.Now().Add(time.Hour).UnixNano(), AutoDeactivate: true}); err != nil {
		t.Fatal(err)
	}
	waitForExpLen(t, e, 1)
	e.Sync() // insert is published; Match below cannot miss it
	e.Match(&Tick{Symbol: "FIRE", Last: 200, Present: TickAllPresent(),
		TS: time.Now().UnixNano()})
	if _, ok := e.Triggers().Pop(); !ok {
		t.Fatal("alert did not fire")
	}
	e.Sync() // flusher applies the removal and queues the dereg
	waitForExpLen(t, e, 0)
}

// TestExpiryDeregBackstopSweep is a characterization test of the pre-existing
// backstop: a stale entry whose deadline passes before its dereg is consumed
// is still dropped by the sweep's ref-identity liveness check. It must stay
// green through Tasks 3-4.
func TestExpiryDeregBackstopSweep(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = 5 * time.Millisecond
	e := New(cfg)
	defer e.Close()

	id := mkID(7)
	if err := e.Upsert(AlertSpec{ID: id, Symbol: "BACK", PriceType: PriceLast,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1,
		Expires: time.Now().Add(30 * time.Millisecond).UnixNano()}); err != nil {
		t.Fatal(err)
	}
	waitForExpLen(t, e, 1)
	if err := e.Cancel(id); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	time.Sleep(60 * time.Millisecond) // deadline passes after the terminal transition
	waitForExpLen(t, e, 0)
}

// TestExpiryDeregOnCancel pins removeIfLive's dereg: Cancel must drop the
// registration immediately, not at the deadline.
func TestExpiryDeregOnCancel(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = time.Hour
	e := New(cfg)
	defer e.Close()

	id := mkID(2)
	if err := e.Upsert(AlertSpec{ID: id, Symbol: "CAN", PriceType: PriceLast,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1,
		Expires: time.Now().Add(time.Hour).UnixNano()}); err != nil {
		t.Fatal(err)
	}
	waitForExpLen(t, e, 1)
	if err := e.Cancel(id); err != nil {
		t.Fatal(err)
	}
	waitForExpLen(t, e, 0)
}

// TestExpiryDeregOnReplace: replacing a live expiring alert must drop the
// old registration and keep exactly the new handout's.
func TestExpiryDeregOnReplace(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = time.Hour
	e := New(cfg)

	id := mkID(3)
	if err := e.Upsert(AlertSpec{ID: id, Symbol: "REP", PriceType: PriceLast,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1,
		Expires: time.Now().Add(time.Hour).UnixNano()}); err != nil {
		t.Fatal(err)
	}
	waitForExpLen(t, e, 1)
	newExp := time.Now().Add(2 * time.Hour).UnixNano()
	if err := e.Upsert(AlertSpec{ID: id, Symbol: "REP", PriceType: PriceLast,
		Direction: DirLTE, TargetPrice: 50, ValidFrom: 1, Expires: newExp}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	waitForExpLen(t, e, 1) // old gone, new present
	e.Close()              // reaper drained and gone; table reads are safe
	var got []expEntry
	for x := range e.expiry.All() {
		got = append(got, x)
	}
	if len(got) != 1 || got[0].expires != newExp || got[0].ref.e.id != id {
		t.Fatalf("surviving registration = %+v, want exactly the new handout (expires %d)", got, newExp)
	}
}

// TestExpiryDeregOnUpsertAfterTerminal pins the terminal branch: an upsert
// landing on an already-fired slot holds the fired ref and must dereg it —
// the flusher pass cannot, because this upsert already replaced refs[id]
// before the removal is applied.
func TestExpiryDeregOnUpsertAfterTerminal(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = time.Hour
	e := New(cfg)

	id := mkID(4)
	if err := e.Upsert(AlertSpec{ID: id, Symbol: "TERM", PriceType: PriceLast,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1,
		Expires: time.Now().Add(time.Hour).UnixNano(), AutoDeactivate: true}); err != nil {
		t.Fatal(err)
	}
	waitForExpLen(t, e, 1)
	e.Sync() // insert is published; Match below cannot miss it
	e.Match(&Tick{Symbol: "TERM", Last: 200, Present: TickAllPresent(),
		TS: time.Now().UnixNano()})
	if _, ok := e.Triggers().Pop(); !ok {
		t.Fatal("alert did not fire")
	}
	// Same-ID upsert before Sync: the slot is TRIGGERED, so Upsert takes
	// the terminal branch — this goroutine owns the old handout's dereg.
	newExp := time.Now().Add(2 * time.Hour).UnixNano()
	if err := e.Upsert(AlertSpec{ID: id, Symbol: "TERM", PriceType: PriceLast,
		Direction: DirGTE, TargetPrice: 300, ValidFrom: 1, Expires: newExp}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	waitForExpLen(t, e, 1)
	e.Close()
	var got []expEntry
	for x := range e.expiry.All() {
		got = append(got, x)
	}
	if len(got) != 1 || got[0].expires != newExp || got[0].ref.e.id != id {
		t.Fatalf("surviving registration = %+v, want exactly the new handout (expires %d)", got, newExp)
	}
}
