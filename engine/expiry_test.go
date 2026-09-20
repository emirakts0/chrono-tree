package engine

import (
	"slices"
	"sync"
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
		Expires: time.Now().Add(time.Hour).UnixNano()}); err != nil {
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

// TestPausedAlertStillExpires pins that pausing never makes an expiring
// alert immortal: the sweep's terminal CAS accepts StatusPaused, so a paused
// slot must still be expired, removed, and deregistered — never fired.
func TestPausedAlertStillExpires(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = 20 * time.Millisecond
	e := New(cfg)
	defer e.Close()

	id := mkID(11)
	if err := e.Upsert(AlertSpec{ID: id, Symbol: "PAUSE", PriceType: PriceLast,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1,
		Expires: time.Now().Add(50 * time.Millisecond).UnixNano()}); err != nil {
		t.Fatal(err)
	}
	if err := e.SetStatus(id, StatusPaused); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if st, ok := e.Status(id); !ok || st != StatusPaused {
		t.Fatalf("Status = (%v, %v), want (StatusPaused, true)", st, ok)
	}
	// Wait past the deadline: the sweep must expire the PAUSED slot.
	deadline := time.Now().Add(5 * time.Second)
	for e.Stats().Live != 0 {
		if time.Now().After(deadline) {
			t.Fatal("paused alert never expired")
		}
		time.Sleep(5 * time.Millisecond)
	}
	e.Sync()
	if _, ok := e.Status(id); ok {
		t.Fatal("expired paused alert still present in refs")
	}
	sid, ok := e.syms.Get("PAUSE")
	if !ok {
		t.Fatal("symbol not interned")
	}
	if n := e.states[sid].snap.Load().trees[treeIndex(PriceLast, DirGTE)].Len(); n != 0 {
		t.Fatalf("tree Len = %d after expiry, want 0", n)
	}
	waitForExpLen(t, e, 0)
	if trs := drainTriggers(e); len(trs) != 0 {
		t.Fatalf("paused alert fired instead of expiring: %+v", trs)
	}
	if err := e.SetStatus(id, StatusActive); err != ErrNotFound {
		t.Fatalf("SetStatus after expiry = %v, want ErrNotFound", err)
	}
}

// TestSweepRemovalRetiresAndRecyclesSlot pins the sweep's slot lifecycle:
// the mutRemove a sweep submits must retire the slot (retired bit set) and,
// after the 2×interval recycle grace, return it to the free list. A sweep
// wiring a wrong gen into its mutRemove — or dropping retireGen — would leak
// every swept slot and exhaust the arena in production.
func TestSweepRemovalRetiresAndRecyclesSlot(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = 5 * time.Millisecond
	e := New(cfg)
	defer e.Close()

	id := mkID(12)
	if err := e.Upsert(AlertSpec{ID: id, Symbol: "SLOT", PriceType: PriceLast,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1,
		Expires: time.Now().Add(30 * time.Millisecond).UnixNano()}); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	aIdx := entryIdx(e.refs[id].e)
	e.mu.Unlock()
	e.Sync()

	deadline := time.Now().Add(5 * time.Second)
	for e.Stats().Live != 0 {
		if time.Now().After(deadline) {
			t.Fatal("alert never expired")
		}
		time.Sleep(2 * time.Millisecond)
	}
	e.Sync() // flusher applied the sweep's mutRemove; retireGen has landed

	if w := e.slots.get(aIdx).Load(); w&slotRetiredBit == 0 {
		t.Fatalf("slot %d word %#x lacks the retired bit after sweep removal", aIdx, w)
	}
	// Recycle grace is 2×interval; poll until the slot returns to the free list.
	for {
		e.slots.mu.Lock()
		freed := slices.Contains(e.slots.free, aIdx)
		e.slots.mu.Unlock()
		if freed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot %d not recycled within 5s", aIdx)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestSweepDrainsDueSetBeyondCap pins the len(due) >= maxPerSweep break
// branch and the multi-tick remainder drain: a due set strictly larger than
// sweep's 10,000 cap must be fully expired across consecutive ticks.
func TestSweepDrainsDueSetBeyondCap(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = 10 * time.Millisecond
	e := New(cfg)
	defer e.Close()

	const n = 10_501 // strictly larger than sweep's maxPerSweep = 10_000
	exp := time.Now().Add(50 * time.Millisecond).UnixNano()
	for i := 0; i < n; i++ {
		if err := e.Upsert(AlertSpec{ID: mkID(uint32(i)), Symbol: "BIG",
			PriceType: PriceLast, Direction: DirGTE, TargetPrice: 100,
			ValidFrom: 1, Expires: exp}); err != nil {
			t.Fatal(err)
		}
	}
	e.Sync()
	sid, ok := e.syms.Get("BIG")
	if !ok {
		t.Fatal("symbol not interned")
	}
	// Well past deadline + several reaper ticks: everything must be gone.
	deadline := time.Now().Add(10 * time.Second)
	for {
		s := e.Stats()
		if s.Live == 0 && s.ExpiryLen == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("due set not fully drained: Live=%d ExpiryLen=%d", s.Live, s.ExpiryLen)
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.Sync()
	if got := e.states[sid].snap.Load().trees[treeIndex(PriceLast, DirGTE)].Len(); got != 0 {
		t.Fatalf("tree Len = %d after final Sync, want 0", got)
	}
}

// TestSweepRemovalQueuesBehindPendingInsert pins the reaper→flusher
// removal-vs-insert ordering for the expiry path (TestUpsertCancelRaceNoZombie
// pins it for Cancel only): a registration due at birth may be swept while the
// publishing Upsert's insert is still unflushed — the sweep's mutRemove lands
// FIFO-after the pending insert, so no zombie entry can survive.
func TestSweepRemovalQueuesBehindPendingInsert(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = 5 * time.Millisecond
	e := New(cfg)
	defer e.Close()

	// Due at birth: validate accepts a past Expires as long as it is after
	// ValidFrom, so the registration is sweepable the instant the reaper sees it.
	now := time.Now()
	id := mkID(13)
	if err := e.Upsert(AlertSpec{ID: id, Symbol: "PEND", PriceType: PriceLast,
		Direction: DirGTE, TargetPrice: 100,
		ValidFrom: now.Add(-time.Hour).UnixNano(),
		Expires:   now.Add(-time.Millisecond).UnixNano()}); err != nil {
		t.Fatal(err)
	}
	// Deliberately no Sync while the reaper may act: the sweep can process the
	// registration before, during, or after the insert's flush. Settle.
	deadline := time.Now().Add(5 * time.Second)
	for {
		s := e.Stats()
		if s.Live == 0 && s.ExpiryLen == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("born-due alert not settled: Live=%d ExpiryLen=%d", s.Live, s.ExpiryLen)
		}
		time.Sleep(5 * time.Millisecond)
	}
	e.Sync()
	if _, ok := e.Status(id); ok {
		t.Fatal("expired alert still present in refs")
	}
	sid, ok := e.syms.Get("PEND")
	if !ok {
		t.Fatal("symbol not interned")
	}
	if got := e.states[sid].snap.Load().trees[treeIndex(PriceLast, DirGTE)].Len(); got != 0 {
		t.Fatalf("tree Len = %d after settle, want 0 (zombie entry)", got)
	}
}

// TestStatsConcurrentWithSweep pins read-side safety for the reaper-written
// gauges: while registrations, deregs, sweeps, and expLen writes run hot,
// concurrent Stats reads must stay race-free (meaningful under -race —
// TestStatusAndStatsConcurrent's mutation mix never sets Expires, so expLen
// is otherwise never reaper-written during a Stats read window) and the
// engine must settle to zero once the writers stop.
func TestStatsConcurrentWithSweep(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = 5 * time.Millisecond
	e := New(cfg)
	defer e.Close()

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for r := 0; r < 3; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = e.Stats()
			}
		}()
	}

	const iters = 500
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			id := mkID(uint32(i + 1))
			if err := e.Upsert(AlertSpec{ID: id, Symbol: "SCW", PriceType: PriceLast,
				Direction: DirGTE, TargetPrice: 100, ValidFrom: 1,
				Expires: time.Now().Add(20 * time.Millisecond).UnixNano()}); err != nil {
				t.Error(err)
				return
			}
			if i%2 == 0 {
				if err := e.Cancel(id); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()
	wg.Wait()
	close(stop)
	readers.Wait()
	e.Sync()

	deadline := time.Now().Add(5 * time.Second)
	for {
		s := e.Stats()
		if s.Live == 0 && s.ExpiryLen == 0 && s.MutQDepth == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("engine did not settle: %+v", s)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestUpsertAfterSweepExpiryTerminal pins the expired-flavored terminal
// branch (only StatusTriggered is constructed by existing tests): a same-ID
// Upsert landing on an EXPIRED slot must (a) enqueue no second removal for a
// removal the sweep already owns and (b) treat its deregExpiry of the
// already-deregistered key as an idempotent no-op. Mirrors
// TestSameKeyUpsertRacingFireRemoval, with the sweep's first half standing in
// for fire's.
func TestUpsertAfterSweepExpiryTerminal(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = time.Hour // no sweep interference: the expiry below is manual
	e := New(cfg)

	id := mkID(14)
	if err := e.Upsert(AlertSpec{ID: id, Symbol: "SWT", PriceType: PriceLast,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1,
		Expires: time.Now().Add(time.Hour).UnixNano()}); err != nil {
		t.Fatal(err)
	}
	e.Sync() // insert is published
	waitForExpLen(t, e, 1)
	e.mu.Lock()
	ref := e.refs[id]
	e.mu.Unlock()

	// Sweep's first half, exactly as sweep performs it: the terminal CAS plus
	// its mutRemove. The dereg stands in for the flusher's refClean pass that
	// would ride the removal. None of it is Synced yet.
	ok := e.slots.casStatusAny(entryIdx(ref.e), StatusExpired, StatusActive)
	if !ok {
		t.Fatal("setup: CAS ACTIVE→EXPIRED failed")
	}
	if err := e.submit(mutation{op: mutRemove, sid: ref.sid, e: ref.e}); err != nil {
		t.Fatal(err)
	}
	e.deregExpiry(ref)

	// Same-ID upsert in the window: the slot is EXPIRED, so Upsert takes the
	// terminal branch — CAS fails, hasReplace=false — and its deregExpiry of
	// the already-deregistered key must be a no-op.
	newExp := time.Now().Add(2 * time.Hour).UnixNano()
	if err := e.Upsert(AlertSpec{ID: id, Symbol: "SWT", PriceType: PriceLast,
		Direction: DirGTE, TargetPrice: 300, ValidFrom: 1, Expires: newExp}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if s := e.Stats(); s.Live != 1 {
		t.Fatalf("Live = %d, want 1", s.Live)
	}
	if n := e.states[ref.sid].snap.Load().trees[treeIndex(PriceLast, DirGTE)].Len(); n != 1 {
		t.Fatalf("tree Len = %d, want 1 (the replacement)", n)
	}
	waitForExpLen(t, e, 1)
	e.Close() // reaper drained and gone; table reads are safe
	var got []expEntry
	for x := range e.expiry.All() {
		got = append(got, x)
	}
	if len(got) != 1 || got[0].expires != newExp || got[0].ref == ref || got[0].ref.e.id != id {
		t.Fatalf("surviving registration = %+v, want exactly the new handout (expires %d)", got, newExp)
	}
}

// TestUpsertAlreadyDueDeadlineIsSwept drives the born-due window at volume:
// registrations whose deadline passed before the reaper ever saw them must be
// swept — the Delete-before-liveness-check ordering in sweep — leaving only
// the future-deadline control, with no fires.
func TestUpsertAlreadyDueDeadlineIsSwept(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = 2 * time.Millisecond
	e := New(cfg)
	defer e.Close()

	const n = 200
	now := time.Now()
	for i := 0; i < n; i++ {
		if err := e.Upsert(AlertSpec{ID: mkID(uint32(i + 1)), Symbol: "DUE",
			PriceType: PriceLast, Direction: DirGTE, TargetPrice: 100,
			ValidFrom: now.Add(-time.Hour).UnixNano(),
			Expires:   now.Add(-time.Millisecond).UnixNano()}); err != nil {
			t.Fatal(err)
		}
	}
	ctl := mkID(uint32(n + 1))
	if err := e.Upsert(AlertSpec{ID: ctl, Symbol: "DUE", PriceType: PriceLast,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1,
		Expires: now.Add(time.Hour).UnixNano()}); err != nil {
		t.Fatal(err)
	}
	e.Sync()

	deadline := time.Now().Add(5 * time.Second)
	for {
		s := e.Stats()
		if s.Live == 1 && s.ExpiryLen == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("born-expired alerts not swept: Live=%d ExpiryLen=%d", s.Live, s.ExpiryLen)
		}
		time.Sleep(5 * time.Millisecond)
	}
	e.Sync()
	for i := 1; i <= n; i++ {
		if _, ok := e.Status(mkID(uint32(i))); ok {
			t.Fatalf("born-expired alert %d still in refs", i)
		}
	}
	if st, ok := e.Status(ctl); !ok || st != StatusActive {
		t.Fatalf("control status = (%v, %v), want (StatusActive, true)", st, ok)
	}
	sid, ok := e.syms.Get("DUE")
	if !ok {
		t.Fatal("symbol not interned")
	}
	if got := e.states[sid].snap.Load().trees[treeIndex(PriceLast, DirGTE)].Len(); got != 1 {
		t.Fatalf("tree Len = %d, want 1 (control only)", got)
	}
	if trs := drainTriggers(e); len(trs) != 0 {
		t.Fatalf("born-expired alerts fired: %+v", trs)
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
		Expires: time.Now().Add(time.Hour).UnixNano()}); err != nil {
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
