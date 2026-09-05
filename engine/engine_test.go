package engine

import (
	"bytes"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
	"unsafe"

	"go.uber.org/goleak"
)

func TestEntrySize(t *testing.T) {
	// Field order is chosen for minimal padding; hot arena must stay dense.
	if got := unsafe.Sizeof(entry{}); got != 64 {
		t.Fatalf("sizeof(entry) = %d, want 64 (check field order/padding)", got)
	}
}

func TestFlagRoundTrip(t *testing.T) {
	for _, pt := range []PriceType{PriceBid, PriceAsk, PriceMid, PriceLast} {
		for _, dir := range []Direction{DirGTE, DirLTE} {
			for _, auto := range []bool{false, true} {
				f := makeFlags(pt, dir, auto)
				e := entry{flags: f}
				if e.priceType() != pt {
					t.Fatalf("priceType round trip: got %d want %d", e.priceType(), pt)
				}
				if e.direction() != dir {
					t.Fatalf("direction round trip: got %d want %d", e.direction(), dir)
				}
				if e.autoDeactivate() != auto {
					t.Fatalf("autoDeactivate round trip: got %v want %v", e.autoDeactivate(), auto)
				}
			}
		}
	}
}

func TestCompareEntry(t *testing.T) {
	cmp := makeEntryCompare(0)
	low := entry{price: 15}
	high := entry{price: 25}
	a := entry{price: 25, id: AlertID{1}}
	b := entry{price: 25, id: AlertID{2}}
	if cmp(low, high) >= 0 || cmp(high, low) <= 0 {
		t.Fatal("price ordering broken")
	}
	if cmp(a, b) >= 0 || cmp(b, a) <= 0 {
		t.Fatal("id tie-break broken")
	}
	if cmp(a, a) != 0 {
		t.Fatal("equality broken")
	}
	var zero AlertID
	one := AlertID{1}
	if bytes.Compare(zero[:], one[:]) >= 0 {
		t.Fatal("zero AlertID must sort first (entryKey relies on it)")
	}
	// dims sort before price, slot 0 most significant (needs a width > 0
	// comparator; the width-0 cmp above ignores dims by construction).
	wide := makeEntryCompare(dimMax)
	x := entry{dims: Dims(1, 2), price: 100}
	y := entry{dims: Dims(1, 3), price: 0}
	if wide(x, y) >= 0 || wide(y, x) <= 0 {
		t.Fatal("dims ordering broken: slot 1 must outrank price")
	}
	if wide(entry{dims: Dims(1)}, entry{dims: Dims(2)}) >= 0 {
		t.Fatal("dims ordering broken: slot 0")
	}
}

func TestSlotArena(t *testing.T) {
	a := newSlotArena(1000)
	seen := map[uint32]bool{}
	for i := 0; i < 100; i++ {
		idx := a.alloc()
		if seen[idx] {
			t.Fatalf("idx %d handed out twice", idx)
		}
		seen[idx] = true
		if s := a.status(idx); s != StatusZero {
			t.Fatalf("fresh slot status = %v, want StatusZero", s)
		}
		a.setStatus(idx, StatusActive)
	}
	// Retire two slots; they must not be reusable until recycle's grace passes.
	a.retire(7)
	a.retire(8)
	for i := 0; i < 10; i++ {
		if idx := a.alloc(); idx == 7 || idx == 8 {
			t.Fatal("retired slot reused before recycle")
		}
	}
	// Recycle with a before-time in the future: retired slots return to use.
	a.recycle(time.Now().Add(time.Hour))
	reused := 0
	for i := 0; i < 2; i++ {
		idx := a.alloc()
		if idx == 7 || idx == 8 {
			reused++
			// The word differs from its previous era (generation bumped at
			// handout) but the status is a clean StatusZero.
			if s := a.status(idx); s != StatusZero {
				t.Fatal("recycled slot not reset to StatusZero")
			}
		}
	}
	if reused != 2 {
		t.Fatal("recycle did not return both retired slots")
	}
}

func TestTriggerQueueFIFO(t *testing.T) {
	q := NewTriggerQueue(4)
	for i := 0; i < 4; i++ {
		if !q.TryPush(Trigger{Price: Price(i)}) {
			t.Fatalf("push %d rejected on non-full queue", i)
		}
	}
	if q.TryPush(Trigger{}) {
		t.Fatal("push accepted on full queue")
	}
	if q.Dropped() != 1 {
		t.Fatalf("Dropped = %d, want 1", q.Dropped())
	}
	var got []Price
	for {
		tr, ok := q.Pop()
		if !ok {
			break
		}
		got = append(got, tr.Price)
	}
	if !slices.Equal(got, []Price{0, 1, 2, 3}) {
		t.Fatalf("FIFO broken: %v", got)
	}
	// Queue is empty again; slot reused after full cycle.
	if !q.TryPush(Trigger{Price: 9}) {
		t.Fatal("push rejected after drain")
	}
	tr, ok := q.Pop()
	if !ok || tr.Price != 9 {
		t.Fatal("reuse after drain broken")
	}
}

func TestTriggerQueuePopBatch(t *testing.T) {
	q := NewTriggerQueue(8)
	for i := 0; i < 5; i++ {
		q.TryPush(Trigger{Price: Price(i)})
	}
	dst := make([]Trigger, 3)
	if n := q.PopBatch(dst); n != 3 {
		t.Fatalf("PopBatch = %d, want 3", n)
	}
	if n := q.PopBatch(dst); n != 2 {
		t.Fatalf("PopBatch = %d, want 2", n)
	}
	if n := q.PopBatch(dst); n != 0 {
		t.Fatalf("PopBatch = %d, want 0", n)
	}
}

func TestTriggerQueueConcurrent(t *testing.T) {
	q := NewTriggerQueue(1024)
	const producers, each = 8, 10_000
	var wg sync.WaitGroup
	producersDone := make(chan struct{})
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				// Unique value per push: duplicate delivery is detectable.
				q.TryPush(Trigger{Price: Price(p*each + i)})
			}
		}(p)
	}
	delivered := make(map[Price]bool)
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			tr, ok := q.Pop()
			if ok {
				if delivered[tr.Price] {
					t.Errorf("trigger %v delivered twice", tr.Price)
					return
				}
				delivered[tr.Price] = true
				continue
			}
			select {
			case <-producersDone: // drained after all producers finished
				return
			default:
				runtime.Gosched()
			}
		}
	}()
	wg.Wait()
	close(producersDone)
	<-consumerDone
	// Drop+count contract: every push was either delivered exactly once or
	// counted in Dropped. With the consumer draining until empty after the
	// producers finish, every successful TryPush is eventually popped.
	total := producers * each
	if got := len(delivered) + int(q.Dropped()); got != total {
		t.Fatalf("delivered %d + dropped %d = %d, want %d",
			len(delivered), q.Dropped(), got, total)
	}
}

func TestInterner(t *testing.T) {
	in := NewInterner()
	if _, ok := in.Get("USDTRY"); ok {
		t.Fatal("Get on empty interner returned true")
	}
	a := in.Intern("USDTRY")
	b := in.Intern("EURTRY")
	if a == b {
		t.Fatal("distinct symbols got same id")
	}
	if again := in.Intern("USDTRY"); again != a {
		t.Fatal("re-intern returned different id")
	}
	if got, ok := in.Get("USDTRY"); !ok || got != a {
		t.Fatal("Get after intern failed")
	}
	if in.Name(a) != "USDTRY" || in.Name(b) != "EURTRY" {
		t.Fatal("Name round trip broken")
	}
}

func TestTreeIndexDistinct(t *testing.T) {
	seen := map[int]bool{}
	for pt := PriceType(0); pt < priceTypeCount; pt++ {
		for _, dir := range []Direction{DirGTE, DirLTE} {
			i := treeIndex(pt, dir)
			if i < 0 || i >= 8 {
				t.Fatalf("treeIndex(%d,%d)=%d out of range", pt, dir, i)
			}
			if seen[i] {
				t.Fatalf("treeIndex(%d,%d)=%d collides", pt, dir, i)
			}
			seen[i] = true
		}
	}
	if len(seen) != 8 {
		t.Fatalf("expected 8 distinct trees, got %d", len(seen))
	}
}

func TestSnapshotCopyIsolation(t *testing.T) {
	s := newSnapshot(0)
	ti := treeIndex(PriceBid, DirGTE)
	s.trees[ti].Insert(entry{price: 425, id: AlertID{1}, idx: 1, flags: makeFlags(PriceBid, DirGTE, true)})
	cp := s.copy()
	cp.trees[ti].Insert(entry{price: 100, id: AlertID{2}, idx: 2, flags: makeFlags(PriceBid, DirGTE, true)})
	if s.trees[ti].Len() != 1 || cp.trees[ti].Len() != 2 {
		t.Fatalf("COW isolation broken: orig=%d copy=%d, want 1 and 2", s.trees[ti].Len(), cp.trees[ti].Len())
	}
	s.release()
	cp.release()
}

func TestEngineNewClose(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	if e.Triggers() == nil {
		t.Fatal("nil trigger queue")
	}
	if s := e.Stats(); s.Live != 0 || s.DroppedTriggers != 0 {
		t.Fatalf("fresh engine stats = %+v, want zeros", s)
	}
	e.Close()
	e.Close() // must be idempotent
}

func TestFlusherAppliesMutations(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	sid, err := e.stateFor("USDTRY")
	if err != nil {
		t.Fatal(err)
	}
	if e.states[sid].snap.Load() == nil {
		t.Fatal("stateFor did not publish an initial snapshot")
	}
	ent := entry{price: 425, id: AlertID{1}, idx: e.slots.alloc(),
		validFrom: 1, flags: makeFlags(PriceBid, DirGTE, true)}
	e.slots.setStatus(ent.idx, StatusActive)
	entGen := e.slots.gen(ent.idx)

	e.submit(mutation{op: mutInsert, sid: sid, e: ent})
	e.Sync()
	ti := treeIndex(PriceBid, DirGTE)
	if got := e.states[sid].snap.Load().trees[ti].Len(); got != 1 {
		t.Fatalf("after insert Len=%d, want 1", got)
	}

	e.submit(mutation{op: mutRemove, sid: sid, e: ent, gen: entGen})
	e.Sync()
	if got := e.states[sid].snap.Load().trees[ti].Len(); got != 0 {
		t.Fatalf("after remove Len=%d, want 0", got)
	}
	// mutRemove cleanup: refs deleted, live decremented, slot retired.
	if _, ok := e.refs[ent.id]; ok {
		t.Fatal("refs entry survived removal")
	}
}

func TestSyncBarrier(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	sid, _ := e.stateFor("EURTRY")
	for i := 0; i < 100; i++ {
		idx := e.slots.alloc()
		e.slots.setStatus(idx, StatusActive)
		e.mu.Lock()
		e.refs[AlertID{byte(i + 1)}] = &alertRef{sid: sid, e: entry{
			price: Price(i), id: AlertID{byte(i + 1)}, idx: idx,
			flags: makeFlags(PriceLast, DirLTE, false)}}
		e.live++
		e.mu.Unlock()
		e.submit(mutation{op: mutInsert, sid: sid,
			e: entry{price: Price(i), id: AlertID{byte(i + 1)}, idx: idx,
				validFrom: 1, flags: makeFlags(PriceLast, DirLTE, false)}})
	}
	e.Sync()
	if got := e.states[sid].snap.Load().trees[treeIndex(PriceLast, DirLTE)].Len(); got != 100 {
		t.Fatalf("Len=%d, want 100", got)
	}
}

func TestSyncAfterCloseReturns(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	e.Close()
	done := make(chan struct{})
	go func() {
		e.Sync() // must not hang on a closed engine
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Sync hung after Close")
	}
}

func TestStateForOverLimitIsStable(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.MaxSymbols = 2
	e := New(cfg)
	defer e.Close()
	if _, err := e.stateFor("S0"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.stateFor("S0"); err != nil { // fast path, in range
		t.Fatal(err)
	}
	if _, err := e.stateFor("S1"); err != nil { // fills the table
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ { // rejected interning; repeat calls hit the fast path
		if _, err := e.stateFor("SX"); err != ErrSymbolLimit {
			t.Fatalf("call %d: err=%v, want ErrSymbolLimit", i, err)
		}
	}
}

func testSpec(id byte, sym string, pt PriceType, dir Direction, price Price) AlertSpec {
	return AlertSpec{
		ID: AlertID{id}, Symbol: sym, PriceType: pt, Direction: dir,
		TargetPrice: price, ValidFrom: 1, AutoDeactivate: true,
	}
}

func TestUpsertInsertAndReplace(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	sid, _ := e.syms.Get("USDTRY")
	if got := e.states[sid].snap.Load().trees[treeIndex(PriceBid, DirGTE)].Len(); got != 1 {
		t.Fatalf("Len=%d, want 1", got)
	}
	if s := e.Stats(); s.Live != 1 {
		t.Fatalf("Live=%d, want 1", s.Live)
	}
	// Replace same ID with a different price: still exactly one live alert.
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 430)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if s := e.Stats(); s.Live != 1 {
		t.Fatalf("after replace Live=%d, want 1", s.Live)
	}
	ti := treeIndex(PriceBid, DirGTE)
	n := 0
	for en := range e.states[sid].snap.Load().trees[ti].All() {
		n++
		if en.price != 430 {
			t.Fatalf("stale entry price=%v, want 43", en.price)
		}
	}
	if n != 1 {
		t.Fatalf("entries=%d, want 1", n)
	}
}

func TestUpsertValidation(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	cases := []AlertSpec{
		{Symbol: "X", PriceType: PriceBid, Direction: DirGTE},                       // zero ID
		{ID: AlertID{1}, PriceType: PriceBid, Direction: DirGTE},                    // empty symbol
		{ID: AlertID{1}, Symbol: "X", PriceType: priceTypeCount, Direction: DirGTE}, // bad price type
		{ID: AlertID{1}, Symbol: "X", PriceType: PriceBid, Direction: 99},           // bad direction
		{ID: AlertID{1}, Symbol: "X", PriceType: PriceBid, Direction: DirGTE,
			ValidFrom: 100, Expires: 50}, // expires before valid
	}
	for i, c := range cases {
		if err := e.Upsert(c); err == nil {
			t.Fatalf("case %d: expected validation error", i)
		}
	}
}

func TestUpsertLimits(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	// MaxAlerts covers the alert-limit probe plus the five live alerts needed
	// for the symbol-limit section (USDTRY + S0..S2).
	cfg.MaxAlerts = 5
	cfg.MaxSymbols = 4
	e := New(cfg)
	defer e.Close()
	for i := byte(0); i < 2; i++ {
		if err := e.Upsert(testSpec(i+1, "USDTRY", PriceBid, DirGTE, 425)); err != nil {
			t.Fatal(err)
		}
	}
	// MaxSymbols=4 with USDTRY already interned: S0..S2 fill the remaining
	// slots; S3 must be rejected.
	for i := byte(0); i < 3; i++ {
		if err := e.Upsert(testSpec(10+i, fmt.Sprintf("S%d", i), PriceBid, DirGTE, 1)); err != nil {
			t.Fatalf("symbol %d: err=%v, want nil", i, err)
		}
	}
	// live == MaxAlerts now: a fresh insert must be rejected.
	if err := e.Upsert(testSpec(3, "USDTRY", PriceBid, DirGTE, 425)); err != ErrAlertLimit {
		t.Fatalf("err=%v, want ErrAlertLimit", err)
	}
	if err := e.Upsert(testSpec(20, "S3", PriceBid, DirGTE, 1)); err != ErrSymbolLimit {
		t.Fatalf("err=%v, want ErrSymbolLimit", err)
	}
}

func drainTriggers(e *Engine) []Trigger {
	var out []Trigger
	dst := make([]Trigger, 64)
	for {
		n := e.Triggers().PopBatch(dst)
		if n == 0 {
			return out
		}
		out = append(out, dst[:n]...)
	}
}

func TestMatchGTEFiresOnce(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425))
	e.Sync()
	tick := Tick{Symbol: "USDTRY", Bid: 430, Present: TickAllPresent(), TS: 100}
	e.Match(&tick)
	e.Match(&tick) // duplicate delivery of the same tick must not re-fire
	got := drainTriggers(e)
	if len(got) != 1 || got[0].ID != (AlertID{1}) || got[0].Price != 430 {
		t.Fatalf("triggers=%+v, want one fire of alert 1 at 430", got)
	}
	e.Sync()
	sid, _ := e.syms.Get("USDTRY")
	if n := e.states[sid].snap.Load().trees[treeIndex(PriceBid, DirGTE)].Len(); n != 0 {
		t.Fatalf("fired alert still indexed: %d entries", n)
	}
}

func TestMatchLTEAndBoundary(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	// LTE at exactly the market price must fire (<=).
	e.Upsert(testSpec(2, "USDTRY", PriceAsk, DirLTE, 500))
	e.Sync()
	e.Match(&Tick{Symbol: "USDTRY", Ask: 500, Present: TickAllPresent(), TS: 100})
	got := drainTriggers(e)
	if len(got) != 1 || got[0].ID != (AlertID{2}) {
		t.Fatalf("triggers=%+v, want one fire of alert 2", got)
	}
}

func TestMatchSkips(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	// Paused.
	e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425))
	e.SetStatus(AlertID{1}, StatusPaused)
	// Not yet valid.
	e.Upsert(AlertSpec{ID: AlertID{2}, Symbol: "USDTRY", PriceType: PriceBid,
		Direction: DirGTE, TargetPrice: 425, ValidFrom: 200, AutoDeactivate: true})
	// Expired (lazy inline check; reaper runs on 1s cadence, don't wait).
	e.Upsert(AlertSpec{ID: AlertID{3}, Symbol: "USDTRY", PriceType: PriceBid,
		Direction: DirGTE, TargetPrice: 425, ValidFrom: 1, Expires: 50, AutoDeactivate: true})
	// Wrong price type: alert on BID, tick carries only ASK.
	e.Upsert(testSpec(4, "USDTRY", PriceBid, DirGTE, 425))
	e.Sync()
	e.Match(&Tick{Symbol: "USDTRY", Ask: 1000, Present: 1 << uint(PriceAsk), TS: 100})
	if got := drainTriggers(e); len(got) != 0 {
		t.Fatalf("skips failed, triggers=%+v", got)
	}
	// Unknown symbol: no-op.
	e.Match(&Tick{Symbol: "NOPE", Bid: 1000, Present: TickAllPresent(), TS: 100})
	// Not triggered by later valid tick for alert 1 (still paused) — sanity.
	// Alert 4 is an active BID alert and legitimately fires on this tick;
	// only alerts 1–3 (paused / not yet valid / expired) must stay silent.
	e.Match(&Tick{Symbol: "USDTRY", Bid: 1000, Present: 1 << uint(PriceBid), TS: 100})
	for _, tr := range drainTriggers(e) {
		if tr.ID == (AlertID{1}) || tr.ID == (AlertID{2}) || tr.ID == (AlertID{3}) {
			t.Fatalf("skip guard fired: %+v", tr)
		}
	}
}

func TestMatchZeroAllocs(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	for i := byte(0); i < 50; i++ {
		e.Upsert(testSpec(i+1, fmt.Sprintf("S%d", i%5), PriceType(i%4),
			Direction(i%2), Price(i)*10))
	}
	e.Sync()
	tick := Tick{Symbol: "S3", Bid: 1e9, Ask: 1e9, Mid: 1e9, Last: 1e9,
		Present: TickAllPresent(), TS: 1 << 40}
	allocs := testing.AllocsPerRun(200, func() { e.Match(&tick) })
	if allocs != 0 {
		t.Fatalf("Match allocates: %.0f allocs/op, want 0", allocs)
	}
}

func TestCancelAndSetStatus(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425))
	e.Sync()
	if err := e.SetStatus(AlertID{1}, StatusPaused); err != nil {
		t.Fatal(err)
	}
	if err := e.SetStatus(AlertID{1}, StatusActive); err != nil {
		t.Fatal(err)
	}
	if err := e.SetStatus(AlertID{1}, StatusActive); err != nil {
		t.Fatal("idempotent same-state SetStatus should succeed:", err)
	}
	if err := e.SetStatus(AlertID{1}, Status(99)); err != ErrInvalidStatus {
		t.Fatalf("err=%v, want ErrInvalidStatus", err)
	}
	if err := e.Cancel(AlertID{1}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if s := e.Stats(); s.Live != 0 {
		t.Fatalf("Live=%d after cancel, want 0", s.Live)
	}
	if err := e.Cancel(AlertID{1}); err != ErrNotFound {
		t.Fatalf("err=%v, want ErrNotFound", err)
	}
	if err := e.SetStatus(AlertID{2}, StatusPaused); err != ErrNotFound {
		t.Fatalf("err=%v, want ErrNotFound", err)
	}
}

func TestReaperExpires(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = 20 * time.Millisecond
	e := New(cfg)
	defer e.Close()
	expiry := time.Now().Add(40 * time.Millisecond).UnixNano()
	e.Upsert(AlertSpec{ID: AlertID{9}, Symbol: "USDTRY", PriceType: PriceBid,
		Direction: DirGTE, TargetPrice: 425, ValidFrom: 1, Expires: expiry,
		AutoDeactivate: true})
	e.Sync()
	if s := e.Stats(); s.Live != 1 {
		t.Fatalf("Live=%d, want 1", s.Live)
	}
	// Wait long enough for expiry + one sweep + one flush.
	deadline := time.Now().Add(2 * time.Second)
	for e.Stats().Live != 0 {
		if time.Now().After(deadline) {
			t.Fatal("alert never expired")
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.Sync()
	sid, _ := e.syms.Get("USDTRY")
	if n := e.states[sid].snap.Load().trees[treeIndex(PriceBid, DirGTE)].Len(); n != 0 {
		t.Fatalf("expired entry still indexed: %d", n)
	}
}

func TestStaleExpiryDoesNotKillReusedSlot(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = 20 * time.Millisecond // recycle grace = 40ms
	e := New(cfg)
	defer e.Close()
	expiry := time.Now().Add(250 * time.Millisecond).UnixNano()
	// A: expiring alert, cancelled immediately; slot retired then recycled.
	e.Upsert(AlertSpec{ID: AlertID{1}, Symbol: "USDTRY", PriceType: PriceBid,
		Direction: DirGTE, TargetPrice: 425, ValidFrom: 1, Expires: expiry,
		AutoDeactivate: true})
	e.mu.Lock()
	aIdx := e.refs[AlertID{1}].e.idx
	e.mu.Unlock()
	if err := e.Cancel(AlertID{1}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	// Wait past the recycle grace so A's slot is back on the free list.
	time.Sleep(80 * time.Millisecond)
	// B: never-expiring alert; must recycle A's slot.
	if err := e.Upsert(AlertSpec{ID: AlertID{2}, Symbol: "EURTRY", PriceType: PriceAsk,
		Direction: DirLTE, TargetPrice: 500, ValidFrom: 1, AutoDeactivate: true}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	e.mu.Lock()
	bIdx := e.refs[AlertID{2}].e.idx
	e.mu.Unlock()
	if aIdx != bIdx {
		t.Fatalf("test setup failed: B got slot %d, wanted recycled %d", bIdx, aIdx)
	}
	// Wait well past A's expiry and several sweeps.
	time.Sleep(350 * time.Millisecond)
	e.Sync()
	// B must still be ACTIVE and must still fire.
	if s := e.slots.status(bIdx); s != StatusActive {
		t.Fatalf("B's slot status = %v, want StatusActive (stale expiry killed it)", s)
	}
	e.Match(&Tick{Symbol: "EURTRY", Ask: 490, Present: 1 << uint(PriceAsk), TS: time.Now().UnixNano()})
	fired := false
	for _, tr := range drainTriggers(e) {
		if tr.ID == (AlertID{2}) {
			fired = true
		}
	}
	if !fired {
		t.Fatal("B failed to fire after A's stale expiry entry was swept")
	}
}

// TestDuplicateRemovalDoesNotAliasSlots pins the double-retire bug: two
// mutRemoves landing for one entry used to park the slot twice, so alloc
// handed the SAME index to two future alerts. retireGen's retired-bit dedupe
// must make the second landing a no-op. The direct part mirrors the
// reviewer's repro against the internal API.
func TestDuplicateRemovalDoesNotAliasSlots(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	sid, err := e.stateFor("USDTRY")
	if err != nil {
		t.Fatal(err)
	}
	idx := e.slots.alloc()
	e.slots.setStatus(idx, StatusActive)
	gen := e.slots.gen(idx)
	ent := entry{price: 425, id: AlertID{1}, idx: idx, validFrom: 1,
		flags: makeFlags(PriceBid, DirGTE, true)}
	e.submit(mutation{op: mutInsert, sid: sid, e: ent})
	e.Sync()
	// Two direct mutRemove submissions for the same entry (gen-identical):
	// a replacement racing a fire's removal.
	e.submit(mutation{op: mutRemove, sid: sid, e: ent, gen: gen})
	e.submit(mutation{op: mutRemove, sid: sid, e: ent, gen: gen})
	e.Sync()
	e.slots.recycle(time.Now().Add(time.Hour))
	hits := 0
	var prev uint32
	for i := 0; i < 2; i++ {
		a := e.slots.alloc()
		if i > 0 && a == prev {
			t.Fatalf("alloc handed the same index %d twice", a)
		}
		if a == idx {
			hits++
		}
		prev = a
	}
	if hits > 1 {
		t.Fatalf("retired index %d returned to the free list %d times, want at most 1", idx, hits)
	}

	// End-to-end: fire an alert, then Upsert-replace the same ID while the
	// slot is TRIGGERED. The replace must NOT queue a second removal for a
	// slot whose removal fire already owns.
	e2 := New(DefaultConfig())
	defer e2.Close()
	if err := e2.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425)); err != nil {
		t.Fatal(err)
	}
	e2.Sync()
	e2.mu.Lock()
	oldIdx := e2.refs[AlertID{1}].e.idx
	e2.mu.Unlock()
	e2.Match(&Tick{Symbol: "USDTRY", Bid: 430, Present: TickAllPresent(), TS: 100})
	if got := drainTriggers(e2); len(got) != 1 || got[0].ID != (AlertID{1}) {
		t.Fatalf("triggers=%+v, want one fire of alert 1", got)
	}
	// Replace while TRIGGERED (fire's removal queued, not yet landed).
	if err := e2.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 990)); err != nil {
		t.Fatal(err)
	}
	e2.Sync()
	if s := e2.Stats(); s.Live != 1 {
		t.Fatalf("after replace Live=%d, want 1", s.Live)
	}
	sid2, _ := e2.syms.Get("USDTRY")
	if n := e2.states[sid2].snap.Load().trees[treeIndex(PriceBid, DirGTE)].Len(); n != 1 {
		t.Fatalf("entries=%d, want exactly the replacement", n)
	}
	// oldIdx must have been retired at most once; allocs stay distinct.
	// The reviewer looped up to 300 attempts; with the fix it passes first.
	e2.slots.recycle(time.Now().Add(time.Hour))
	a1 := e2.slots.alloc()
	a2 := e2.slots.alloc()
	if a1 == a2 {
		t.Fatalf("alloc handed the same index %d twice after replace", a1)
	}
	if a1 != oldIdx && a2 != oldIdx {
		// Fine: free-list ordering is an implementation detail; what matters
		// is that oldIdx appears at most once among fresh handouts.
	}
	seen := 0
	for _, a := range []uint32{a1, a2} {
		if a == oldIdx {
			seen++
		}
	}
	if seen > 1 {
		t.Fatalf("old index %d handed out %d times, want at most 1", oldIdx, seen)
	}
}

// TestMatchOverLimitSymbolNoPanic pins the Match bounds bug: a symbol
// interned past MaxSymbols is rejected by stateFor but stays in the
// interner, so its sid indexes past states and Match used to panic.
func TestMatchOverLimitSymbolNoPanic(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.MaxSymbols = 2
	e := New(cfg)
	defer e.Close()
	if _, err := e.stateFor("A"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.stateFor("B"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.stateFor("CX"); err != ErrSymbolLimit {
		t.Fatalf("err=%v, want ErrSymbolLimit", err)
	}
	// "CX" is interned with sid == len(states) forever: ticks for it must be
	// silent no-ops, not panics.
	for i := 0; i < 3; i++ {
		e.Match(&Tick{Symbol: "CX", Bid: 1, Ask: 1, Mid: 1, Last: 1,
			Present: TickAllPresent(), TS: 100})
	}
	if got := drainTriggers(e); len(got) != 0 {
		t.Fatalf("triggers=%v, want none", got)
	}
}

// TestIntegritySweepCleansLeakedTriggered pins the lost-enqueue leak: a
// TRIGGERED entry whose removal never reached the mutation queue used to
// stay indexed forever. The integrity sweep must re-submit it.
func TestIntegritySweepCleansLeakedTriggered(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = 5 * time.Millisecond
	cfg.IntegrityEvery = 2 // integrity every 10ms
	e := New(cfg)
	defer e.Close()
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425)); err != nil {
		t.Fatal(err) // Expires 0 → sentinel registration
	}
	e.Sync()
	// Simulate a lost enqueue: transition Active→Triggered directly,
	// bypassing fire's trySubmit entirely.
	e.mu.Lock()
	ref := e.refs[AlertID{1}]
	e.mu.Unlock()
	s := e.slots.get(ref.e.idx)
	w := s.Load()
	if !s.CompareAndSwap(w, w&^0xff|uint32(StatusTriggered)) {
		t.Fatal("setup CAS failed")
	}
	deadline := time.Now().Add(2 * time.Second)
	for e.Stats().Live != 0 {
		if time.Now().After(deadline) {
			t.Fatal("leaked TRIGGERED entry never cleaned by the integrity sweep")
		}
		time.Sleep(2 * time.Millisecond)
	}
	e.Sync()
	sid, _ := e.syms.Get("USDTRY")
	if n := e.states[sid].snap.Load().trees[treeIndex(PriceBid, DirGTE)].Len(); n != 0 {
		t.Fatalf("tree not empty after integrity sweep: %d entries", n)
	}
}

// TestCloseWaitsForReaders pins the Close enforcement: trees must not be
// released while a reader still holds a pin, and Close must complete once
// the reader drains.
func TestCloseWaitsForReaders(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	sid, err := e.stateFor("USDTRY")
	if err != nil {
		t.Fatal(err)
	}
	snap := e.states[sid].snap.Load()
	if !snap.pin() {
		t.Fatal("pin failed on a live snapshot")
	}
	closed := make(chan struct{})
	go func() {
		e.Close()
		close(closed)
	}()
	select {
	case <-closed:
		snap.unpin()
		t.Fatal("Close returned while a reader held the snapshot")
	case <-time.After(50 * time.Millisecond):
	}
	snap.unpin()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not complete after readers drained")
	}
}

// TestExpiryRegistrySlotReuse pins the comparator-collision hole: the expiry
// registry keyed by (expires, idx) alone made a recycled slot's new
// registration equal to its previous occupant's stale entry, and btype
// Insert is a no-op on equal keys — the new registration was silently
// dropped. The gen tie-break must keep both entries distinct.
func TestExpiryRegistrySlotReuse(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	// Slow reaper: recycle grace 400ms, integrity cadence 30 ticks = 6s, so
	// the stale registration deterministically outlives this test and the
	// collision window is wide open when B registers.
	cfg.ReaperInterval = 200 * time.Millisecond
	e := New(cfg)
	defer e.Close() // any Fatalf below must not leak engine goroutines into later tests
	// A: never-expiring; gets slot X plus a sentinel registration.
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	e.mu.Lock()
	aIdx := e.refs[AlertID{1}].e.idx
	e.mu.Unlock()
	if err := e.Cancel(AlertID{1}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	// Wait for the reaper to return slot X to the free list. The grace is
	// 2× interval (400ms), but recycling only happens on a reaper tick, and
	// the qualifying tick's phase plus scheduling under load can push the
	// recycle past any fixed sleep — poll the condition instead.
	deadline := time.Now().Add(5 * time.Second)
	for {
		e.slots.mu.Lock()
		freed := slices.Contains(e.slots.free, aIdx)
		e.slots.mu.Unlock()
		if freed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot %d not recycled within 5s", aIdx)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// B: never-expiring; must recycle slot X and register with a NEW gen.
	if err := e.Upsert(testSpec(2, "USDTRY", PriceBid, DirGTE, 430)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	e.mu.Lock()
	bIdx := e.refs[AlertID{2}].e.idx
	e.mu.Unlock()
	if bIdx != aIdx {
		t.Fatalf("setup failed: B got slot %d, wanted recycled %d", bIdx, aIdx)
	}
	// Let the reaper drain B's registration (channel length is safe to poll;
	// the table itself stays reaper-owned until Close quiesces it), then
	// stop it: after Close the expiry table is quiescent and safe to inspect.
	deadline = time.Now().Add(5 * time.Second)
	for len(e.expQ) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("B registration not drained from expQ within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.Close()
	bGen := e.slots.gen(bIdx)
	found := 0
	for x := range e.expiry.All() {
		if x.e.id != (AlertID{2}) {
			continue
		}
		if x.expires != expiryNever {
			t.Fatalf("B registration expires=%d, want the never-due sentinel", x.expires)
		}
		if x.gen != bGen {
			t.Fatalf("stale registration for B: gen %d, want the new occupant's %d", x.gen, bGen)
		}
		found++
	}
	if found != 1 {
		t.Fatalf("registry holds %d registrations for B, want exactly 1 (collision dropped it)", found)
	}
}
