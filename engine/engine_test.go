package engine

import (
	"bytes"
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
	if got := unsafe.Sizeof(entry{}); got != 48 {
		t.Fatalf("sizeof(entry) = %d, want 48 (check field order/padding)", got)
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
	low := entry{price: 1.5}
	high := entry{price: 2.5}
	a := entry{price: 2.5, id: AlertID{1}}
	b := entry{price: 2.5, id: AlertID{2}}
	if compareEntry(low, high) >= 0 || compareEntry(high, low) <= 0 {
		t.Fatal("price ordering broken")
	}
	if compareEntry(a, b) >= 0 || compareEntry(b, a) <= 0 {
		t.Fatal("id tie-break broken")
	}
	if compareEntry(a, a) != 0 {
		t.Fatal("equality broken")
	}
	var zero AlertID
	one := AlertID{1}
	if bytes.Compare(zero[:], one[:]) >= 0 {
		t.Fatal("zero AlertID must sort first (entryKey relies on it)")
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
		if s := Status(a.get(idx).Load()); s != StatusZero {
			t.Fatalf("fresh slot status = %v, want StatusZero", s)
		}
		a.get(idx).Store(uint32(StatusActive))
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
			if s := Status(a.get(idx).Load()); s != StatusZero {
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
		if !q.TryPush(Trigger{Price: float64(i)}) {
			t.Fatalf("push %d rejected on non-full queue", i)
		}
	}
	if q.TryPush(Trigger{}) {
		t.Fatal("push accepted on full queue")
	}
	if q.Dropped() != 1 {
		t.Fatalf("Dropped = %d, want 1", q.Dropped())
	}
	var got []float64
	for {
		tr, ok := q.Pop()
		if !ok {
			break
		}
		got = append(got, tr.Price)
	}
	if !slices.Equal(got, []float64{0, 1, 2, 3}) {
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
		q.TryPush(Trigger{Price: float64(i)})
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
				q.TryPush(Trigger{Price: float64(p*each + i)})
			}
		}(p)
	}
	delivered := make(map[float64]bool)
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
	s := newSnapshot()
	ti := treeIndex(PriceBid, DirGTE)
	s.trees[ti].Insert(entry{price: 42.5, id: AlertID{1}, idx: 1, flags: makeFlags(PriceBid, DirGTE, true)})
	cp := s.copy()
	cp.trees[ti].Insert(entry{price: 10, id: AlertID{2}, idx: 2, flags: makeFlags(PriceBid, DirGTE, true)})
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
	ent := entry{price: 42.5, id: AlertID{1}, idx: e.slots.alloc(),
		validFrom: 1, flags: makeFlags(PriceBid, DirGTE, true)}
	e.slots.get(ent.idx).Store(uint32(StatusActive))

	e.submit(mutation{op: mutInsert, sid: sid, e: ent})
	e.Sync()
	ti := treeIndex(PriceBid, DirGTE)
	if got := e.states[sid].snap.Load().trees[ti].Len(); got != 1 {
		t.Fatalf("after insert Len=%d, want 1", got)
	}

	e.submit(mutation{op: mutRemove, sid: sid, e: ent})
	e.Sync()
	if got := e.states[sid].snap.Load().trees[ti].Len(); got != 0 {
		t.Fatalf("after remove Len=%d, want 0", got)
	}
	// mutRemove cleanup: refs/meta deleted, live decremented, slot retired.
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
		e.slots.get(idx).Store(uint32(StatusActive))
		e.mu.Lock()
		e.refs[AlertID{byte(i + 1)}] = &alertRef{sid: sid, e: entry{
			price: float64(i), id: AlertID{byte(i + 1)}, idx: idx,
			flags: makeFlags(PriceLast, DirLTE, false)}}
		e.live++
		e.mu.Unlock()
		e.submit(mutation{op: mutInsert, sid: sid,
			e: entry{price: float64(i), id: AlertID{byte(i + 1)}, idx: idx,
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
