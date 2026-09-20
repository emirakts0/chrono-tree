package engine

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
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

func TestEntryMetaPackUnpack(t *testing.T) {
	idx, gen, flags := uint32(0xdeadbeef), uint32(0x7f_ff_ff), uint8(0xa5)
	en := entry{meta: makeEntryMeta(idx, gen, flags)}
	if got := entryIdx(en); got != idx {
		t.Fatalf("entryIdx = %#x, want %#x", got, idx)
	}
	// gen occupies 23 bits, exactly what cur>>slotGenShift yields.
	if got := entryGen(en); got != gen {
		t.Fatalf("entryGen = %#x, want %#x", got, gen)
	}
	if got := entryFlags(en); got != flags {
		t.Fatalf("entryFlags = %#x, want %#x", got, flags)
	}
	// The Descend-probe sentinel (next task but one) must sort after any
	// meta whose idx is a real arena index: idx is the major 32 bits.
	probe := uint64(^uint32(0)) << 32
	if !(en.meta < probe) {
		t.Fatalf("real-entry meta %#x not before probe sentinel %#x", en.meta, probe)
	}
	// gen is masked to its 23 bits: an oversized gen must not change the word.
	if makeEntryMeta(idx, gen|1<<23, flags) != en.meta {
		t.Fatal("oversized gen leaked past its 23 bits")
	}
}

func TestExpEntrySize(t *testing.T) {
	// One registration per alert: ref borrows the shared alertRef so the
	// table carries no entry copy, idx, or generation — liveness is checked
	// by pointer identity, and the comparator's tie-break is the pointer
	// itself.
	if got := unsafe.Sizeof(expEntry{}); got != 16 {
		t.Fatalf("sizeof(expEntry) = %d, want 16 (check field order/padding)", got)
	}
}

func TestFlagRoundTrip(t *testing.T) {
	for _, pt := range []PriceType{PriceBid, PriceAsk, PriceMid, PriceLast} {
		for _, dir := range []Direction{DirGTE, DirLTE} {
			f := makeFlags(pt, dir)
			e := entry{meta: makeEntryMeta(0, 0, f)}
			if e.priceType() != pt {
				t.Fatalf("priceType round trip: got %d want %d", e.priceType(), pt)
			}
			if e.direction() != dir {
				t.Fatalf("direction round trip: got %d want %d", e.direction(), dir)
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
		idx, _ := a.alloc()
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
	a.retire(7, time.Now())
	a.retire(8, time.Now())
	for i := 0; i < 10; i++ {
		if idx, _ := a.alloc(); idx == 7 || idx == 8 {
			t.Fatal("retired slot reused before recycle")
		}
	}
	// Recycle with a before-time in the future: retired slots return to use.
	a.recycle(time.Now().Add(time.Hour))
	reused := 0
	for i := 0; i < 2; i++ {
		idx, _ := a.alloc()
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

func TestTriggerQueueC(t *testing.T) {
	q := NewTriggerQueue(2)
	if !q.TryPush(Trigger{Price: 7}) {
		t.Fatal("push rejected on non-full queue")
	}
	select {
	case tr := <-q.C():
		if tr.Price != 7 {
			t.Fatalf("C() delivered %v, want 7", tr.Price)
		}
	case <-time.After(time.Second):
		t.Fatal("blocking receive on C() starved with a queued trigger")
	}
	select {
	case tr := <-q.C():
		t.Fatalf("C() delivered %v from an empty queue", tr.Price)
	default:
	}
}

func TestTriggerQueueCapacityClamp(t *testing.T) {
	q := NewTriggerQueue(0)
	if !q.TryPush(Trigger{}) {
		t.Fatal("push rejected on capacity-clamped queue")
	}
	if q.Len() != 1 {
		t.Fatalf("Len = %d, want 1", q.Len())
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
	if in.Len() != 2 {
		t.Fatalf("Len = %d, want 2", in.Len())
	}
}

// TestInternerConcurrentIntern pins idempotence under concurrency: the same
// symbol interned from many goroutines must map to one id, distinct symbols
// to distinct ids, and the id space must stay dense (0..n-1).
func TestInternerConcurrentIntern(t *testing.T) {
	in := NewInterner()
	const workers, syms = 8, 100
	ids := make([][]SymbolID, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ids[w] = make([]SymbolID, syms)
			for i := 0; i < syms; i++ {
				ids[w][i] = in.Intern(fmt.Sprintf("SYM%03d", i))
			}
		}(w)
	}
	wg.Wait()
	for s := 0; s < syms; s++ {
		for w := 1; w < workers; w++ {
			if ids[w][s] != ids[0][s] {
				t.Fatalf("symbol %d: worker %d got id %d, worker 0 got %d",
					s, w, ids[w][s], ids[0][s])
			}
		}
	}
	seen := make(map[SymbolID]bool)
	for s := 0; s < syms; s++ {
		if seen[ids[0][s]] {
			t.Fatalf("distinct symbols share id %d", ids[0][s])
		}
		seen[ids[0][s]] = true
	}
	if len(seen) != syms {
		t.Fatalf("id space not dense: %d distinct ids for %d symbols", len(seen), syms)
	}
}

// TestInternerNameUnknown pins the forgiving Name contract: an id that was
// never assigned returns "" instead of panicking.
func TestInternerNameUnknown(t *testing.T) {
	in := NewInterner()
	in.Intern("USDTRY")
	if got := in.Name(SymbolID(42)); got != "" {
		t.Fatalf("Name(unknown) = %q, want empty", got)
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
	s.trees[ti].Insert(entry{price: 425, id: AlertID{1}, meta: makeEntryMeta(1, 0, makeFlags(PriceBid, DirGTE))})
	cp := s.copy()
	cp.trees[ti].Insert(entry{price: 100, id: AlertID{2}, meta: makeEntryMeta(2, 0, makeFlags(PriceBid, DirGTE))})
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
	idx, gen := e.slots.alloc()
	ent := entry{price: 425, id: AlertID{1},
		validFrom: 1, meta: makeEntryMeta(idx, gen, makeFlags(PriceBid, DirGTE))}
	e.slots.setStatus(idx, StatusActive)
	// Publish the ref so the cleanup pass below has something real to clean.
	e.mu.Lock()
	e.refs[ent.id] = &alertRef{sid: sid, e: ent}
	e.live++
	e.mu.Unlock()

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
	// mutRemove cleanup: refs deleted, live decremented, slot retired.
	e.mu.Lock()
	_, refSurvived := e.refs[ent.id]
	live := e.live
	e.mu.Unlock()
	if refSurvived {
		t.Fatal("refs entry survived removal")
	}
	if live != 0 {
		t.Fatalf("live = %d after removal, want 0", live)
	}
	if w := e.slots.get(idx).Load(); w&slotRetiredBit == 0 {
		t.Fatal("slot did not acquire the retired bit on removal")
	}
}

// TestMutationQueueNeverDrops: with the unbounded mutQueue, removals
// submitted while the flusher is stalled accumulate instead of dropping,
// and all of them land after the stall.
func TestMutationQueueNeverDrops(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	sid, err := e.stateFor("USDTRY")
	if err != nil {
		t.Fatal(err)
	}
	const n = 8_000 // twice the old channel depth
	idxs := make([]uint32, 0, n+1)
	// Stall the flusher inside applyBatch's refs pass: one real mutRemove
	// wakes the flusher into the stall (a mutSync would panic on its nil
	// done channel), then submits must still return immediately (enqueue
	// never blocks) and survive the stall.
	e.mu.Lock()
	idx0, gen0 := e.slots.alloc()
	e.slots.setStatus(idx0, StatusActive)
	if err := e.submit(mutation{op: mutRemove, sid: sid,
		e: entry{price: 99, id: mkID(0),
			validFrom: 1, meta: makeEntryMeta(idx0, gen0, makeFlags(PriceLast, DirGTE))}}); err != nil {
		e.mu.Unlock()
		t.Fatal(err)
	}
	idxs = append(idxs, idx0)
	time.Sleep(10 * time.Millisecond) // let the flusher reach the held lock
	for i := 0; i < n; i++ {
		idx, gen := e.slots.alloc()
		e.slots.setStatus(idx, StatusActive)
		idxs = append(idxs, idx)
		ent := entry{price: Price(100 + i), id: mkID(uint32(i + 1)),
			validFrom: 1, meta: makeEntryMeta(idx, gen, makeFlags(PriceLast, DirGTE))}
		if err := e.submit(mutation{op: mutRemove, sid: sid, e: ent}); err != nil {
			e.mu.Unlock()
			t.Fatal(err)
		}
	}
	e.mu.Unlock()
	e.Sync()
	if left := e.mutQ.pending(); left != 0 {
		t.Fatalf("pending = %d after Sync, want 0 — removals lost", left)
	}
	retired := 0
	for _, idx := range idxs {
		if e.slots.get(idx).Load()&slotRetiredBit != 0 {
			retired++
		}
	}
	if retired != n+1 {
		t.Fatalf("%d/%d removals retired their slot", retired, n+1)
	}
}

// TestCloseDuringDrain pins the shutdown drain: Close racing a flusher that
// is still working through a deep mutation backlog must terminate reliably.
// The mutClose sentinel can land mid-batch (pulled in by drain, invisible to
// runFlusher's dequeue-side check), so applyBatch must be the one to see it —
// otherwise the flusher parks on the emptied queue and Close hangs on
// flushWG forever.
func TestCloseDuringDrain(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	sid, err := e.stateFor("USDTRY")
	if err != nil {
		t.Fatal(err)
	}
	const n = 20_000 // many FlushBatch-sized rounds pending at Close
	// Hold e.mu so the flusher stalls inside applyBatch's refs pass while the
	// backlog builds — Close then races a drain that is many batches behind.
	e.mu.Lock()
	for i := 0; i < n; i++ {
		idx, gen := e.slots.alloc()
		e.slots.setStatus(idx, StatusActive)
		ent := entry{price: Price(100 + i%97), id: mkID(uint32(i + 1)),
			validFrom: 1, meta: makeEntryMeta(idx, gen, makeFlags(PriceLast, DirGTE))}
		if err := e.submit(mutation{op: mutRemove, sid: sid, e: ent}); err != nil {
			e.mu.Unlock()
			t.Fatal(err)
		}
	}
	e.mu.Unlock()
	done := make(chan struct{})
	go func() {
		e.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung draining the mutation backlog — sentinel lost mid-batch")
	}
}

// TestSyncAcrossSymbols pins the Sync barrier over a wide symbol fan-out:
// upserts across many symbols must all be applied and published when Sync
// returns — trees populated, not just the control-plane Live counter
// (which Upsert bumps synchronously).
func TestSyncAcrossSymbols(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	const syms = 100
	for i := 0; i < syms; i++ {
		if err := e.Upsert(testSpec(byte(i+1), fmt.Sprintf("SYM%03d", i),
			PriceLast, DirGTE, 100)); err != nil {
			t.Fatal(err)
		}
	}
	e.Sync()
	if s := e.Stats(); s.Live != syms {
		t.Fatalf("Live = %d, want %d", s.Live, syms)
	}
	for i := 0; i < syms; i++ {
		sid, ok := e.syms.Get(fmt.Sprintf("SYM%03d", i))
		if !ok {
			t.Fatalf("symbol %d not interned", i)
		}
		if n := e.states[sid].snap.Load().trees[treeIndex(PriceLast, DirGTE)].Len(); n != 1 {
			t.Fatalf("symbol %d: tree Len = %d after Sync, want 1 — barrier missed a mutation", i, n)
		}
	}
}

// TestStressConcurrentMixedOps drives concurrent Upsert/Cancel/Match/Sync
// across symbols from several goroutines; after the final Sync every
// non-cancelled alert must have fired exactly once, no removal may have
// been shed, and live must be zero.
func TestStressConcurrentMixedOps(t *testing.T) {
	defer goleak.VerifyNone(t)
	const syms, workers, perSym, cancels = 16, 4, 40, 8
	e := New(DefaultConfig())
	defer e.Close()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for s := w; s < syms; s += workers {
				sym := fmt.Sprintf("STRESS%02d", s)
				for i := 0; i < perSym; i++ {
					a := AlertSpec{ID: mkID(uint32(s*perSym + i)), Symbol: sym,
						PriceType: PriceLast, Direction: DirGTE, TargetPrice: 100,
						ValidFrom: 1}
					if err := e.Upsert(a); err != nil {
						t.Error(err)
						return
					}
				}
				e.Sync()
				for i := 0; i < cancels; i++ {
					if err := e.Cancel(mkID(uint32(s*perSym + i))); err != nil {
						t.Error(err)
						return
					}
				}
				e.Sync()
				e.Match(&Tick{Symbol: sym, Last: 200,
					Present: 1 << uint(PriceLast), TS: 1 << 40})
				e.Sync()
			}
		}(w)
	}
	wg.Wait()
	e.Sync()
	counts := map[AlertID]int{}
	for _, tr := range drainTriggers(e) {
		counts[tr.ID]++
	}
	for s := 0; s < syms; s++ {
		for i := 0; i < perSym; i++ {
			id := mkID(uint32(s*perSym + i))
			want := 1
			if i < cancels {
				want = 0
			}
			if got := counts[id]; got != want {
				t.Fatalf("alert %d fired %d times, want %d", s*perSym+i, got, want)
			}
		}
	}
	st := e.Stats()
	if st.Live != 0 {
		t.Fatalf("Live = %d after final Sync, want 0", st.Live)
	}
	if st.DroppedTriggers != 0 {
		t.Fatalf("DroppedTriggers = %d, want 0", st.DroppedTriggers)
	}
}

func TestSyncBarrier(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	sid, _ := e.stateFor("EURTRY")
	for i := 0; i < 100; i++ {
		idx, gen := e.slots.alloc()
		e.slots.setStatus(idx, StatusActive)
		e.mu.Lock()
		e.refs[AlertID{byte(i + 1)}] = &alertRef{sid: sid, e: entry{
			price: Price(i), id: AlertID{byte(i + 1)},
			meta: makeEntryMeta(idx, gen, makeFlags(PriceLast, DirLTE))}}
		e.live++
		e.mu.Unlock()
		e.submit(mutation{op: mutInsert, sid: sid,
			e: entry{price: Price(i), id: AlertID{byte(i + 1)},
				validFrom: 1, meta: makeEntryMeta(idx, gen, makeFlags(PriceLast, DirLTE))}})
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
		TargetPrice: price, ValidFrom: 1,
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

// TestUpsertBadDimsDoesNotIntern pins validation ordering: a rejected upsert
// must not intern its symbol (the Interner never evicts).
func TestUpsertBadDimsDoesNotIntern(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.DimCount = 1
	e := New(cfg)
	defer e.Close()
	a := testSpec(1, "GARBAGE", PriceBid, DirGTE, 425)
	a.Dims = Dims() // all sentinels: invalid at width 1
	if err := e.Upsert(a); err != ErrDims {
		t.Fatalf("err=%v, want ErrDims", err)
	}
	if _, ok := e.syms.Get("GARBAGE"); ok {
		t.Fatal("rejected upsert interned its symbol")
	}
	if s := e.Stats(); s.Symbols != 0 {
		t.Fatalf("Symbols=%d after rejected upsert, want 0", s.Symbols)
	}
	b := testSpec(2, "USDTRY", PriceBid, DirGTE, 425)
	b.Dims = Dims(7)
	if err := e.Upsert(b); err != nil {
		t.Fatal(err)
	}
	if s := e.Stats(); s.Symbols != 1 {
		t.Fatalf("Symbols=%d after valid upsert, want 1", s.Symbols)
	}
}

// TestStatusGetter pins the read-only status lookup: Active/Paused are
// observable without mutating; a removed alert reports false.
func TestStatusGetter(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	if st, ok := e.Status(AlertID{9}); ok || st != StatusZero {
		t.Fatalf("Status(unknown) = (%v, %v), want (StatusZero, false)", st, ok)
	}
	e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425))
	// The ledger is updated synchronously: visible before the flusher runs.
	if st, ok := e.Status(AlertID{1}); !ok || st != StatusActive {
		t.Fatalf("pre-Sync Status = (%v, %v), want (StatusActive, true)", st, ok)
	}
	e.Sync()
	if st, ok := e.Status(AlertID{1}); !ok || st != StatusActive {
		t.Fatalf("Status = (%v, %v), want (StatusActive, true)", st, ok)
	}
	if err := e.SetStatus(AlertID{1}, StatusPaused); err != nil {
		t.Fatal(err)
	}
	if st, ok := e.Status(AlertID{1}); !ok || st != StatusPaused {
		t.Fatalf("Status = (%v, %v), want (StatusPaused, true)", st, ok)
	}
	// Un-pause, fire: after the removal lands the alert is gone from the ledger.
	if err := e.SetStatus(AlertID{1}, StatusActive); err != nil {
		t.Fatal(err)
	}
	// The slot word stays TRIGGERED from the fire CAS until re-handout, so
	// the flusher's timing cannot race the assertion (a ledger read could).
	e.mu.Lock()
	firedIdx := entryIdx(e.refs[AlertID{1}].e)
	e.mu.Unlock()
	e.Match(&Tick{Symbol: "USDTRY", Bid: 430, Present: TickAllPresent(), TS: 100})
	drainTriggers(e)
	if s := e.slots.status(firedIdx); s != StatusTriggered {
		t.Fatalf("fired slot status = %v, want StatusTriggered", s)
	}
	e.Sync()
	if _, ok := e.Status(AlertID{1}); ok {
		t.Fatal("fired alert still present after removal applied")
	}
	// Cancel: same ledger semantics.
	e.Upsert(testSpec(2, "USDTRY", PriceBid, DirGTE, 425))
	e.Sync()
	if err := e.Cancel(AlertID{2}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if _, ok := e.Status(AlertID{2}); ok {
		t.Fatal("cancelled alert still present after removal applied")
	}
}

// TestStatusAndStatsConcurrent pins read-side safety: Status and Stats under
// the full mutation mix — meaningful under -race.
func TestStatusAndStatsConcurrent(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	const syms, perSym, readers = 8, 50, 4
	stop := make(chan struct{})
	var rg sync.WaitGroup
	for r := 0; r < readers; r++ {
		rg.Add(1)
		go func() {
			defer rg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				e.Status(mkID(1))
				_ = e.Stats()
			}
		}()
	}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for s := w; s < syms; s += 4 {
				sym := fmt.Sprintf("CS%02d", s)
				for i := 0; i < perSym; i++ {
					id := mkID(uint32(s*perSym + i + 1))
					if err := e.Upsert(AlertSpec{ID: id, Symbol: sym, PriceType: PriceLast,
						Direction: DirGTE, TargetPrice: 100, ValidFrom: 1}); err != nil {
						t.Error(err)
						return
					}
					if i%3 == 0 {
						if err := e.SetStatus(id, StatusPaused); err != nil {
							t.Error(err)
							return
						}
					}
					if i%5 == 0 {
						if err := e.Cancel(id); err != nil {
							t.Error(err)
							return
						}
					}
				}
				e.Sync()
				e.Match(&Tick{Symbol: sym, Last: 200, Present: 1 << uint(PriceLast), TS: 1 << 40})
				e.Sync()
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	rg.Wait()
	e.Sync()
	drainTriggers(e)
	// The pinned contract: reads stayed safe and the queue drained.
	if s := e.Stats(); s.MutQDepth != 0 {
		t.Fatalf("MutQDepth=%d after final Sync, want 0", s.MutQDepth)
	}
}

// TestStatsFields pins the observability gauges: Symbols counts interned
// symbols, MutQDepth drains to zero at Sync, and ExpiryLen tracks the
// reaper's registration table.
func TestStatsFields(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = time.Hour // keep registrations in the table
	e := New(cfg)
	defer e.Close()
	exp := time.Now().Add(time.Hour).UnixNano()
	for i := 0; i < 3; i++ {
		a := AlertSpec{ID: mkID(uint32(i + 1)), Symbol: fmt.Sprintf("S%d", i),
			PriceType: PriceBid, Direction: DirGTE, TargetPrice: 100,
			ValidFrom: 1, Expires: exp}
		if err := e.Upsert(a); err != nil {
			t.Fatal(err)
		}
	}
	e.Sync()
	s := e.Stats()
	if s.Symbols != 3 {
		t.Fatalf("Symbols=%d, want 3", s.Symbols)
	}
	if s.MutQDepth != 0 {
		t.Fatalf("MutQDepth=%d after Sync, want 0", s.MutQDepth)
	}
	waitForExpLen(t, e, 3)
	if s := e.Stats(); s.ExpiryLen != 3 {
		t.Fatalf("ExpiryLen=%d, want 3", s.ExpiryLen)
	}
}

// TestUpsertAlertLimitDoesNotIntern pins the alert-limit gate for new ids: a
// rejected upsert must not intern its symbol; a replace at full capacity
// still passes.
func TestUpsertAlertLimitDoesNotIntern(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.MaxAlerts = 1
	e := New(cfg)
	defer e.Close()
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425)); err != nil {
		t.Fatal(err)
	}
	if err := e.Upsert(testSpec(2, "GARBAGE", PriceBid, DirGTE, 1)); err != ErrAlertLimit {
		t.Fatalf("err=%v, want ErrAlertLimit", err)
	}
	if _, ok := e.syms.Get("GARBAGE"); ok {
		t.Fatal("alert-limit-rejected upsert interned its symbol")
	}
	if s := e.Stats(); s.Symbols != 1 {
		t.Fatalf("Symbols=%d after rejected upsert, want 1", s.Symbols)
	}
	// Replace of the live alert passes the gate even at full capacity.
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 430)); err != nil {
		t.Fatalf("replace at full capacity rejected: %v", err)
	}
	if s := e.Stats(); s.Symbols != 1 || s.Live != 1 {
		t.Fatalf("after replace Stats=%+v, want Symbols=1 Live=1", s)
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
	// Free one alert so the next insert passes the alert gate (ErrAlertLimit
	// takes precedence at both limits) and reaches the symbol limit.
	if err := e.Cancel(AlertID{10}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if err := e.Upsert(testSpec(20, "S3", PriceBid, DirGTE, 1)); err != ErrSymbolLimit {
		t.Fatalf("err=%v, want ErrSymbolLimit", err)
	}
}

// TestUpsertSameIDConcurrent pins the single-handout contract: concurrent
// upserts of one new ID serialize — the later one sees the earlier one's
// published ref and replaces it — so an ID never gains two live,
// independently firable tree entries.
func TestUpsertSameIDConcurrent(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	const n = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, Price(400+i))); err != nil {
				t.Error(err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	e.Sync()
	if s := e.Stats(); s.Live != 1 {
		t.Fatalf("Live=%d, want 1", s.Live)
	}
	sid, _ := e.syms.Get("USDTRY")
	if got := e.states[sid].snap.Load().trees[treeIndex(PriceBid, DirGTE)].Len(); got != 1 {
		t.Fatalf("tree Len=%d, want 1 (one live handout per ID)", got)
	}
}

// TestUpsertLimitExact pins the limit's atomicity: when concurrent upserts of
// distinct IDs race for the last slots, exactly MaxAlerts succeed — the
// check and the publish cannot interleave to overshoot the cap.
func TestUpsertLimitExact(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.MaxAlerts = 8
	e := New(cfg)
	defer e.Close()
	const n = 32
	var okCount atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if err := e.Upsert(testSpec(byte(i), "USDTRY", PriceBid, DirGTE, 425)); err == nil {
				okCount.Add(1)
			} else if err != ErrAlertLimit {
				t.Errorf("upsert %d: err=%v, want nil or ErrAlertLimit", i, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	e.Sync()
	if got := okCount.Load(); got != int64(cfg.MaxAlerts) {
		t.Fatalf("accepted upserts=%d, want exactly %d", got, cfg.MaxAlerts)
	}
	if s := e.Stats(); s.Live != cfg.MaxAlerts {
		t.Fatalf("Live=%d, want %d", s.Live, cfg.MaxAlerts)
	}
}

// TestUpsertCancelRaceNoZombie pins the enqueue-before-publish order: a
// Cancel that observes a ref a same-ID upsert just published enqueues its
// removal after that upsert's insert, so the tree can never hold an entry
// whose slot is already retired — a zombie, invisible to the ledger and
// fatal to a recycled slot's next occupant. Oracle per round: tree count
// equals ledger count, and the survivors fire exactly once.
func TestUpsertCancelRaceNoZombie(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	const rounds = 100
	for r := 1; r <= rounds; r++ {
		if err := e.Upsert(testSpec(byte(r), "USDTRY", PriceBid, DirGTE, 425)); err != nil {
			t.Fatal(err)
		}
		e.Sync()
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func(r int) {
			defer wg.Done()
			<-start
			if err := e.Upsert(testSpec(byte(r), "USDTRY", PriceBid, DirGTE, 430)); err != nil {
				t.Error(err)
			}
		}(r)
		go func(id AlertID) {
			defer wg.Done()
			<-start
			_ = e.Cancel(id) // either order is legal; ErrNotFound/InvalidTransition ok
		}(AlertID{byte(r)})
		close(start)
		wg.Wait()
		e.Sync()
		sid, _ := e.syms.Get("USDTRY")
		n := e.states[sid].snap.Load().trees[treeIndex(PriceBid, DirGTE)].Len()
		if live := e.Stats().Live; uint64(n) != live {
			t.Fatalf("round %d: tree Len=%d, Live=%d — zombie entry or lost alert", r, n, live)
		}
	}
	live := e.Stats().Live
	e.Match(&Tick{Symbol: "USDTRY", Bid: 431, Present: 1 << uint(PriceBid), TS: 1 << 40})
	e.Sync()
	if got := len(drainTriggers(e)); uint64(got) != live {
		t.Fatalf("triggers=%d, want %d (one per live alert, none for zombies)", got, live)
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
		Direction: DirGTE, TargetPrice: 425, ValidFrom: 200})
	// Expired (lazy inline check; reaper runs on 1s cadence, don't wait).
	e.Upsert(AlertSpec{ID: AlertID{3}, Symbol: "USDTRY", PriceType: PriceBid,
		Direction: DirGTE, TargetPrice: 425, ValidFrom: 1, Expires: 50})
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
	})
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
	// The registration must leave the reaper's table too.
	e.Sync()
	for e.Stats().ExpiryLen != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("ExpiryLen=%d after expiry, want 0", e.Stats().ExpiryLen)
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
	})
	e.mu.Lock()
	aIdx := entryIdx(e.refs[AlertID{1}].e)
	e.mu.Unlock()
	if err := e.Cancel(AlertID{1}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	// Wait past the recycle grace so A's slot is back on the free list.
	time.Sleep(80 * time.Millisecond)
	// B: never-expiring alert; must recycle A's slot.
	if err := e.Upsert(AlertSpec{ID: AlertID{2}, Symbol: "EURTRY", PriceType: PriceAsk,
		Direction: DirLTE, TargetPrice: 500, ValidFrom: 1}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	e.mu.Lock()
	bIdx := entryIdx(e.refs[AlertID{2}].e)
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
	idx, gen := e.slots.alloc()
	e.slots.setStatus(idx, StatusActive)
	ent := entry{price: 425, id: AlertID{1},
		validFrom: 1, meta: makeEntryMeta(idx, gen, makeFlags(PriceBid, DirGTE))}
	e.submit(mutation{op: mutInsert, sid: sid, e: ent})
	e.Sync()
	// Two direct mutRemove submissions for the same entry (gen-identical):
	// a replacement racing a fire's removal.
	e.submit(mutation{op: mutRemove, sid: sid, e: ent})
	e.submit(mutation{op: mutRemove, sid: sid, e: ent})
	e.Sync()
	e.slots.recycle(time.Now().Add(time.Hour))
	hits := 0
	var prev uint32
	for i := 0; i < 2; i++ {
		a, _ := e.slots.alloc()
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
	oldIdx := entryIdx(e2.refs[AlertID{1}].e)
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
	a1, _ := e2.slots.alloc()
	a2, _ := e2.slots.alloc()
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

// TestSameKeyUpsertRacingFireRemoval pins the delayed-removal aliasing bug:
// fire CASes the slot ACTIVE→TRIGGERED but defers its mutRemove; a same-key
// Upsert (only Expires differs) landing in that window used to be deleted by
// the delayed removal, leaving the alert live in the ledger but invisible to
// Match. The comparator's idx tie-break keeps handouts distinct, so the
// removal deletes only its own record and the replacement's insert lands.
func TestSameKeyUpsertRacingFireRemoval(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()

	// Alert live and published.
	spec := testSpec(1, "USDTRY", PriceBid, DirGTE, 425)
	if err := e.Upsert(spec); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	e.mu.Lock()
	ref := e.refs[AlertID{1}]
	e.mu.Unlock()
	sid := ref.sid
	oldEnt := ref.e

	// Fire's first half: CAS ACTIVE→TRIGGERED (exactly what fire does), then
	// "pause" before enqueuing the deferred removal. This reproduces the
	// queue order the real preemption window yields: replacement insert first,
	// delayed old-handout removal second.
	if !e.slots.cas(entryIdx(oldEnt), StatusActive, StatusTriggered) {
		t.Fatal("setup: CAS ACTIVE→TRIGGERED failed")
	}

	// Same-key upsert in the pause window. Slot is terminal, so Upsert queues
	// no replace-removal — only the insert of the new handout.
	spec.Expires = 1 << 40
	if err := e.Upsert(spec); err != nil {
		t.Fatal(err)
	}
	e.Sync()

	// The delayed fire removal finally lands — after the replacement's insert.
	e.submit(mutation{op: mutRemove, sid: sid, e: oldEnt})
	e.Sync()

	// The replacement is live and must be indexed exactly once.
	if s := e.Stats(); s.Live != 1 {
		t.Fatalf("Live=%d, want 1", s.Live)
	}
	ti := treeIndex(PriceBid, DirGTE)
	if n := e.states[sid].snap.Load().trees[ti].Len(); n != 1 {
		t.Fatalf("tree Len=%d, want 1 (replacement must be indexed)", n)
	}
	// And it must fire on a crossing tick.
	e.Match(&Tick{Symbol: "USDTRY", Bid: 430, Present: 1 << uint(PriceBid), TS: 2})
	for _, tr := range drainTriggers(e) {
		if tr.ID == (AlertID{1}) {
			return
		}
	}
	t.Fatal("replacement alert silently lost: live in ledger, invisible to Match")
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

// TestNeverExpiringNotRegistered pins the registration contract: alerts
// with Expires == 0 never enter the expiry table — the table holds only
// real deadlines. (The old variant asserted the never-due sentinel; the
// sentinel existed only to feed the integrity sweep's live-alert registry,
// which is gone.)
func TestNeverExpiringNotRegistered(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close() // idempotent with the mid-test Close; covers Upsert-failure paths
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425)); err != nil {
		t.Fatal(err)
	}
	exp := time.Now().Add(time.Hour).UnixNano()
	a := AlertSpec{ID: AlertID{2}, Symbol: "USDTRY", PriceType: PriceBid,
		Direction: DirGTE, TargetPrice: 430, ValidFrom: 1, Expires: exp}
	if err := e.Upsert(a); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	deadline := time.Now().Add(5 * time.Second)
	for e.expQ.pending() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("registrations not drained from expQ within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.Close() // quiesces the table; safe to inspect
	n := 0
	for x := range e.expiry.All() {
		if x.ref.e.id == (AlertID{1}) {
			t.Fatal("never-expiring alert registered in the expiry table")
		}
		if x.ref.e.id == (AlertID{2}) {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("expiring alert has %d registrations, want 1", n)
	}
}

// TestSnapshotPerTreeCOW pins per-tree copy-on-write: a mutation touching one
// tree must republish a snapshot whose untouched trees remain scannable.
// Behavioral pin only — the safety of any storage sharing scheme is decided
// by btype's Copy/Release semantics, not by this test.
func TestSnapshotPerTreeCOW(t *testing.T) {
	e := New(DefaultConfig())
	defer e.Close()
	for i := 0; i < 8; i++ {
		a := AlertSpec{ID: mkID(uint32(i)), Symbol: "S", PriceType: PriceLast,
			Direction: DirGTE, TargetPrice: 100 + Price(i), ValidFrom: 1}
		if err := e.Upsert(a); err != nil {
			t.Fatal(err)
		}
	}
	e.Sync()
	sid, ok := e.syms.Get("S")
	if !ok {
		t.Fatal("symbol not interned")
	}
	old := e.states[sid].snap.Load()

	// Mutate only the (PriceLast, DirGTE) tree: cancel one alert.
	if err := e.Cancel(mkID(3)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	next := e.states[sid].snap.Load()
	if next == old {
		t.Fatal("snapshot not republished after cancel")
	}

	// The republished snapshot must leave the untouched (PriceBid, DirGTE)
	// tree scannable; this tick's Present mask carries only PriceLast, so
	// Match never evaluates PriceBid — the pin covers republish plus the
	// fired-path on the touched tree.
	tick := Tick{Symbol: "S", Last: 150, Present: 1 << uint(PriceLast), TS: 2}
	e.Match(&tick)
	n := 0
	for {
		if _, ok := e.Triggers().Pop(); !ok {
			break
		}
		n++
	}
	// 8 GTE targets all at/under 150; exactly the cancelled one is gone.
	if want := 8 - 1; n != want {
		t.Fatalf("fired %d, want %d", n, want)
	}
}

// TestSweepSkipsStaleEntryOnRecycledSlot pins the ABA guard without gen:
// A (short TTL) is cancelled, its slot recycled and taken by B (long TTL).
// A's lingering registration comes due while B occupies the slot; the sweep
// must validate liveness by ref identity and never touch B. Safe-by-order:
// recycle runs in the same reaper goroutine, after sweep.
func TestSweepSkipsStaleEntryOnRecycledSlot(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = 20 * time.Millisecond
	e := New(cfg)
	defer e.Close()
	a := AlertSpec{ID: AlertID{1}, Symbol: "ABA", PriceType: PriceBid,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1,
		Expires: time.Now().Add(300 * time.Millisecond).UnixNano()}
	if err := e.Upsert(a); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	e.mu.Lock()
	aIdx := entryIdx(e.refs[AlertID{1}].e)
	e.mu.Unlock()
	if err := e.Cancel(AlertID{1}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
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
		time.Sleep(5 * time.Millisecond)
	}
	b := AlertSpec{ID: AlertID{2}, Symbol: "ABA", PriceType: PriceBid,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1,
		Expires: time.Now().Add(time.Hour).UnixNano()}
	if err := e.Upsert(b); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	e.mu.Lock()
	bIdx := entryIdx(e.refs[AlertID{2}].e)
	e.mu.Unlock()
	if bIdx != aIdx {
		t.Fatalf("setup failed: B got slot %d, wanted recycled %d", bIdx, aIdx)
	}
	// A's deadline (300ms) passes; several sweeps run against B's slot.
	time.Sleep(600 * time.Millisecond)
	if st := e.slots.status(bIdx); st != StatusActive {
		t.Fatalf("B status = %v, want Active — stale entry killed the recycled slot's occupant", st)
	}
	e.Close() // quiesce; the table is safe to inspect
	for x := range e.expiry.All() {
		if x.ref.e.id == (AlertID{1}) {
			t.Fatal("stale registration lingered past its deadline")
		}
	}
}

// TestSweepSkipsReplacedHandout pins the same guard against same-ID
// replacement: the old handout's registration comes due after the map
// already points at the new ref; the replacement must survive to its own
// deadline.
func TestSweepSkipsReplacedHandout(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = 20 * time.Millisecond
	e := New(cfg)
	defer e.Close()
	old := AlertSpec{ID: AlertID{7}, Symbol: "REP", PriceType: PriceLast,
		Direction: DirGTE, TargetPrice: 100, ValidFrom: 1,
		Expires: time.Now().Add(200 * time.Millisecond).UnixNano()}
	if err := e.Upsert(old); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	repl := old
	repl.TargetPrice = 150
	repl.Expires = time.Now().Add(time.Hour).UnixNano()
	if err := e.Upsert(repl); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	time.Sleep(500 * time.Millisecond) // old deadline passes
	e.mu.Lock()
	ref, ok := e.refs[AlertID{7}]
	e.mu.Unlock()
	if !ok {
		t.Fatal("replacement removed from refs")
	}
	if st := e.slots.status(entryIdx(ref.e)); st != StatusActive {
		t.Fatalf("replacement status = %v, want Active — old handout's registration expired it", st)
	}
	if ref.e.price != 150 {
		t.Fatalf("refs points at price %d, want the replacement's 150", ref.e.price)
	}
	e.Close()
	regs := 0
	for x := range e.expiry.All() {
		if x.ref.e.id == (AlertID{7}) {
			regs++
			if x.ref != ref {
				t.Fatal("old handout's registration lingered past its deadline")
			}
		}
	}
	if regs != 1 {
		t.Fatalf("replacement has %d registrations, want 1", regs)
	}
}

// TestExpiryRegistryPointerTiebreak pins comparator uniqueness without gen:
// two registrations at an identical absolute deadline must coexist — btype
// Insert is a no-op on equal keys, so a field-based tie-break would silently
// drop the second registration. (Since the flusher's removal pass eagerly
// dergs fired/cancelled/replaced handouts, a stale entry can no longer
// collide with a recycled slot's fresh registration, so the collision is
// constructed with two live registrations instead.)
func TestExpiryRegistryPointerTiebreak(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = time.Hour // no sweep interference
	e := New(cfg)
	defer e.Close()
	exp := time.Now().Add(time.Hour).UnixNano()
	for _, id := range []AlertID{{1}, {2}} {
		a := AlertSpec{ID: id, Symbol: "PTB", PriceType: PriceBid,
			Direction: DirGTE, TargetPrice: 100, ValidFrom: 1, Expires: exp}
		if err := e.Upsert(a); err != nil {
			t.Fatal(err)
		}
	}
	waitForExpLen(t, e, 2)
	e.Close() // quiesce; both registrations must be present and distinct
	n := 0
	seen := map[*alertRef]bool{}
	for x := range e.expiry.All() {
		n++
		if x.expires != exp {
			t.Fatalf("registration expires=%d, want %d", x.expires, exp)
		}
		if seen[x.ref] {
			t.Fatal("duplicate registration for the same ref")
		}
		seen[x.ref] = true
	}
	if n != 2 {
		t.Fatalf("table holds %d registrations, want 2 (collision dropped one)", n)
	}
}

// TestNewRejectsNonPositiveReaperInterval pins the fail-loudly contract: a
// bad ReaperInterval must panic in New, synchronously, before any goroutine
// starts — not crash the process later from the reaper's ticker.
func TestNewRejectsNonPositiveReaperInterval(t *testing.T) {
	defer goleak.VerifyNone(t)
	for _, d := range []time.Duration{0, -time.Second} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("New(ReaperInterval=%v) did not panic", d)
				}
			}()
			cfg := DefaultConfig()
			cfg.ReaperInterval = d
			New(cfg)
		}()
	}
}

// TestFireDoesNotAdoptRecycledSlotOccupant pins the fire-side ABA guard: a
// deferred fire holding an entry from a pre-cancel snapshot must never adopt
// the slot's next occupant. Before the generation gate in fire's CAS loop,
// fire checked only the status byte, so a scan preempted past the
// 2×-interval recycle grace could CAS the new occupant ACTIVE→TRIGGERED: a
// ghost trigger for the dead alert and a bricked replacement. The sweep
// path's equivalent ABA is pinned by TestStaleExpiryDoesNotKillReusedSlot.
func TestFireDoesNotAdoptRecycledSlotOccupant(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = 5 * time.Millisecond
	e := New(cfg)
	defer e.Close()
	if err := e.Upsert(testSpec(1, "S", PriceBid, DirGTE, 100)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	e.mu.Lock()
	oldRef := e.refs[AlertID{1}]
	e.mu.Unlock()
	oldEnt := oldRef.e
	sid := oldRef.sid
	aIdx := entryIdx(oldEnt)
	gen1 := e.slots.gen(aIdx)
	if err := e.Cancel(AlertID{1}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	// Poll the free list until the reaper's recycle puts aIdx back in use.
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
		time.Sleep(5 * time.Millisecond)
	}
	if err := e.Upsert(testSpec(2, "S", PriceBid, DirGTE, 9999)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	e.mu.Lock()
	bIdx := entryIdx(e.refs[AlertID{2}].e)
	e.mu.Unlock()
	if bIdx != aIdx {
		t.Fatalf("setup failed: B got slot %d, wanted recycled %d", bIdx, aIdx)
	}
	if gen2 := e.slots.gen(bIdx); gen2 <= gen1 {
		t.Fatalf("setup failed: re-handout gen %d not bumped past %d", gen2, gen1)
	}
	if st := e.slots.status(bIdx); st != StatusActive {
		t.Fatalf("B's slot status = %v, want StatusActive", st)
	}
	// The deferred-fire interleaving: exactly what a Match scan pinned on the
	// pre-cancel snapshot does if preempted past the recycle grace.
	e.fire(sid, &oldEnt, 100, 1<<40)
	// B's slot must be untouched by the stale fire.
	if st := e.slots.status(bIdx); st != StatusActive {
		t.Fatalf("stale fire adopted the recycled slot: B's status = %v, want StatusActive", st)
	}
	for _, tr := range drainTriggers(e) {
		if tr.ID == (AlertID{1}) {
			t.Fatalf("ghost trigger for cancelled alert 1: %+v", tr)
		}
	}
	// B must still fire on a later crossing tick.
	e.Match(&Tick{Symbol: "S", Bid: 10000, Present: 1 << uint(PriceBid), TS: 1 << 41})
	fired := false
	for _, tr := range drainTriggers(e) {
		if tr.ID == (AlertID{2}) {
			fired = true
		}
	}
	if !fired {
		t.Fatal("B failed to fire after the stale fire interleaving — slot bricked")
	}
	if s := e.Stats(); s.Live != 1 {
		t.Fatalf("Live = %d, want 1", s.Live)
	}
}

// TestMatchGTEBoundaryAllOnesID pins the GTE boundary for the all-ones id,
// the one id that ties the Descend probe's id. Before the probe carried the
// entryMetaMaxIdx idx sentinel, the identity tie-break ordered an all-ones-id
// entry at idx>0 after the probe, so the exact-boundary tick silently missed.
// LTE is unaffected: the Ascend probe's zero id sorts before every accepted
// id.
func TestMatchGTEBoundaryAllOnesID(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	// Consume slot idx 0 first: the probe's idx is 0, and the comparator's
	// identity tie-break only orders the entry after the probe when the
	// entry's idx > 0 — with idx 0 the compare hits full equality and the
	// boundary works by accident.
	if err := e.Upsert(testSpec(9, "WARM", PriceBid, DirGTE, 1)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if err := e.Upsert(AlertSpec{ID: maxAlertID, Symbol: "S", PriceType: PriceBid,
		Direction: DirGTE, TargetPrice: 425, ValidFrom: 1}); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	e.mu.Lock()
	onesIdx := entryIdx(e.refs[maxAlertID].e)
	e.mu.Unlock()
	if onesIdx == 0 {
		t.Fatal("setup failed: all-ones alert landed on slot 0; warm-up alert did not consume it")
	}
	e.Match(&Tick{Symbol: "S", Bid: 425, Present: TickAllPresent(), TS: 100})
	got := drainTriggers(e)
	if len(got) != 1 || got[0].ID != maxAlertID {
		t.Fatalf("all-ones-ID alert at the boundary: triggers=%+v, want exactly one fire", got)
	}
	// Control: an ordinary id fires at exactly the target under the same setup.
	if err := e.Upsert(testSpec(1, "CTRL", PriceBid, DirGTE, 425)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	e.Match(&Tick{Symbol: "CTRL", Bid: 425, Present: TickAllPresent(), TS: 100})
	got = drainTriggers(e)
	if len(got) != 1 || got[0].ID != (AlertID{1}) {
		t.Fatalf("control alert at the boundary: triggers=%+v, want exactly one fire", got)
	}
}

// TestMatchRacingClose runs Match's actual load→pin→scan→unpin guard sequence
// (including firing and mutRemove enqueues) against a concurrent Close.
// TestCloseWaitsForReaders pins the pin/unpin drain with manual calls; this
// pins the real hot path: no panic (use-after-release of tree memory, nil
// deref in the snap retry loop) and Close returning promptly.
func TestMatchRacingClose(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	const n = 1000
	for i := 0; i < n; i++ {
		a := testSpec(1, "S", PriceType(i%4), Direction(i%2), Price(100+i%50))
		a.ID = mkID(uint32(i + 1))
		if err := e.Upsert(a); err != nil {
			t.Fatal(err)
		}
	}
	e.Sync()
	stop := make(chan struct{})
	panicMsg := make(chan string, 4)
	var wg sync.WaitGroup
	tick := Tick{Symbol: "S", Bid: 1e9, Ask: 1e9, Mid: 1e9, Last: 1e9,
		Present: TickAllPresent(), TS: 1 << 40}
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					select {
					case panicMsg <- fmt.Sprint(r):
					default:
					}
				}
			}()
			for {
				select {
				case <-stop:
					return
				default:
				}
				e.Match(&tick)
			}
		}()
	}
	closed := make(chan struct{})
	go func() {
		e.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		close(stop)
		wg.Wait()
		t.Fatal("Close did not return within 5s while Match goroutines spun")
	}
	close(stop)
	wg.Wait()
	select {
	case msg := <-panicMsg:
		t.Fatalf("Match panicked racing Close: %s", msg)
	default:
	}
}

// TestUpsertReplaceAcrossSymbols pins the replace path when the new spec
// lands on a different symbol: the mutRemove is keyed to the OLD sid while
// the insert uses the NEW one. All existing replace tests keep the symbol.
func TestUpsertReplaceAcrossSymbols(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	if err := e.Upsert(testSpec(1, "AAA", PriceBid, DirGTE, 100)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if err := e.Upsert(testSpec(1, "BBB", PriceBid, DirGTE, 430)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	aaaSid, _ := e.syms.Get("AAA")
	bbbSid, _ := e.syms.Get("BBB")
	ti := treeIndex(PriceBid, DirGTE)
	if got := e.states[aaaSid].snap.Load().trees[ti].Len(); got != 0 {
		t.Fatalf("AAA tree Len=%d after cross-symbol replace, want 0", got)
	}
	bbb := e.states[bbbSid].snap.Load().trees[ti]
	if got := bbb.Len(); got != 1 {
		t.Fatalf("BBB tree Len=%d, want 1", got)
	}
	for en := range bbb.All() {
		if en.price != 430 {
			t.Fatalf("BBB entry price=%d, want 430", en.price)
		}
	}
	if s := e.Stats(); s.Live != 1 {
		t.Fatalf("Live=%d, want 1", s.Live)
	}
	e.Match(&Tick{Symbol: "AAA", Bid: 1e9, Present: 1 << uint(PriceBid), TS: 100})
	if got := drainTriggers(e); len(got) != 0 {
		t.Fatalf("AAA tick fired %+v, want nothing", got)
	}
	e.Match(&Tick{Symbol: "BBB", Bid: 1e9, Present: 1 << uint(PriceBid), TS: 100})
	got := drainTriggers(e)
	if len(got) != 1 || got[0].ID != (AlertID{1}) {
		t.Fatalf("BBB tick triggers=%+v, want exactly one fire of alert 1", got)
	}
}

// TestFlusherParksRetiredSnapshotUntilReaderDrains pins the retireRelease
// false branch: a snapshot retired while a reader still holds a pin must be
// parked (not released), stay scannable, and leave e.parked on the next
// applyBatch cycle after the reader unpins. TestCloseWaitsForReaders covers
// only shutdownRelease; this path is asserted by zero other tests.
func TestFlusherParksRetiredSnapshotUntilReaderDrains(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	sid, _ := e.syms.Get("USDTRY")
	old := e.states[sid].snap.Load()
	if !old.pin() {
		t.Fatal("pin failed on a live snapshot")
	}
	if err := e.Cancel(AlertID{1}); err != nil {
		t.Fatal(err)
	}
	if err := e.Upsert(testSpec(2, "USDTRY", PriceBid, DirGTE, 430)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if !slices.Contains(e.parked, old) {
		t.Fatal("retired snapshot with a pinned reader was not parked")
	}
	// Parked must stay scannable: the pre-cancel tree is intact.
	if got := old.trees[treeIndex(PriceBid, DirGTE)].Len(); got != 1 {
		t.Fatalf("parked snapshot tree Len=%d, want 1 (pre-cancel count)", got)
	}
	old.unpin()
	// Any later batch re-runs the parked-release loop.
	if err := e.Upsert(testSpec(3, "USDTRY", PriceBid, DirGTE, 435)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if slices.Contains(e.parked, old) {
		t.Fatal("parked snapshot lingered after its reader drained")
	}
}

// TestCloseWaitsForParkedReaders pins Close's parked-snapshot drain
// loop: a snapshot that retired into e.parked while pinned must also block
// Close (its trees can only be freed once the reader drains). The states[]
// loop alone never exercises the `for _, s := range e.parked` branch.
func TestCloseWaitsForParkedReaders(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	sid, _ := e.syms.Get("USDTRY")
	s0 := e.states[sid].snap.Load()
	if !s0.pin() {
		t.Fatal("pin failed on S0")
	}
	if err := e.Cancel(AlertID{1}); err != nil {
		t.Fatal(err)
	}
	e.Sync() // publishes S1; S0 retires into e.parked (readers==1)
	s1 := e.states[sid].snap.Load()
	if !s1.pin() {
		t.Fatal("pin failed on S1")
	}
	closed := make(chan struct{})
	go func() {
		e.Close()
		close(closed)
	}()
	select {
	case <-closed:
		s0.unpin()
		s1.unpin()
		t.Fatal("Close returned while S0/S1 were pinned")
	case <-time.After(50 * time.Millisecond):
	}
	s0.unpin()
	s1.unpin()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not complete after both readers drained")
	}
}

// TestSyncBarrierHonoredDuringClose pins the syncs-close pass inside the
// final applyBatch: a mutSync enqueued before the mutClose sentinel must be
// acknowledged even when the sentinel shares its batch. TestCloseDuringDrain's
// backlog has no mutSync; TestSyncAfterCloseReturns covers only the post-Close
// fast path. If the final batch ever skipped the syncs pass on stop, Sync
// callers would deadlock with no failing test.
func TestSyncBarrierHonoredDuringClose(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	sid, err := e.stateFor("USDTRY")
	if err != nil {
		t.Fatal(err)
	}
	// Stall the flusher inside applyBatch's refs pass (TestMutationQueueNeverDrops
	// technique): one real mutRemove wakes it into the stall.
	e.mu.Lock()
	idx, gen := e.slots.alloc()
	e.slots.setStatus(idx, StatusActive)
	if err := e.submit(mutation{op: mutRemove, sid: sid,
		e: entry{price: 99, id: mkID(0),
			validFrom: 1, meta: makeEntryMeta(idx, gen, makeFlags(PriceLast, DirGTE))}}); err != nil {
		e.mu.Unlock()
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond) // flusher reaches the held lock
	syncDone := make(chan struct{})
	go func() {
		e.Sync() // mutSync lands after the mutRemove, before the sentinel
		close(syncDone)
	}()
	time.Sleep(10 * time.Millisecond) // enqueue order settles
	closeDone := make(chan struct{})
	go func() {
		e.Close() // mutClose lands after the mutSync
		close(closeDone)
	}()
	time.Sleep(10 * time.Millisecond)
	e.mu.Unlock()
	select {
	case <-syncDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Sync deadlocked: flusher exited before applying the in-flight mutSync")
	}
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
}

// TestOpsAfterCloseReturnErrClosed pins the post-Close contract for the
// control plane: Upsert and Cancel return ErrClosed without panicking or
// blocking, nothing is enqueued, and the trigger queue stays empty. Accepted
// caveat: Cancel CASes the slot word before submit fails — assert the error
// and the queue, not the slot word. Only Sync's post-Close path is pinned
// today.
func TestOpsAfterCloseReturnErrClosed(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	e.Close()
	if err := e.Upsert(testSpec(2, "USDTRY", PriceBid, DirGTE, 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-Close Upsert err=%v, want ErrClosed", err)
	}
	if err := e.Cancel(AlertID{1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-Close Cancel err=%v, want ErrClosed", err)
	}
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 990)); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-Close replace Upsert err=%v, want ErrClosed", err)
	}
	if n := e.mutQ.pending(); n != 0 {
		t.Fatalf("mutQ pending=%d after rejected ops, want 0", n)
	}
	if got := len(drainTriggers(e)); got != 0 {
		t.Fatalf("trigger queue yielded %d triggers after Close, want 0", got)
	}
}

// TestNewRejectsNegativeFlushBatch pins the fail-loudly-in-New contract for
// FlushBatch: New(FlushBatch < 1) must panic synchronously, mirroring
// TestNewRejectsNonPositiveReaperInterval's recover() pattern. Before the
// check, the flusher goroutine panicked asynchronously on
// make([]mutation, 0, negative), killing the process long after New returned.
func TestNewRejectsNegativeFlushBatch(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.FlushBatch = -1
	defer func() {
		if recover() == nil {
			t.Fatalf("New(FlushBatch=%d) did not panic synchronously", cfg.FlushBatch)
		}
	}()
	New(cfg)
}

// TestRetireGenStaleGenDoesNotRetireNewOccupant pins retireGen's gen-mismatch
// early return: a delayed mutRemove landing after the slot was retired,
// recycled, and re-handed must NOT park the new occupant's slot. Only the
// retired-bit dedupe branch (gen-identical duplicate removals) is covered by
// TestDuplicateRemovalDoesNotAliasSlots; the gen-mismatch branch keeps a live
// occupant out of the retired list and is exercised by no other test.
func TestRetireGenStaleGenDoesNotRetireNewOccupant(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	sid, err := e.stateFor("S")
	if err != nil {
		t.Fatal(err)
	}
	idx, _ := e.slots.alloc()
	e.slots.setStatus(idx, StatusActive)
	gen1 := e.slots.gen(idx)
	ent := entry{price: 425, id: AlertID{1},
		validFrom: 1, meta: makeEntryMeta(idx, gen1, makeFlags(PriceBid, DirGTE))}
	e.submit(mutation{op: mutInsert, sid: sid, e: ent})
	e.Sync()
	e.submit(mutation{op: mutRemove, sid: sid, e: ent})
	e.Sync()
	// Move idx through recycle and back out: a backed-up flusher's delayed
	// removal lands after this point.
	e.slots.recycle(time.Now().Add(time.Hour))
	idx2, _ := e.slots.alloc()
	if idx2 != idx {
		t.Fatalf("setup failed: alloc handed %d, wanted recycled %d", idx2, idx)
	}
	if w := e.slots.get(idx).Load(); w&slotRetiredBit != 0 {
		t.Fatal("recycled handout still carries the retired bit")
	}
	gen2 := e.slots.gen(idx)
	if gen2 <= gen1 {
		t.Fatalf("setup failed: re-handout gen %d not bumped past %d", gen2, gen1)
	}
	e.slots.setStatus(idx, StatusActive)
	occ := entry{price: 500, id: AlertID{2},
		validFrom: 1, meta: makeEntryMeta(idx, gen2, makeFlags(PriceBid, DirGTE))}
	e.submit(mutation{op: mutInsert, sid: sid, e: occ})
	e.Sync()
	// The delayed stale removal — gen belongs to the dead handout.
	e.submit(mutation{op: mutRemove, sid: sid, e: ent})
	e.Sync()
	if w := e.slots.get(idx).Load(); w&slotRetiredBit != 0 {
		t.Fatal("stale removal retired the new occupant's slot")
	}
	if g := e.slots.gen(idx); g != gen2 {
		t.Fatalf("slot gen=%d after stale removal, want %d", g, gen2)
	}
	e.slots.mu.Lock()
	inRetired := false
	for _, r := range e.slots.retired {
		if r.idx == idx {
			inRetired = true
		}
	}
	e.slots.mu.Unlock()
	if inRetired {
		t.Fatal("stale removal parked the new occupant in the retired list")
	}
	e.slots.recycle(time.Now().Add(time.Hour))
	e.slots.mu.Lock()
	freed := slices.Contains(e.slots.free, idx)
	e.slots.mu.Unlock()
	if freed {
		t.Fatal("stale removal made the live occupant's slot reusable")
	}
}

// TestUpsertReplacePausedSlot pins that a PAUSED alert is replaceable: the
// replace CAS's from-set must include StatusPaused, and the new handout must
// come back StatusActive — never inheriting PAUSED. Dropping PAUSED from the
// from-set would strand a zombie tree entry with no ledger ref.
func TestUpsertReplacePausedSlot(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if err := e.SetStatus(AlertID{1}, StatusPaused); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 430)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	sid, _ := e.syms.Get("USDTRY")
	ti := treeIndex(PriceBid, DirGTE)
	tree := e.states[sid].snap.Load().trees[ti]
	if got := tree.Len(); got != 1 {
		t.Fatalf("tree Len=%d after paused replace, want 1", got)
	}
	for en := range tree.All() {
		if en.price != 430 {
			t.Fatalf("entry price=%d, want the new 430", en.price)
		}
	}
	if s := e.Stats(); s.Live != 1 {
		t.Fatalf("Live=%d, want 1", s.Live)
	}
	if st, ok := e.Status(AlertID{1}); !ok || st != StatusActive {
		t.Fatalf("Status after paused replace = (%v, %v), want (StatusActive, true)", st, ok)
	}
	e.Match(&Tick{Symbol: "USDTRY", Bid: 435, Present: 1 << uint(PriceBid), TS: 100})
	got := drainTriggers(e)
	if len(got) != 1 || got[0].ID != (AlertID{1}) {
		t.Fatalf("triggers=%+v, want exactly one fire of the replacement", got)
	}
}

// TestCancelPausedAlert pins removeIfLive's from-set deterministically for
// StatusPaused, from both terminal paths: phase 1 cancels a paused alert
// (Cancel from PAUSED); phase 2 replaces one (Upsert over a paused slot).
// TestCancelAndSetStatus only ever cancels from ACTIVE.
func TestCancelPausedAlert(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	defer e.Close()
	ti := treeIndex(PriceBid, DirGTE)

	// Phase 1: cancel a paused alert.
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if err := e.SetStatus(AlertID{1}, StatusPaused); err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel(AlertID{1}); err != nil {
		t.Fatalf("Cancel from PAUSED err=%v, want nil", err)
	}
	e.Sync()
	if s := e.Stats(); s.Live != 0 {
		t.Fatalf("phase 1: Live=%d after cancel, want 0", s.Live)
	}
	sid, _ := e.syms.Get("USDTRY")
	if got := e.states[sid].snap.Load().trees[ti].Len(); got != 0 {
		t.Fatalf("phase 1: tree Len=%d after cancel, want 0", got)
	}
	e.mu.Lock()
	_, ok := e.refs[AlertID{1}]
	e.mu.Unlock()
	if ok {
		t.Fatal("phase 1: refs entry survived the cancel")
	}

	// Phase 2: replace a paused alert.
	if err := e.Upsert(testSpec(2, "USDTRY", PriceBid, DirGTE, 425)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	if err := e.SetStatus(AlertID{2}, StatusPaused); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	oldIdx := entryIdx(e.refs[AlertID{2}].e)
	e.mu.Unlock()
	if err := e.Upsert(testSpec(2, "USDTRY", PriceBid, DirGTE, 480)); err != nil {
		t.Fatalf("phase 2: replace of paused alert err=%v, want nil", err)
	}
	e.Sync()
	if s := e.Stats(); s.Live != 1 {
		t.Fatalf("phase 2: Live=%d after replace, want 1", s.Live)
	}
	tree := e.states[sid].snap.Load().trees[ti]
	if got := tree.Len(); got != 1 {
		t.Fatalf("phase 2: tree Len=%d after replace, want 1", got)
	}
	for en := range tree.All() {
		if en.price != 480 {
			t.Fatalf("phase 2: entry price=%d, want the new 480", en.price)
		}
	}
	// Default ReaperInterval is 1s with a 2s recycle grace: the old slot
	// cannot be recycled within this test, so the retired bit is stable.
	if w := e.slots.get(oldIdx).Load(); w&slotRetiredBit == 0 {
		t.Fatal("phase 2: old slot did not acquire the retired bit")
	}
}

// TestSnapshotPinBackoffPairsWithRetireRelease pins the reader/flusher pairing
// on bare snapshots: pin's increment-then-recheck must always restore the
// reader count to 0 when it backs off a retired snapshot, and retireRelease
// must release exactly when the count reaches 0. No existing test calls pin
// on a retired snapshot or exercises retireRelease's false branch.
func TestSnapshotPinBackoffPairsWithRetireRelease(t *testing.T) {
	// (a) Flush-order-first: retirement already marked when the reader pins.
	s := newSnapshot(0)
	s.retired.Store(true)
	if s.pin() {
		t.Fatal("pin succeeded on a retired snapshot")
	}
	if n := s.readers.Load(); n != 0 {
		t.Fatalf("backed-off pin left readers=%d, want 0", n)
	}
	if !s.retireRelease() {
		t.Fatal("retireRelease returned false with no readers")
	}

	// (b) Reader-first: the pin is held across retirement.
	s2 := newSnapshot(0)
	if !s2.pin() {
		t.Fatal("pin failed on a live snapshot")
	}
	s2.retired.Store(true)
	if s2.retireRelease() {
		t.Fatal("retireRelease released while a reader held the snapshot")
	}
	s2.unpin()
	if !s2.retireRelease() {
		t.Fatal("retireRelease returned false after the reader drained")
	}
	if n := s2.readers.Load(); n != 0 {
		t.Fatalf("readers=%d after release, want 0", n)
	}
}

// TestTriggerQueueOverflowDropsCountedExactly pins the saturated-queue
// accounting deterministically: with no consumer, TryPush succeeds exactly up
// to capacity, every further push is rejected and counted, and the queue
// drains FIFO. Existing tests check one rejected push or a draining consumer
// (nondeterministic drop counts). Drop-counting is the contract — the drops
// themselves are by design.
func TestTriggerQueueOverflowDropsCountedExactly(t *testing.T) {
	q := NewTriggerQueue(4)
	for i := 0; i < 10; i++ {
		want := i < 4
		if got := q.TryPush(Trigger{Price: Price(i)}); got != want {
			t.Fatalf("push %d accepted=%v, want %v", i, got, want)
		}
	}
	if got := q.Dropped(); got != 6 {
		t.Fatalf("Dropped=%d, want 6", got)
	}
	if got := q.Len(); got != 4 {
		t.Fatalf("Len=%d, want 4", got)
	}
	var prices []Price
	for {
		tr, ok := q.Pop()
		if !ok {
			break
		}
		prices = append(prices, tr.Price)
	}
	if !slices.Equal(prices, []Price{0, 1, 2, 3}) {
		t.Fatalf("drain order=%v, want [0 1 2 3]", prices)
	}
	if got := q.Dropped(); got != 6 {
		t.Fatalf("Dropped=%d after drain, want 6 (drops are never re-counted)", got)
	}
}

// TestInternerGetVisibilityImpliesName pins the happens-before between the
// Interner's two maps: once Get(name) reports a symbol interned by another
// goroutine, Name(id) must return the exact string — never "". Pins the
// LoadOrCompute ordering (names.Store before ids publication) under -race.
func TestInternerGetVisibilityImpliesName(t *testing.T) {
	in := NewInterner()
	const n = 1000
	go func() {
		for i := 0; i < n; i++ {
			in.Intern(fmt.Sprintf("SYM%04d", i))
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("SYM%04d", i)
		var id SymbolID
		for {
			got, ok := in.Get(name)
			if ok {
				id = got
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("symbol %d never became visible via Get within 5s", i)
			}
			runtime.Gosched()
		}
		if got := in.Name(id); got != name {
			t.Fatalf("Name(%d)=%q, want %q — Get visibility did not imply Name", id, got, name)
		}
	}
}

// TestTriggersDrainableAfterClose pins the "channel is never closed by the
// engine" contract: a queued trigger survives Close and Pop still delivers
// it afterwards. Every existing test drains triggers before Close.
func TestTriggersDrainableAfterClose(t *testing.T) {
	defer goleak.VerifyNone(t)
	e := New(DefaultConfig())
	if err := e.Upsert(testSpec(1, "USDTRY", PriceBid, DirGTE, 425)); err != nil {
		t.Fatal(err)
	}
	e.Sync()
	e.Match(&Tick{Symbol: "USDTRY", Bid: 430, Present: TickAllPresent(), TS: 100})
	e.Close() // drain strictly after Close returned
	tr, ok := e.Triggers().Pop()
	if !ok || tr.ID != (AlertID{1}) || tr.Price != 430 {
		t.Fatalf("post-Close Pop = (%+v, %v), want the fired trigger of alert 1 at 430", tr, ok)
	}
	if _, ok := e.Triggers().Pop(); ok {
		t.Fatal("second Pop after Close returned a trigger, want false")
	}
}

// TestSlotArenaExhaustionMessage pins the A2-1 fix: exhausting the arena
// must fail with an explicit, actionable message — not a bare index out of
// range from deep inside alloc. Sustained same-ID replace churn allocates
// past the MaxAlerts-derived chunk bound while retired slots wait out the
// recycle grace, so exhaustion is reachable in production configurations.
func TestSlotArenaExhaustionMessage(t *testing.T) {
	a := newSlotArena(4) // ceil(4/65536)=1 chunk + 1 margin = 2 chunks
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on arena exhaustion")
		}
		msg, ok := r.(string)
		if !ok || msg != "chrono-tree: slot arena exhausted: increase MaxAlerts or ReaperInterval" {
			t.Fatalf("panic = %v, want explicit exhaustion message", r)
		}
	}()
	for i := 0; i < 2*slotChunkSize+1; i++ {
		a.alloc()
	}
}
