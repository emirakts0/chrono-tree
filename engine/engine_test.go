package engine

import (
	"bytes"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
	"unsafe"
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
