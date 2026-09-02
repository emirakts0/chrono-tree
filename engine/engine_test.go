package engine

import (
	"bytes"
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
