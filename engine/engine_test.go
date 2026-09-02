package engine

import (
	"bytes"
	"testing"
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
