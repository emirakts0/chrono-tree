package engine

import (
	"math"
	"slices"
	"testing"

	"github.com/tidwall/btype"
)

type confItem struct {
	price float64
	id    uint32
}

func confCompare(a, b confItem) int {
	if a.price < b.price {
		return -1
	}
	if a.price > b.price {
		return 1
	}
	switch {
	case a.id < b.id:
		return -1
	case a.id > b.id:
		return 1
	}
	return 0
}

func newConfTable() *btype.Table[confItem] {
	return btype.NewTableOptions(btype.TableOptions[confItem]{Compare: confCompare})
}

func TestBtypeConformance(t *testing.T) {
	tbl := newConfTable()
	items := []confItem{{10, 1}, {5, 2}, {20, 3}, {10, 0}, {20, 4}}
	for _, it := range items {
		tbl.Insert(it)
	}
	if got := tbl.Len(); got != 5 {
		t.Fatalf("Len = %d, want 5", got)
	}

	// Ascend(key): items >= key, ascending.
	var got []float64
	for it := range tbl.Ascend(confItem{price: 10}) {
		got = append(got, it.price)
	}
	if !slices.Equal(got, []float64{10, 10, 20, 20}) {
		t.Fatalf("Ascend(10) = %v, want [10 10 20 20]", got)
	}

	// Descend(key): items <= key under the FULL compare (price then id),
	// descending. A price-only pivot (id 0) stops at the first id at the
	// boundary price, so same-price items with a larger id are excluded.
	got = got[:0]
	for it := range tbl.Descend(confItem{price: 10}) {
		got = append(got, it.price)
	}
	if !slices.Equal(got, []float64{10, 5}) {
		t.Fatalf("Descend({10,0}) = %v, want [10 5]", got)
	}

	// A max-id probe includes every item at the boundary price: this is the
	// pivot form the engine's GTE scan must use.
	got = got[:0]
	for it := range tbl.Descend(confItem{price: 10, id: math.MaxUint32}) {
		got = append(got, it.price)
	}
	if !slices.Equal(got, []float64{10, 10, 5}) {
		t.Fatalf("Descend({10,max}) = %v, want [10 10 5]", got)
	}

	// Delete removes exactly one item (tie-break by id).
	tbl.Delete(confItem{10, 1})
	if got := tbl.Len(); got != 4 {
		t.Fatalf("Len after delete = %d, want 4", got)
	}

	// Copy is COW: mutating the copy must not affect the original.
	cp := newConfTable()
	*cp = *tbl.Copy()
	cp.Insert(confItem{99, 9})
	if tbl.Len() != 4 || cp.Len() != 5 {
		t.Fatalf("COW isolation broken: orig=%d copy=%d, want 4 and 5", tbl.Len(), cp.Len())
	}
	tbl.Release()
	cp.Release()
}
