package engine

import (
	"sync/atomic"

	"github.com/tidwall/btype"
)

// treeIndex addresses the per-(priceType, direction) trees of a snapshot.
// Partitioning by all three dimensions upfront means a scan touches exactly
// one tree and every entry it reaches is a candidate by construction.
func treeIndex(pt PriceType, dir Direction) int {
	return int(pt)<<1 | int(dir)
}

// snapshot is an immutable view of one symbol's alert trees, published via
// atomic.Pointer and retired with release() once the last reader is done.
type snapshot struct {
	trees [8]btype.Table[entry]
}

func newSnapshot() *snapshot {
	s := &snapshot{}
	for i := range s.trees {
		s.trees[i] = *btype.NewTableOptions(btype.TableOptions[entry]{Compare: compareEntry})
	}
	return s
}

// copy returns a private COW clone, safe to mutate before republishing.
func (s *snapshot) copy() *snapshot {
	n := &snapshot{}
	for i := range s.trees {
		n.trees[i] = *s.trees[i].Copy()
	}
	return n
}

func (s *snapshot) release() {
	for i := range s.trees {
		s.trees[i].Release()
	}
}

// symbolState publishes snapshots for one symbol.
type symbolState struct {
	snap atomic.Pointer[snapshot]
}
