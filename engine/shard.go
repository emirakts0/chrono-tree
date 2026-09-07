package engine

import (
	"runtime"
	"sync/atomic"

	"github.com/tidwall/btype"
)

// treeIndex addresses the per-(priceType, direction) trees of a snapshot, so
// a scan touches exactly one tree.
func treeIndex(pt PriceType, dir Direction) int {
	return int(pt)<<1 | int(dir)
}

// snapshot is an immutable view of one symbol's alert trees, published via
// atomic.Pointer. Readers pin/unpin an atomic count; the flusher defers
// release (btype Release clears table headers in place) until readers drain.
type snapshot struct {
	trees   [8]btype.Table[entry]
	readers atomic.Int32 // in-flight Match scans; gates release
	retired atomic.Bool  // set by the flusher before a replacement publishes
}

func newSnapshot(width uint8) *snapshot {
	s := &snapshot{}
	cmp := makeEntryCompare(width)
	for i := range s.trees {
		s.trees[i] = *btype.NewTableOptions(btype.TableOptions[entry]{Compare: cmp})
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

// pin registers an in-flight scan so release cannot overlap it. The
// increment-then-recheck pairs with retireRelease's mark-then-check: a
// reader whose increment the flusher misses is guaranteed to observe retired
// and back off.
func (s *snapshot) pin() bool {
	s.readers.Add(1)
	if s.retired.Load() {
		s.readers.Add(-1)
		return false
	}
	return true
}

func (s *snapshot) unpin() { s.readers.Add(-1) }

// shutdownRelease marks the snapshot retired, spins until every reader has
// unpinned, then releases the trees. Close's synchronous variant of
// retireRelease.
func (s *snapshot) shutdownRelease() {
	s.retired.Store(true)
	for s.readers.Load() != 0 {
		runtime.Gosched()
	}
	s.release()
}

// retireRelease marks the snapshot retired and releases its trees once no
// reader remains. Returns false when readers are still pinned; the caller
// parks the snapshot for a later re-check. Flusher-owned.
func (s *snapshot) retireRelease() bool {
	s.retired.Store(true)
	if s.readers.Load() != 0 {
		return false
	}
	s.release()
	return true
}

// symbolState publishes snapshots for one symbol.
type symbolState struct {
	snap atomic.Pointer[snapshot]
}
