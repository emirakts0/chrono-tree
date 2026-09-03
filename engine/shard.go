package engine

import (
	"runtime"
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
// btype's Release() clears the Table headers in place, so a snapshot must
// never be released while a reader is mid-scan: readers pin/unpin an
// atomic count, and the flusher defers release until it drains to zero.
type snapshot struct {
	trees   [8]btype.Table[entry]
	readers atomic.Int32 // in-flight Match scans; gates release
	retired atomic.Bool  // set by the flusher before a replacement publishes
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

// pin registers an in-flight scan so the flusher cannot release this
// snapshot's trees while they are being read. Increment-then-recheck pairs
// with retireRelease's mark-then-count-check: a reader whose increment the
// flusher cannot see is guaranteed to observe retired and back off (the
// seq-cst Dekker ordering), so release never overlaps a scan. Lock-free
// and allocation-free.
func (s *snapshot) pin() bool {
	s.readers.Add(1)
	if s.retired.Load() {
		s.readers.Add(-1)
		return false
	}
	return true
}

func (s *snapshot) unpin() { s.readers.Add(-1) }

// shutdownRelease is Close's synchronous variant of retireRelease: mark the
// snapshot retired (late pins back off and reload), then wait for every
// in-flight reader to unpin before releasing the trees. This enforces the
// no-release-under-a-scan invariant at shutdown instead of trusting the
// lifecycle contract: Close cannot free trees a Match scan is still walking.
// Readers only ever decrement, so the spin always terminates.
func (s *snapshot) shutdownRelease() {
	s.retired.Store(true)
	for s.readers.Load() != 0 {
		runtime.Gosched()
	}
	s.release()
}

// retireRelease marks the snapshot retired (late pins back off and reload
// the newer snapshot) and releases its trees once no reader remains.
// Returns false when readers are still pinned; the caller parks the
// snapshot for a later re-check. Flusher-owned.
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
