// Package engine is chrono-tree's in-memory alert evaluation engine:
// lock-free snapshot reads, COW batched mutations, CAS-gated exactly-once firing.
package engine

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

// AlertID is a 128-bit UUID (v7 expected: time-ordered).
type AlertID [16]byte

// Price is a fixed-point integer in base units. The engine is scale-agnostic;
// decimal conversion happens only in the price package.
type Price int64

// PriceType selects which quote field of a tick an alert watches.
type PriceType uint8

const (
	PriceBid PriceType = iota
	PriceAsk
	PriceMid
	PriceLast
	priceTypeCount = 4
)

// Direction is the comparison direction of an alert's target price.
type Direction uint8

const (
	DirGTE Direction = iota // fires when market >= target
	DirLTE                  // fires when market <= target
)

// Status is the alert lifecycle state, packed into an atomic slot word.
type Status uint32

const (
	StatusZero Status = iota // slot not in use
	StatusActive
	StatusPaused
	StatusTriggered
	StatusCancelled
	StatusExpired
)

// entry is the hot-path alert record, stored by value inside btype.Table.
// Field order keeps the record at exactly one cache line (TestEntrySize);
// dims is sentinel-padded past the engine width.
type entry struct {
	price     Price          // target price, secondary sort key
	id        AlertID        // tie-breaker so equal (dims, price) coexist
	dims      [dimMax]uint16 // leading sort keys
	validFrom int64          // unix nanos
	expires   int64          // unix nanos; 0 = never
	idx       uint32         // slot arena index
	flags     uint8
}

const (
	flagActive         uint8 = 1 << 0
	flagPriceTypeMask  uint8 = 1<<1 | 1<<2
	flagPriceTypeShift       = 1
	flagDirection      uint8 = 1 << 3
	flagAutoDeactivate uint8 = 1 << 4
)

func (e *entry) priceType() PriceType {
	return PriceType(e.flags & flagPriceTypeMask >> flagPriceTypeShift)
}

func (e *entry) direction() Direction {
	if e.flags&flagDirection != 0 {
		return DirLTE
	}
	return DirGTE
}

func (e *entry) autoDeactivate() bool {
	return e.flags&flagAutoDeactivate != 0
}

func makeFlags(pt PriceType, dir Direction, autoDeactivate bool) uint8 {
	f := flagActive | uint8(pt)<<flagPriceTypeShift
	if dir == DirLTE {
		f |= flagDirection
	}
	if autoDeactivate {
		f |= flagAutoDeactivate
	}
	return f
}

// makeEntryCompare returns the comparator specialized to the engine's dim
// width. btype pivots use the full comparator, so boundary probes must be
// dims- and id-aware: AlertID{} sorts before every real UUID (for Ascend),
// the all-ones id sorts after every real UUID (for Descend).
func makeEntryCompare(width uint8) func(a, b entry) int {
	return func(a, b entry) int {
		for i := 0; i < int(width); i++ {
			if a.dims[i] != b.dims[i] {
				if a.dims[i] < b.dims[i] {
					return -1
				}
				return 1
			}
		}
		if a.price < b.price {
			return -1
		}
		if a.price > b.price {
			return 1
		}
		return bytes.Compare(a.id[:], b.id[:])
	}
}

// entryKey builds a Descend-unfriendly probe for Ascend: yields every entry
// with the probe's dims and price >= probe.
func entryKey(dims [dimMax]uint16, price Price) entry {
	return entry{dims: dims, price: price}
}

// entryKeyMax builds a probe whose id sorts after every real UUID, for
// Descend: yields every entry with the probe's dims and price <= probe.
func entryKeyMax(dims [dimMax]uint16, price Price) entry {
	var maxID AlertID
	for i := range maxID {
		maxID[i] = 0xff
	}
	return entry{dims: dims, price: price, id: maxID}
}

// slotArena hands out dense uint32 indices into fixed-size chunks of atomic
// status words. Chunks are allocated lazily under mu; the chunk-pointer
// slice has fixed length so hot-path get() never sees a growing header.
//
// Slot word layout: bits 0-7 status, bit 8 retired, bits 9-31 generation.
// Two invariants:
//   - gen gates staleness: alloc bumps the generation at every handout, so
//     a reference built against an older generation (above all the reaper's
//     expiry-table entries, which linger past the recycle grace) can never
//     win a full-word CAS against the slot's new occupant.
//   - retired dedupes removals: a mutRemove may land twice for one entry;
//     retireGen sets the bit exactly once per handout, so only the first
//     removal parks the slot.
//
// Freed slots wait a grace period (2× reaper interval, via recycle) before
// reuse, so readers of old snapshots never touch a recycled slot.
type slotArena struct {
	mu      sync.Mutex
	chunks  []*slotChunk // fixed length, indexed idx>>slotChunkBits
	free    []uint32
	retired []retiredSlot
	next    uint32
}

type slotChunk = [slotChunkSize]atomic.Uint32 // 256 KiB

type retiredSlot struct {
	idx uint32
	at  time.Time
}

const slotChunkBits = 16
const slotChunkSize = 1 << slotChunkBits

// Slot word layout: bits 0-7 status, bit 8 retired, bits 9-31 generation.
const slotRetiredBit = uint32(1) << 8
const slotGenShift = 9

func newSlotArena(maxAlerts uint64) *slotArena {
	n := (maxAlerts + slotChunkSize - 1) / slotChunkSize
	n++ // margin for slots in flight (allocated, removal queued)
	return &slotArena{chunks: make([]*slotChunk, n)}
}

func (a *slotArena) alloc() uint32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	var idx uint32
	if n := len(a.free); n > 0 {
		idx = a.free[n-1]
		a.free = a.free[:n-1]
	} else {
		idx = a.next
		a.next++
	}
	ci := idx >> slotChunkBits
	if a.chunks[ci] == nil {
		a.chunks[ci] = new(slotChunk)
	}
	s := &a.chunks[ci][idx&(slotChunkSize-1)]
	// Safe without CAS: alloc runs under a.mu, and a slot on the free list
	// always carries the retired bit, so no transition CAS can be in flight.
	w := s.Load() // 0 for a never-touched slot: generation 0
	s.Store(((w>>slotGenShift)+1)<<slotGenShift | uint32(StatusZero))
	return idx
}

// get returns the atomic status word for idx. Lock-free; hot-path safe.
func (a *slotArena) get(idx uint32) *atomic.Uint32 {
	return &a.chunks[idx>>slotChunkBits][idx&(slotChunkSize-1)]
}

// slotStatus extracts the status byte from a packed slot word.
func slotStatus(w uint32) Status { return Status(w & 0xff) }

// status reads the current status of idx, ignoring generation bits.
func (a *slotArena) status(idx uint32) Status {
	return slotStatus(a.get(idx).Load())
}

// setStatus transitions idx to to in a CAS loop, preserving generation bits.
func (a *slotArena) setStatus(idx uint32, to Status) {
	s := a.get(idx)
	for {
		w := s.Load()
		if s.CompareAndSwap(w, w&^0xff|uint32(to)) {
			return
		}
	}
}

// cas attempts one transition from→to on the full observed word, so it also
// fails when the generation moved (stale reference).
func (a *slotArena) cas(idx uint32, from, to Status) bool {
	s := a.get(idx)
	w := s.Load()
	if slotStatus(w) != from {
		return false
	}
	return s.CompareAndSwap(w, w&^0xff|uint32(to))
}

// casAny attempts the transition to from each of froms once.
func (a *slotArena) casAny(idx uint32, to Status, froms ...Status) bool {
	for _, from := range froms {
		if a.cas(idx, from, to) {
			return true
		}
	}
	return false
}

// gen reads the slot's current generation; it moves only at handout (alloc).
func (a *slotArena) gen(idx uint32) uint32 {
	return a.get(idx).Load() >> slotGenShift
}

// casGen is cas restricted to a specific generation, rejecting callers that
// hold a reference from an older generation even though the current status
// matches.
func (a *slotArena) casGen(idx uint32, gen uint32, from, to Status) bool {
	s := a.get(idx)
	w := s.Load()
	if w>>slotGenShift != gen || slotStatus(w) != from {
		return false
	}
	return s.CompareAndSwap(w, w&^0xff|uint32(to))
}

// casGenAny attempts the generation-checked transition to from each of
// froms once. Accepted ABA window: the 23-bit generation wraps after
// 8,388,608 handouts of a single slot — bounded, non-cascading, accepted.
func (a *slotArena) casGenAny(idx uint32, gen uint32, to Status, froms ...Status) bool {
	for _, from := range froms {
		if a.casGen(idx, gen, from, to) {
			return true
		}
	}
	return false
}

// retire parks a freed slot until recycle moves it past its grace period.
// Low-level: it does not touch the slot word; production paths use retireGen.
func (a *slotArena) retire(idx uint32, at time.Time) {
	a.mu.Lock()
	a.retired = append(a.retired, retiredSlot{idx: idx, at: at})
	a.mu.Unlock()
}

// retireGen sets the retired bit iff gen still identifies the current
// occupant and the bit is clear, then parks the slot for recycling. Only the
// first removal per handout parks the slot, which makes duplicate mutRemoves
// harmless.
func (a *slotArena) retireGen(idx, gen uint32, at time.Time) {
	s := a.get(idx)
	for {
		w := s.Load()
		if w&slotRetiredBit != 0 {
			return // already retired by the first landing
		}
		if w>>slotGenShift != gen {
			return // stale: the slot belongs to a newer occupant
		}
		if s.CompareAndSwap(w, w|slotRetiredBit) {
			break
		}
	}
	a.retire(idx, at)
}

// recycle returns retired slots older than before to the free list.
func (a *slotArena) recycle(before time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	i := 0
	for ; i < len(a.retired); i++ {
		if a.retired[i].at.After(before) {
			break
		}
		a.free = append(a.free, a.retired[i].idx)
	}
	a.retired = append(a.retired[:0], a.retired[i:]...)
}
