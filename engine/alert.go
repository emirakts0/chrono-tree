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
	meta      uint64         // idx (bits 32-63) | gen (bits 8-30) | flags (bits 0-7)
}

const (
	flagPriceTypeMask  uint8 = 1<<1 | 1<<2
	flagPriceTypeShift       = 1
	flagDirection      uint8 = 1 << 3 // bit 0 is reserved/free
)

const (
	metaFlagsMask = uint64(0xff)
	metaGenShift  = 8
	metaGenMask   = uint64(0x7f_ff_ff) << metaGenShift // 23 bits, matches the slot word
	metaIdxShift  = 32
)

// makeEntryMeta packs the entry tail: idx major, then the 23-bit slot
// generation, then flags. Set once at upsert; never mutated afterwards.
func makeEntryMeta(idx, gen uint32, flags uint8) uint64 {
	return uint64(idx)<<metaIdxShift | (uint64(gen)&(1<<23-1))<<metaGenShift | uint64(flags)
}

func entryIdx(e entry) uint32  { return uint32(e.meta >> metaIdxShift) }
func entryGen(e entry) uint32  { return uint32(e.meta&metaGenMask) >> metaGenShift }
func entryFlags(e entry) uint8 { return uint8(e.meta & metaFlagsMask) }

func (e *entry) priceType() PriceType {
	return PriceType(entryFlags(*e) & flagPriceTypeMask >> flagPriceTypeShift)
}

func (e *entry) direction() Direction {
	if entryFlags(*e)&flagDirection != 0 {
		return DirLTE
	}
	return DirGTE
}

func makeFlags(pt PriceType, dir Direction) uint8 {
	f := uint8(pt) << flagPriceTypeShift
	if dir == DirLTE {
		f |= flagDirection
	}
	return f
}

// makeEntryCompare returns the comparator specialized to the engine's dim
// width. btype pivots use the full comparator, so boundary probes must be
// dims- and id-aware: AlertID{} sorts before every real UUID (for Ascend),
// the all-ones-id probe with the max-idx sentinel sorts after every real
// entry (for Descend).
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
		if c := bytes.Compare(a.id[:], b.id[:]); c != 0 {
			return c
		}
		// Identity tie-break: same alert, different handout. A delayed removal
		// of an old handout must never alias the replacement's live record.
		// meta is idx-major, so ordering matches the old bare-idx compare; the
		// gen bits additionally distinguish two handouts of one alert that
		// land on the same recycled slot.
		switch {
		case a.meta < b.meta:
			return -1
		case a.meta > b.meta:
			return 1
		}
		return 0
	}
}

// entryKey builds a Descend-unfriendly probe for Ascend: yields every entry
// with the probe's dims and price >= probe.
func entryKey(dims [dimMax]uint16, price Price) entry {
	return entry{dims: dims, price: price}
}

// maxAlertID is the all-ones UUID: it sorts after every real id, for
// Descend probes.
var maxAlertID = AlertID{
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
}

// entryMetaMaxIdx is the Descend-probe sentinel: idx ^uint32(0) in meta's
// major bits, so the probe sorts after every real entry — lesser ids by the
// id compare, the all-ones id by idx (a real idx of 2^32-1 implies a 16 GiB
// arena). GTE thus mirrors LTE's zero-id probe, which sorts before every
// accepted id.
const entryMetaMaxIdx = uint64(^uint32(0)) << metaIdxShift

// entryKeyMax builds a probe that sorts after every real entry, for Descend:
// yields every entry with the probe's dims and price <= probe.
func entryKeyMax(dims [dimMax]uint16, price Price) entry {
	return entry{dims: dims, price: price, id: maxAlertID, meta: entryMetaMaxIdx}
}

// slotArena hands out dense uint32 indices into fixed-size chunks of atomic
// status words. Chunks are allocated lazily under mu; the chunk-pointer
// slice has fixed length so hot-path get() never sees a growing header.
//
// Slot word layout: bits 0-7 status, bit 8 retired, bits 9-31 generation.
// Two invariants:
//   - gen gates staleness because alloc bumps the generation at every
//     handout, so a reference built against an older generation is rejected
//     by the generation check in fire (and retireGen's gen comparison)
//     rather than by the CAS alone.
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

func (a *slotArena) alloc() (idx, gen uint32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if n := len(a.free); n > 0 {
		idx = a.free[n-1]
		a.free = a.free[:n-1]
	} else {
		idx = a.next
		a.next++
	}
	ci := idx >> slotChunkBits
	if int(ci) >= len(a.chunks) {
		// Exhausted: sustained replace churn allocates past the MaxAlerts
		// bound while retired slots wait out the recycle grace. Fail loudly
		// instead of a bare index out of range.
		panic("chrono-tree: slot arena exhausted: increase MaxAlerts or ReaperInterval")
	}
	if a.chunks[ci] == nil {
		a.chunks[ci] = new(slotChunk)
	}
	s := &a.chunks[ci][idx&(slotChunkSize-1)]
	// Safe without CAS: alloc runs under a.mu, and a slot on the free list
	// always carries the retired bit, so no transition CAS can be in flight.
	w := s.Load() // 0 for a never-touched slot: generation 0
	gen = (w>>slotGenShift + 1) & (1<<23 - 1)
	s.Store(gen<<slotGenShift | uint32(StatusZero))
	return idx, gen
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

// gen reads the slot's current generation; it moves only at handout (alloc).
func (a *slotArena) gen(idx uint32) uint32 {
	return a.get(idx).Load() >> slotGenShift
}

// casStatusAny attempts the transition to from each of froms, retrying
// while the word keeps moving between matched states. It carries no expected
// generation: callers establish slot liveness by other means — the sweep by
// ref-identity against e.refs, control-plane paths by holding the alert's
// ref — while the full-word CAS itself preserves generation bits, so a
// stale word from an older handout can never win. Not used by fire: the
// variadic froms loop costs ~8% on fire-heavy serial scans.
func (a *slotArena) casStatusAny(idx uint32, to Status, froms ...Status) bool {
	s := a.get(idx)
	for {
		w := s.Load()
		st := slotStatus(w)
		matched := false
		for _, from := range froms {
			if st == from {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
		if s.CompareAndSwap(w, w&^0xff|uint32(to)) {
			return true
		}
	}
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
