// Package engine is chrono-tree's in-memory alert evaluation engine:
// lock-free snapshot reads, COW batched mutations, CAS-gated exactly-once firing.
package engine

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

// AlertID is a 128-bit UUID (v7 expected: time-ordered). Always stored by value.
type AlertID [16]byte

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

// Status is the alert lifecycle state, held in an atomic slot so snapshot
// readers can retire an alert without mutating shared trees.
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
// Field order minimizes padding: 48 bytes on 64-bit (guarded by TestEntrySize).
type entry struct {
	price     float64 // target price, primary sort key
	id        AlertID // tie-breaker so equal prices coexist
	validFrom int64   // unix nanos
	expires   int64   // unix nanos; 0 = never
	idx       uint32  // index into Engine slot arena
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

// compareEntry orders entries by (price, id): price-ordered iteration with
// UUIDs breaking ties. btype pivots are compared with the FULL comparator, so
// boundary probes must be id-aware: AlertID{} sorts before any real UUID
// (correct for Ascend); a Descend probe needs an id that sorts after every
// real UUID or same-price entries are skipped (pinned by Task 1's test).
func compareEntry(a, b entry) int {
	if a.price < b.price {
		return -1
	}
	if a.price > b.price {
		return 1
	}
	return bytes.Compare(a.id[:], b.id[:])
}

// entryKey builds a probe entry for keyed Ascend seeks: id AlertID{} sorts
// first, so Ascend(entryKey(price)) yields every entry with price >= probe.
func entryKey(price float64) entry { return entry{price: price} }

// entryKeyMax builds a probe entry for keyed Descend seeks: the all-ones id
// sorts after every real UUID, so Descend(entryKeyMax(price)) yields every
// entry with price <= probe (including entries exactly at the boundary price).
func entryKeyMax(price float64) entry {
	var maxID AlertID
	for i := range maxID {
		maxID[i] = 0xff
	}
	return entry{price: price, id: maxID}
}

// slotArena hands out dense uint32 indices into fixed-size chunks of atomic
// status words. The chunk-pointer slice has fixed length (set at construction
// from MaxAlerts) so hot-path get() never observes a growing slice header;
// chunks themselves are allocated lazily under mu. Publication safety: a
// chunk pointer is stored before any entry referencing the slot is submitted
// to the mutation queue, and the queue/atomic-pointer chain provides the
// happens-before edge to readers.
//
// Freed slots are retired, not immediately reused: a reader holding an old
// snapshot may still see the dead entry and touch its slot, so reuse waits
// for a grace period (2× reaper interval) driven by recycle().
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

func newSlotArena(maxAlerts uint64) *slotArena {
	n := (maxAlerts + slotChunkSize - 1) / slotChunkSize
	// Margin: slots in flight (allocated, removal queued) can briefly exceed
	// the live-alert count; one extra chunk absorbs any lag.
	n++
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
	a.chunks[ci][idx&(slotChunkSize-1)].Store(uint32(StatusZero))
	return idx
}

// get returns the atomic status word for idx. Lock-free; hot-path safe.
func (a *slotArena) get(idx uint32) *atomic.Uint32 {
	return &a.chunks[idx>>slotChunkBits][idx&(slotChunkSize-1)]
}

// retire parks a freed slot until recycle moves it past its grace period.
func (a *slotArena) retire(idx uint32) {
	a.mu.Lock()
	a.retired = append(a.retired, retiredSlot{idx: idx, at: time.Now()})
	a.mu.Unlock()
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
