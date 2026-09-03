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

// Price is a price in base units of the instrument's smallest quoted tick
// (fixed-point integer). Scale-agnostic: the engine never knows where the
// decimal point is; conversion between decimal text/floats and base units
// happens only in the price package, at the ingestion boundary.
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
	price     Price   // target price in base units, primary sort key
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
func entryKey(price Price) entry { return entry{price: price} }

// entryKeyMax builds a probe entry for keyed Descend seeks: the all-ones id
// sorts after every real UUID, so Descend(entryKeyMax(price)) yields every
// entry with price <= probe (including entries exactly at the boundary price).
func entryKeyMax(price Price) entry {
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
//
// Every slot word packs a generation counter and a retired bit:
// word = gen<<9 | retired<<8 | status (Status values fit in 8 bits; gen is
// 23 bits). alloc() bumps the generation on every handout, so any stale
// reference to the slot built against an older generation — above all the
// reaper's expiry-table entries, which linger until their expires passes and
// are NOT covered by the recycle grace (tree-entry staleness is: the flusher
// removes those within flush lag, well inside the grace) — can never
// transition the word: the full-word CAS fails. A stale expiry sweep hitting
// a recycled slot would otherwise silently kill the slot's new occupant
// (Active→Expired) and park its live slot forever. Full-word CASes make every
// transition fail against a word from another generation, which kills
// in-flight stale CAS attempts; for freshly-loaded checks (the reaper's
// expiry sweep) the generation captured at registration time must be
// validated explicitly — see casGen.
//
// The retired bit is the duplicate-removal guard: a mutRemove may land twice
// for one entry (e.g. a replacement racing a fire), and parking the slot
// twice would hand the SAME index to two future alerts. retireGen sets the
// bit exactly once per handout, so only the first removal parks the slot.
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
	s := &a.chunks[ci][idx&(slotChunkSize-1)]
	// The non-CAS Load/Store below is safe: alloc runs under a.mu, and a
	// free-listed slot always carries the retired bit — production retire
	// paths all go through retireGen (bare retire is test-only) — while a
	// never-touched slot is invisible to everyone until this Store publishes
	// it, and references to a freed slot die within flush lag, well inside
	// the recycle grace, so no transition CAS can be in flight on it.
	w := s.Load() // 0 for a never-touched slot: generation 0
	// Fresh word: next generation, retired bit clear, status zero.
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

// cas attempts one transition from→to. The CAS is on the full observed word,
// so it fails if the status is not from OR the generation moved (stale
// reference) — which is exactly the protection we want.
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

// gen reads the slot's current generation. Valid while the caller holds the
// slot: the generation moves only at handout (alloc).
func (a *slotArena) gen(idx uint32) uint32 {
	return a.get(idx).Load() >> slotGenShift
}

// casGen is cas restricted to a specific generation. A caller holding a
// reference from an older generation (e.g. a reaper expiry entry created
// before the slot was retired, recycled, and reused) is rejected here even
// though the CURRENT status may match from: cas/casAny alone cannot detect
// staleness, because they load the live word.
func (a *slotArena) casGen(idx uint32, gen uint32, from, to Status) bool {
	s := a.get(idx)
	w := s.Load()
	if w>>slotGenShift != gen || slotStatus(w) != from {
		return false
	}
	return s.CompareAndSwap(w, w&^0xff|uint32(to))
}

// casGenAny attempts the generation-checked transition to from each of
// froms once.
//
// Accepted ABA window: the 23-bit generation wraps after 8,388,608 handouts
// of a single slot; a stale expiry entry whose expires horizon spans that
// many reuses of its slot could match once — bounded, non-cascading, accepted.
func (a *slotArena) casGenAny(idx uint32, gen uint32, to Status, froms ...Status) bool {
	for _, from := range froms {
		if a.casGen(idx, gen, from, to) {
			return true
		}
	}
	return false
}

// retire parks a freed slot until recycle moves it past its grace period.
// Low-level: it does not touch the slot word. Production removal paths use
// retireGen, which sets the retired bit first so duplicates cannot park the
// slot twice.
func (a *slotArena) retire(idx uint32) {
	a.mu.Lock()
	a.retired = append(a.retired, retiredSlot{idx: idx, at: time.Now()})
	a.mu.Unlock()
}

// retireGen marks the slot retired (bit 8) iff gen still identifies the
// current occupant and the bit is clear, then parks it for recycling. It
// returns without parking for a duplicate removal — the retired bit is
// already set — or a stale one — the slot has since been recycled into a new
// generation. Exactly one removal per handout ever parks the slot, which is
// what makes duplicate mutRemoves harmless.
func (a *slotArena) retireGen(idx, gen uint32) {
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
	a.retire(idx)
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
