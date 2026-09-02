// Package engine is chrono-tree's in-memory alert evaluation engine:
// lock-free snapshot reads, COW batched mutations, CAS-gated exactly-once firing.
package engine

import "bytes"

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
