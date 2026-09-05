// Package alertstore is the persistent alert catalog: one bbolt record
// per alert (state updated in place) plus secondary indexes so the
// inquiry API never full-scans. The engine stays the only in-RAM alert
// state; this store is the source of truth across restarts.
package alertstore

import (
	"encoding/binary"
	"errors"

	"github.com/emir/chrono-tree/engine"
)

// State is the persisted lifecycle state. Values are also the idx_state
// key prefix bytes, so active < triggered < cancelled in key order.
type State uint8

const (
	StateActive State = iota + 1
	StateTriggered
	StateCancelled
)

func (s State) String() string {
	switch s {
	case StateActive:
		return "active"
	case StateTriggered:
		return "triggered"
	case StateCancelled:
		return "cancelled"
	}
	return "unknown"
}

// Alert is the persisted record: everything needed to enrich an engine
// Trigger and to answer inquiries, independent of process lifetime.
// FiredPrice/FiredAt are zero until the pump flips State to triggered.
type Alert struct {
	ID             engine.AlertID
	Symbol         string
	Decimals       uint8
	Venue, Tier    string
	PriceType      engine.PriceType
	Direction      engine.Direction
	TargetPrice    engine.Price
	ValidFrom      int64 // unix nanos
	Expires        int64 // unix nanos; 0 = never
	State          State
	AutoDeactivate bool
	CreatedAt      int64 // unix nanos
	FiredPrice     engine.Price
	FiredAt        int64 // unix nanos
}

// Record layout: 16 id | decimals | priceType | direction | state |
// autoDeactivate | 6×8 big-endian int64s, then length-prefixed symbol,
// venue, tier (uint16 lengths). Fixed head keeps decode branch-free.
const recHeadLen = 16 + 5 + 6*8

var errShortRecord = errors.New("alertstore: truncated record")

func appendAlert(b []byte, a *Alert) []byte {
	b = append(b, a.ID[:]...)
	b = append(b, a.Decimals, uint8(a.PriceType), uint8(a.Direction), uint8(a.State))
	if a.AutoDeactivate {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	var tmp [8]byte
	for _, v := range [6]int64{
		int64(a.TargetPrice), a.ValidFrom, a.Expires, a.CreatedAt,
		int64(a.FiredPrice), a.FiredAt,
	} {
		binary.BigEndian.PutUint64(tmp[:], uint64(v))
		b = append(b, tmp[:]...)
	}
	b = appendString(b, a.Symbol)
	b = appendString(b, a.Venue)
	b = appendString(b, a.Tier)
	return b
}

func appendString(b []byte, s string) []byte {
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(s)))
	b = append(b, l[:]...)
	return append(b, s...)
}

func decodeAlert(b []byte) (Alert, error) {
	if len(b) < recHeadLen {
		return Alert{}, errShortRecord
	}
	var a Alert
	copy(a.ID[:], b[:16])
	a.Decimals = b[16]
	a.PriceType = engine.PriceType(b[17])
	a.Direction = engine.Direction(b[18])
	a.State = State(b[19])
	a.AutoDeactivate = b[20] != 0
	i := 21
	for _, p := range []*int64{
		(*int64)(&a.TargetPrice), &a.ValidFrom, &a.Expires, &a.CreatedAt,
		(*int64)(&a.FiredPrice), &a.FiredAt,
	} {
		*p = int64(binary.BigEndian.Uint64(b[i : i+8]))
		i += 8
	}
	rest := b[recHeadLen:]
	for _, p := range []*string{&a.Symbol, &a.Venue, &a.Tier} {
		var err error
		if rest, err = cutString(rest, p); err != nil {
			return Alert{}, err
		}
	}
	return a, nil
}

func cutString(b []byte, dst *string) ([]byte, error) {
	if len(b) < 2 {
		return nil, errShortRecord
	}
	n := int(binary.BigEndian.Uint16(b[:2]))
	if len(b) < 2+n {
		return nil, errShortRecord
	}
	*dst = string(b[2 : 2+n]) // copies: b is only valid inside the tx
	return b[2+n:], nil
}

// invTS inverts a unix-nanos timestamp so that ascending byte order of
// the big-endian encoding equals newest-first — index scans need no
// reverse cursors.
func invTS(ts int64) uint64 { return ^uint64(ts) }

// idxKey builds a secondary-index key: value prefix (state/direction
// byte or field string) + inverted createdAt + id. The id suffix makes
// ordering deterministic on equal timestamps.
func idxKey(prefix []byte, ts int64, id engine.AlertID) []byte {
	k := make([]byte, 0, len(prefix)+24)
	k = append(k, prefix...)
	var t [8]byte
	binary.BigEndian.PutUint64(t[:], invTS(ts))
	k = append(k, t[:]...)
	return append(k, id[:]...)
}

// idxTail extracts createdAt and id from an index key whose value prefix
// has length off (the caller knows what it sought with). ok is false
// for foreign-length keys — tolerated, never fatal.
func idxTail(key []byte, off int) (ts int64, id engine.AlertID, ok bool) {
	if off < 0 || len(key) != off+24 {
		return 0, engine.AlertID{}, false
	}
	ts = int64(^binary.BigEndian.Uint64(key[off : off+8]))
	copy(id[:], key[off+8:])
	return ts, id, true
}

func statePrefix(s State) []byte { return []byte{byte(s)} }

func dirPrefix(d engine.Direction) []byte { return []byte{byte(d)} }
