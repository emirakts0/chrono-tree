package alertstore

import (
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/emir/chrono-tree/engine"
)

var (
	bktAlerts       = []byte("alerts")
	bktIdxState     = []byte("idx_state")
	bktIdxSymbol    = []byte("idx_symbol")
	bktIdxVenue     = []byte("idx_venue")
	bktIdxTier      = []byte("idx_tier")
	bktIdxDirection = []byte("idx_direction")
	bktIdxCreated   = []byte("idx_created") // ~createdAt|id over ALL records: the no-filter inquiry walk
	bktAll          = [][]byte{bktAlerts, bktIdxState, bktIdxSymbol, bktIdxVenue, bktIdxTier, bktIdxDirection, bktIdxCreated}
)

// Store is the persistent alert catalog. Safe for concurrent use:
// readers share MVCC view txs, writes serialize through bolt's single
// writer (Batch opportunistically coalesces concurrent commits into one
// fsync). Every commit is synced — the store is the source of truth.
type Store struct {
	db *bolt.DB
}

// Open creates or opens the bbolt file. A corrupt or foreign-locked
// file is an error: source of truth or nothing.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("alertstore open %s: %w", path, err)
	}
	err = db.Batch(func(tx *bolt.Tx) error {
		for _, n := range bktAll {
			if _, err := tx.CreateBucketIfNotExists(n); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("alertstore init: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Put writes a, replacing any previous record for its ID, and maintains
// every index atomically: the previous record's entries are removed
// first (a re-registered terminal ID changes state, so blind upsert of
// index keys would orphan the old ones).
func (s *Store) Put(a Alert) error {
	return s.db.Batch(func(tx *bolt.Tx) error {
		alerts := tx.Bucket(bktAlerts)
		if raw := alerts.Get(a.ID[:]); raw != nil {
			prev, err := decodeAlert(raw)
			if err != nil {
				return err
			}
			if err := deleteIndexes(tx, &prev); err != nil {
				return err
			}
		}
		return putIndexed(tx, &a)
	})
}

func (s *Store) Get(id engine.AlertID) (Alert, bool, error) {
	var a Alert
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bktAlerts).Get(id[:])
		if raw == nil {
			return nil
		}
		var err error
		if a, err = decodeAlert(raw); err != nil {
			return err
		}
		found = true
		return nil
	})
	if err != nil {
		return Alert{}, false, err
	}
	return a, found, nil
}

// Delete removes the record and all its index entries. Rollback path
// for an upsert whose engine submission failed.
func (s *Store) Delete(id engine.AlertID) error {
	return s.db.Batch(func(tx *bolt.Tx) error {
		alerts := tx.Bucket(bktAlerts)
		raw := alerts.Get(id[:])
		if raw == nil {
			return nil
		}
		prev, err := decodeAlert(raw)
		if err != nil {
			return err
		}
		if err := deleteIndexes(tx, &prev); err != nil {
			return err
		}
		return alerts.Delete(id[:])
	})
}

// BatchGet resolves many IDs in one read tx — the pump's bulk
// enrichment fetch. Unknown IDs are absent from the map.
func (s *Store) BatchGet(ids []engine.AlertID) (map[engine.AlertID]Alert, error) {
	out := make(map[engine.AlertID]Alert, len(ids))
	err := s.db.View(func(tx *bolt.Tx) error {
		alerts := tx.Bucket(bktAlerts)
		for _, id := range ids {
			raw := alerts.Get(id[:])
			if raw == nil {
				continue
			}
			a, err := decodeAlert(raw)
			if err != nil {
				return err
			}
			out[id] = a
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Cancel flips active→cancelled. False for unknown or terminal alerts —
// terminal stays terminal.
func (s *Store) Cancel(id engine.AlertID) (bool, error) {
	n, err := s.CancelBatch([]engine.AlertID{id})
	return n == 1, err
}

// CancelBatch flips each active alert to cancelled in one write tx.
// Bolt's single writer makes the read-check-flip inside the tx atomic
// against MarkTriggeredBatch — whichever lands first wins.
func (s *Store) CancelBatch(ids []engine.AlertID) (int, error) {
	flipped := 0
	err := s.db.Batch(func(tx *bolt.Tx) error {
		alerts := tx.Bucket(bktAlerts)
		for _, id := range ids {
			a, ok, err := getInTx(alerts, id)
			if err != nil || !ok || a.State != StateActive {
				return err
			}
			if err := flipState(tx, alerts, &a, StateCancelled, 0, 0); err != nil {
				return err
			}
			flipped++
		}
		return nil
	})
	return flipped, err
}

// Fired is one engine trigger to be marked on its alert record.
type Fired struct {
	ID    engine.AlertID
	Price engine.Price
	At    int64 // unix nanos
}

// MarkTriggeredBatch flips each fired alert active→triggered, writing
// fired price/time — iff it is still active when the tx runs (a cancel
// racing the publish wins; the flip is skipped). Re-writing the four
// value indexes per flip is idempotent: same key, nil value.
func (s *Store) MarkTriggeredBatch(fired []Fired) (int, error) {
	flipped := 0
	err := s.db.Batch(func(tx *bolt.Tx) error {
		alerts := tx.Bucket(bktAlerts)
		for i := range fired {
			f := &fired[i]
			a, ok, err := getInTx(alerts, f.ID)
			if err != nil || !ok || a.State != StateActive {
				return err
			}
			if err := flipState(tx, alerts, &a, StateTriggered, f.Price, f.At); err != nil {
				return err
			}
			flipped++
		}
		return nil
	})
	return flipped, err
}

func getInTx(alerts *bolt.Bucket, id engine.AlertID) (Alert, bool, error) {
	raw := alerts.Get(id[:])
	if raw == nil {
		return Alert{}, false, nil
	}
	a, err := decodeAlert(raw)
	if err != nil {
		return Alert{}, false, err
	}
	return a, true, nil
}

// flipState rewrites one record in a new state, swapping only its
// idx_state entry (value indexes carry no state).
func flipState(tx *bolt.Tx, alerts *bolt.Bucket, a *Alert, to State, firedPrice engine.Price, firedAt int64) error {
	if err := tx.Bucket(bktIdxState).Delete(idxKey(statePrefix(a.State), a.CreatedAt, a.ID)); err != nil {
		return err
	}
	a.State = to
	a.FiredPrice = firedPrice
	a.FiredAt = firedAt
	return putIndexed(tx, a)
}

func putIndexed(tx *bolt.Tx, a *Alert) error {
	var buf []byte // grown by appendAlert; tiny, avoids a package-level scratch
	buf = appendAlert(buf, a)
	if err := tx.Bucket(bktAlerts).Put(a.ID[:], buf); err != nil {
		return err
	}
	return putIndexes(tx, a)
}

func putIndexes(tx *bolt.Tx, a *Alert) error {
	type entry struct{ bkt, key []byte }
	for _, e := range []entry{
		{bktIdxState, idxKey(statePrefix(a.State), a.CreatedAt, a.ID)},
		{bktIdxSymbol, idxKey([]byte(a.Symbol), a.CreatedAt, a.ID)},
		{bktIdxVenue, idxKey([]byte(a.Venue), a.CreatedAt, a.ID)},
		{bktIdxTier, idxKey([]byte(a.Tier), a.CreatedAt, a.ID)},
		{bktIdxDirection, idxKey(dirPrefix(a.Direction), a.CreatedAt, a.ID)},
		{bktIdxCreated, idxKey(nil, a.CreatedAt, a.ID)},
	} {
		if err := tx.Bucket(e.bkt).Put(e.key, nil); err != nil {
			return err
		}
	}
	return nil
}

func deleteIndexes(tx *bolt.Tx, a *Alert) error {
	type entry struct{ bkt, key []byte }
	for _, e := range []entry{
		{bktIdxState, idxKey(statePrefix(a.State), a.CreatedAt, a.ID)},
		{bktIdxSymbol, idxKey([]byte(a.Symbol), a.CreatedAt, a.ID)},
		{bktIdxVenue, idxKey([]byte(a.Venue), a.CreatedAt, a.ID)},
		{bktIdxTier, idxKey([]byte(a.Tier), a.CreatedAt, a.ID)},
		{bktIdxDirection, idxKey(dirPrefix(a.Direction), a.CreatedAt, a.ID)},
		{bktIdxCreated, idxKey(nil, a.CreatedAt, a.ID)},
	} {
		if err := tx.Bucket(e.bkt).Delete(e.key); err != nil {
			return err
		}
	}
	return nil
}
