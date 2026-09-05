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
