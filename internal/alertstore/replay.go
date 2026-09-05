package alertstore

import (
	"bytes"

	bolt "go.etcd.io/bbolt"
)

// EachActive walks the active set newest-first in one read tx. Boot
// replay: the caller rebuilds engine state from these records. fn's
// error stops the walk and is returned verbatim.
func (s *Store) EachActive(fn func(Alert) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		alerts := tx.Bucket(bktAlerts)
		c := tx.Bucket(bktIdxState).Cursor()
		prefix := statePrefix(StateActive)
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			_, id, ok := idxTail(k, len(prefix))
			if !ok {
				continue
			}
			raw := alerts.Get(id[:])
			if raw == nil {
				continue
			}
			a, err := decodeAlert(raw)
			if err != nil {
				return err
			}
			if err := fn(a); err != nil {
				return err
			}
		}
		return nil
	})
}

// Counts tallies each state's index entries in one read tx — the boot
// snapshot for /stats gauges (historical states included).
func (s *Store) Counts() (active, triggered, cancelled int, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bktIdxState)
		for _, e := range []struct {
			s State
			n *int
		}{{StateActive, &active}, {StateTriggered, &triggered}, {StateCancelled, &cancelled}} {
			c := b.Cursor()
			prefix := statePrefix(e.s)
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				*e.n++
			}
		}
		return nil
	})
	return active, triggered, cancelled, err
}
