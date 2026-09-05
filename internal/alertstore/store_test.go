package alertstore

import (
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/emir/chrono-tree/engine"
)

func openStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "alerts.bbolt")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, path
}

// indexSnapshot materializes every index bucket for exact-compare: after
// any mutation sequence the indexes must contain exactly the entries the
// records imply — no orphans, no strays.
func (s *Store) indexSnapshot(t *testing.T) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	err := s.db.View(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bktIdxState, bktIdxSymbol, bktIdxVenue, bktIdxTier, bktIdxDirection, bktIdxCreated} {
			keys := []string{}
			c := tx.Bucket(name).Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				keys = append(keys, string(k))
			}
			out[string(name)] = keys
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func putN(t *testing.T, s *Store, n int) []Alert {
	t.Helper()
	out := make([]Alert, 0, n)
	for i := range n {
		a := sampleAlert()
		copy(a.ID[:], []byte{byte(i >> 8), byte(i), 0xAA})
		a.State = StateActive
		a.AutoDeactivate = false
		a.CreatedAt = int64(1000 + i)
		a.FiredPrice, a.FiredAt = 0, 0
		if i%2 == 0 {
			a.Symbol = "ETHUSDT"
			a.Direction = engine.DirGTE
		}
		if err := s.Put(a); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		out = append(out, a)
	}
	return out
}

// expectedIndexes derives the exact index keys a record set implies.
func expectedIndexes(alerts []Alert) map[string][]string {
	out := map[string][]string{}
	for i := range alerts {
		a := &alerts[i]
		out["idx_state"] = append(out["idx_state"], string(idxKey(statePrefix(a.State), a.CreatedAt, a.ID)))
		out["idx_symbol"] = append(out["idx_symbol"], string(idxKey([]byte(a.Symbol), a.CreatedAt, a.ID)))
		out["idx_venue"] = append(out["idx_venue"], string(idxKey([]byte(a.Venue), a.CreatedAt, a.ID)))
		out["idx_tier"] = append(out["idx_tier"], string(idxKey([]byte(a.Tier), a.CreatedAt, a.ID)))
		out["idx_direction"] = append(out["idx_direction"], string(idxKey(dirPrefix(a.Direction), a.CreatedAt, a.ID)))
		out["idx_created"] = append(out["idx_created"], string(idxKey(nil, a.CreatedAt, a.ID)))
	}
	return out
}

func TestPutGetDelete(t *testing.T) {
	s, _ := openStore(t)
	put := sampleAlert()

	got, found, err := s.Get(put.ID)
	if err != nil || found || got != (Alert{}) {
		t.Fatalf("Get on empty store = (%v, %v, %v), want zero/false/nil", got, found, err)
	}
	if err := s.Put(put); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, found, err = s.Get(put.ID)
	if err != nil || !found || got != put {
		t.Fatalf("Get = (%v, %v, %v), want (%v, true, nil)", got, found, err, put)
	}

	// Replace with a different state/symbol: old index entries must die.
	replace := put
	replace.State = StateCancelled
	replace.Symbol = "ETHUSDT"
	if err := s.Put(replace); err != nil {
		t.Fatalf("Put replace: %v", err)
	}
	got, _, err = s.Get(put.ID)
	if err != nil || got != replace {
		t.Fatalf("Get after replace = (%v, %v)", got, err)
	}
	snap := s.indexSnapshot(t)
	want := expectedIndexes([]Alert{replace})
	for name, keys := range want {
		if len(snap[name]) != len(keys) {
			t.Fatalf("bucket %s has %d keys, want %d (orphan entries)", name, len(snap[name]), len(keys))
		}
		for _, k := range keys {
			if !contains(snap[name], k) {
				t.Fatalf("bucket %s missing key %x", name, k)
			}
		}
	}

	if err := s.Delete(put.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, found, _ := s.Get(put.ID); found {
		t.Fatal("Get after Delete should miss")
	}
	if snap = s.indexSnapshot(t); len(snap["idx_state"]) != 0 || len(snap["idx_created"]) != 0 {
		t.Fatalf("Delete left index entries behind: %v", snap)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func TestOpenLockedFileTimesOut(t *testing.T) {
	s, path := openStore(t) // holds the flock for the test's lifetime
	_ = s
	if _, err := Open(path); err == nil {
		t.Fatal("second Open on a locked file should time out, not hang")
	}
}
