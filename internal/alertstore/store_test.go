package alertstore

import (
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

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

// TestPutBatch: one tx writes many records with full index maintenance,
// and a replacement member swaps its old index entries exactly like Put.
func TestPutBatch(t *testing.T) {
	// Index buckets come back key-sorted (newest-first via invTS), while
	// expectedIndexes lists in record order — compare as multisets.
	sorted := func(m map[string][]string) map[string][]string {
		out := map[string][]string{}
		for k, v := range m {
			s := append([]string{}, v...)
			sort.Strings(s)
			out[k] = s
		}
		return out
	}
	s, _ := openStore(t)

	batch := make([]Alert, 0, 50)
	for i := range 50 {
		a := sampleAlert()
		copy(a.ID[:], []byte{byte(i), 0xBB})
		a.CreatedAt = int64(2000 + i)
		batch = append(batch, a)
	}
	if err := s.PutBatch(batch); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	for _, want := range batch {
		got, found, err := s.Get(want.ID)
		if err != nil || !found || got != want {
			t.Fatalf("Get %v = (%v, %v, %v), want (%v, true, nil)", want.ID, got, found, err, want)
		}
	}
	if snap, want := sorted(s.indexSnapshot(t)), sorted(expectedIndexes(batch)); !reflect.DeepEqual(snap, want) {
		t.Fatal("indexes after PutBatch do not match the records exactly")
	}

	// Replace half the batch with a different state/symbol in one tx:
	// old index entries must die, not orphan.
	repl := make([]Alert, len(batch)/2)
	for i := range repl {
		repl[i] = batch[i]
		repl[i].State = StateCancelled
		repl[i].Symbol = "ETHUSDT"
	}
	if err := s.PutBatch(repl); err != nil {
		t.Fatalf("PutBatch replace: %v", err)
	}
	full := append(append([]Alert{}, repl...), batch[len(repl):]...)
	if snap, want := sorted(s.indexSnapshot(t)), sorted(expectedIndexes(full)); !reflect.DeepEqual(snap, want) {
		t.Fatal("indexes after PutBatch replace do not match the records exactly")
	}
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
	// Delete must leave every index bucket empty, not just the two we
	// used to spot-check.
	snap = s.indexSnapshot(t)
	for name, keys := range snap {
		if len(keys) != 0 {
			t.Fatalf("bucket %s kept %d entries after Delete: %v", name, len(keys), keys)
		}
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

// TestBatchSkipDoesNotAbortFollowingMembers pins the batch contract: a
// terminal or unknown member is skipped, and the flip continues with the
// remaining members (the old code returned a nil error from the whole
// closure on the first skip, silently dropping every later member).
func TestBatchSkipDoesNotAbortFollowingMembers(t *testing.T) {
	s, _ := openStore(t)
	alerts := putN(t, s, 4)

	// alerts[0] becomes triggered before the cancel batch runs.
	if n, err := s.MarkTriggeredBatch([]Fired{{ID: alerts[0].ID, Price: 1, At: 1}}); err != nil || n != 1 {
		t.Fatalf("setup MarkTriggeredBatch = (%d, %v), want (1, nil)", n, err)
	}
	// Leading triggered member must not stop a2/a3 from flipping.
	n, err := s.CancelBatch([]engine.AlertID{alerts[0].ID, alerts[1].ID, alerts[2].ID})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("CancelBatch flipped %d, want 2 (skip aborted the batch)", n)
	}
	// Leading cancelled member and an unknown member, then one active.
	n, err = s.MarkTriggeredBatch([]Fired{
		{ID: alerts[1].ID, Price: 2, At: 2},         // cancelled: skipped
		{ID: engine.AlertID{0xFF}, Price: 2, At: 2}, // unknown: skipped
		{ID: alerts[3].ID, Price: 2, At: 2},         // active: flips
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("MarkTriggeredBatch flipped %d, want 1 (skip aborted the batch)", n)
	}

	for i, want := range []State{StateTriggered, StateCancelled, StateCancelled, StateTriggered} {
		a, found, err := s.Get(alerts[i].ID)
		if err != nil || !found {
			t.Fatalf("Get[%d] = (%v, %v, %v)", i, a, found, err)
		}
		if a.State != want {
			t.Fatalf("alerts[%d].State = %v, want %v", i, a.State, want)
		}
	}
}

func TestOpenLockedFileTimesOut(t *testing.T) {
	s, path := openStore(t) // holds the flock for the test's lifetime
	_ = s
	if _, err := Open(path); err == nil {
		t.Fatal("second Open on a locked file should time out, not hang")
	}
}

func TestCancelOnlyFromActive(t *testing.T) {
	s, _ := openStore(t)
	alerts := putN(t, s, 3)
	byState := func() map[State]int {
		out := map[State]int{}
		for i := range alerts {
			a, _, err := s.Get(alerts[i].ID)
			if err != nil {
				t.Fatal(err)
			}
			out[a.State]++
		}
		return out
	}
	if flipped, err := s.Cancel(alerts[0].ID); err != nil || !flipped {
		t.Fatalf("Cancel(active) = (%v, %v), want (true, nil)", flipped, err)
	}
	if flipped, err := s.Cancel(alerts[0].ID); err != nil || flipped {
		t.Fatalf("Cancel(cancelled) = (%v, %v), want (false, nil)", flipped, err)
	}
	// Mark triggered, then cancel again: terminal states stay terminal.
	if n, err := s.MarkTriggeredBatch([]Fired{{ID: alerts[1].ID, Price: 1, At: 42}}); err != nil || n != 1 {
		t.Fatalf("MarkTriggeredBatch = (%d, %v), want (1, nil)", n, err)
	}
	if flipped, err := s.Cancel(alerts[1].ID); err != nil || flipped {
		t.Fatalf("Cancel(triggered) = (%v, %v), want (false, nil)", flipped, err)
	}
	if flipped, err := s.Cancel(engine.AlertID{0xFF}); err != nil || flipped {
		t.Fatalf("Cancel(unknown) = (%v, %v), want (false, nil)", flipped, err)
	}
	if got := byState(); got[StateActive] != 1 || got[StateTriggered] != 1 || got[StateCancelled] != 1 {
		t.Fatalf("states = %v, want 1/1/1", got)
	}
}

func TestMarkTriggeredWritesFiredFields(t *testing.T) {
	s, _ := openStore(t)
	alerts := putN(t, s, 1)
	want := Fired{ID: alerts[0].ID, Price: engine.Price(650001000000000), At: 1234567890}
	if n, err := s.MarkTriggeredBatch([]Fired{want}); err != nil || n != 1 {
		t.Fatalf("MarkTriggeredBatch = (%d, %v)", n, err)
	}
	a, found, err := s.Get(alerts[0].ID)
	if err != nil || !found {
		t.Fatalf("Get = (%v, %v, %v)", a, found, err)
	}
	if a.State != StateTriggered || a.FiredPrice != want.Price || a.FiredAt != want.At {
		t.Fatalf("record = state %v fired %v@%v, want triggered %v@%v",
			a.State, a.FiredPrice, a.FiredAt, want.Price, want.At)
	}
	// Index integrity: the active state entry must be gone, the
	// triggered entry present.
	snap := s.indexSnapshot(t)
	hasKey := func(bucket, key string) bool { return contains(snap[bucket], key) }
	if hasKey("idx_state", string(idxKey(statePrefix(StateActive), a.CreatedAt, a.ID))) {
		t.Fatal("active idx_state entry survived the flip")
	}
	if !hasKey("idx_state", string(idxKey(statePrefix(StateTriggered), a.CreatedAt, a.ID))) {
		t.Fatal("triggered idx_state entry missing after the flip")
	}
	if !hasKey("idx_symbol", string(idxKey([]byte(a.Symbol), a.CreatedAt, a.ID))) {
		t.Fatal("symbol index entry must be untouched by a state flip")
	}
}

func TestBatchGet(t *testing.T) {
	s, _ := openStore(t)
	alerts := putN(t, s, 4)
	ids := []engine.AlertID{alerts[3].ID, alerts[1].ID, engine.AlertID{0xEE}} // includes an unknown
	got, err := s.BatchGet(ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("BatchGet returned %d, want 2 (unknown id skipped)", len(got))
	}
	for i := range alerts {
		if i == 0 || i == 2 { // only alerts[1] and alerts[3] were requested
			continue
		}
		if a, ok := got[alerts[i].ID]; !ok || a != alerts[i] {
			t.Fatalf("BatchGet[%d] = (%v, %v)", i, a, ok)
		}
	}
}

// TestConcurrentReadWrite pins the concurrency contract: parallel
// readers (BatchGet) against a churning writer under the race detector.
func TestConcurrentReadWrite(t *testing.T) {
	s, _ := openStore(t)
	alerts := putN(t, s, 64)
	ids := make([]engine.AlertID, len(alerts))
	fired := make([]Fired, len(alerts))
	for i := range alerts {
		ids[i] = alerts[i].ID
		fired[i] = Fired{ID: ids[i], Price: 1, At: 1}
	}
	done := make(chan struct{})
	go func() { // writer: cancel → re-activate a subset → trigger, forever
		for {
			select {
			case <-done:
				return
			default:
			}
			_, _ = s.CancelBatch(ids)
			// Re-Put the even ids as active so the next MarkTriggeredBatch
			// cycle performs real active→triggered flips instead of no-opping
			// on terminal states forever (this Put-replace also exercises the
			// index-swap path under concurrency).
			for i := range alerts {
				if i%2 != 0 {
					continue
				}
				a := alerts[i]
				a.State = StateActive
				if err := s.Put(a); err != nil {
					return
				}
			}
			_, _ = s.MarkTriggeredBatch(fired)
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.BatchGet(ids); err != nil {
			t.Fatalf("BatchGet during writes: %v", err)
		}
	}
	close(done)
}
