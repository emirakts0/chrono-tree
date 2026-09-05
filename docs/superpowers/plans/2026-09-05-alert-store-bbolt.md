# bbolt Alert Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the service-side alert catalog from a RAM map into a persistent bbolt store with per-field secondary indexes, boot-time replay into the engine, bulk pump enrichment by alert ID, and `fired_price`/`fired_at` on the record.

**Architecture:** A new `internal/alertstore` package owns a bbolt file (record bucket + six index buckets, including `idx_created` for unfiltered newest-first walks). `service.Core` swaps its `alerts` map for the store; all state flips are check-and-flip inside bolt's single-writer write tx. `chronod` gains a `-db` flag and replays active alerts into the engine at construction; `chronoctl` gains an offline `compact` subcommand.

**Tech Stack:** Go 1.27, `go.etcd.io/bbolt` **v1.5.0** (latest stable), existing frozen `engine`/`price` packages.

**Spec:** `docs/superpowers/specs/2026-09-05-alert-store-bbolt-design.md`

## Global Constraints

- bbolt pinned at `go.etcd.io/bbolt@v1.5.0`. Never set `NoSync: true` — every commit fsyncs (spec §2 "Durability").
- All writes go through `db.Batch` with **idempotent** transaction functions (bbolt contract: Batch fns may be re-run; opportunistic fsync coalescing is why we use it). All reads go through `db.View`.
- `bolt.Open` always passes `&bolt.Options{Timeout: 5 * time.Second}` so a second chronod holding the flock fails fast instead of hanging.
- Byte slices returned by `Get` are only valid inside the transaction — decode into owned memory (`string(...)` copies) before leaving a `View`/`Batch`.
- `engine/` and `price/` have **zero diffs**. HTTP contracts (`/api/alerts`, `/api/alerts/{id}`), gRPC surface, and the web UI are unchanged (additive `fired_price`/`fired_at` JSON fields allowed).
- States: exactly `active`, `triggered`, `cancelled` (store bytes 1, 2, 3). No new states.
- Modern Go 1.27 idioms (per the go-modern-guidelines list): `for i := range n`, `min`/`max`, `slices.*`, `errors.Is`, typed atomics (`atomic.Int64`), `b.Loop()` in benchmarks, `t.Context()` where a context is needed, `any` not `interface{}`, octal `0o600` literals.
- Comment style: the repo writes dense, *why*-focused comments on non-obvious code (see `engine/alert.go`). Match that density; do not narrate obvious code.
- Commit message style: `feat(scope): ...` / `test(scope): ...` lowercase summary line, blank line, body when non-obvious, end with `Co-Authored-By: Claude Code <noreply@anthropic.com>`.

---

### Task 1: Dependency + record/index key codec

**Files:**
- Modify: `go.mod`, `go.sum` (via `go get`)
- Create: `internal/alertstore/keys.go`
- Test: `internal/alertstore/keys_test.go`

**Interfaces:**
- Consumes: `engine.AlertID`, `engine.PriceType` (`PriceBid`..`PriceLast`), `engine.Direction` (`DirGTE`, `DirLTE`), `engine.Price`
- Produces (used by Tasks 2–5):
  - `type State uint8` with `StateActive`/`StateTriggered`/`StateCancelled` (= 1, 2, 3) and `func (s State) String() string`
  - `type Alert struct` (persisted record)
  - `func decodeAlert(b []byte) (Alert, error)`
  - `func idxKey(prefix []byte, ts int64, id engine.AlertID) []byte`
  - `func idxTail(key []byte, off int) (ts int64, id engine.AlertID, ok bool)`
  - `func statePrefix(s State) []byte`
  - `func dirPrefix(d engine.Direction) []byte`

- [ ] **Step 1: Add the dependency**

```bash
go get go.etcd.io/bbolt@v1.5.0
```

Expected: `go.mod` gains `go.etcd.io/bbolt v1.5.0` and `go mod tidy` leaves a clean diff.

- [ ] **Step 2: Write the failing codec test**

`internal/alertstore/keys_test.go`:

```go
package alertstore

import (
	"bytes"
	"testing"

	"github.com/emir/chrono-tree/engine"
)

func sampleAlert() Alert {
	return Alert{
		ID: engine.AlertID{0x01, 0x02, 0x03}, Symbol: "BTCUSDT", Decimals: 8,
		Venue: "ATLAS", Tier: "TOP",
		PriceType: engine.PriceAsk, Direction: engine.DirLTE,
		TargetPrice: 650001200000000, ValidFrom: 100, Expires: 200,
		State: StateTriggered, AutoDeactivate: true,
		CreatedAt:  1234567890,
		FiredPrice: 650000000000000, FiredAt: 1234567999,
	}
}

func TestAlertCodecRoundTrip(t *testing.T) {
	a := sampleAlert()
	got, err := decodeAlert(appendAlert(make([]byte, 0, 128), &a))
	if err != nil {
		t.Fatalf("decodeAlert: %v", err)
	}
	if got != a {
		t.Fatalf("round trip mismatch:\n got  %+v\n want %+v", got, a)
	}
}

func TestAlertCodecShortRecord(t *testing.T) {
	for n := range recHeadLen {
		if _, err := decodeAlert(bytes.Repeat([]byte{0}, n)); err == nil {
			t.Fatalf("decodeAlert(%d bytes) should fail", n)
		}
	}
	// Truncated string section: head only.
	if _, err := decodeAlert(bytes.Repeat([]byte{0}, recHeadLen)); err == nil {
		t.Fatal("decodeAlert with no string section should fail")
	}
}

func TestIdxKeyOrdering(t *testing.T) {
	older := idxKey(statePrefix(StateActive), 1000, engine.AlertID{1})
	newer := idxKey(statePrefix(StateActive), 2000, engine.AlertID{2})
	if bytes.Compare(older, newer) <= 0 {
		t.Fatal("newer createdAt must sort FIRST (inverted timestamp)")
	}
	ts, id, ok := idxTail(newer, len(statePrefix(StateActive)))
	if !ok || ts != 2000 || id != (engine.AlertID{2}) {
		t.Fatalf("idxTail = (%d, %v, %v), want (2000, 02…, true)", ts, id, ok)
	}
	if _, _, ok := idxTail(append(newer, 0), len(statePrefix(StateActive))); ok {
		t.Fatal("idxTail must reject wrong-length keys")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/alertstore/ -run 'Codec|IdxKey' -v`
Expected: FAIL — `undefined: Alert` (package has no non-test source yet).

- [ ] **Step 4: Write `internal/alertstore/keys.go`**

```go
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/alertstore/ -v`
Expected: PASS (all three tests).

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/alertstore/
git commit -m "feat(alertstore): record codec and secondary-index keys

Fixed-layout record encoding (69-byte head + length-prefixed symbol,
venue, tier) and index keys with inverted createdAt so ascending byte
order is newest-first.

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 2: Store core — Open/Close, Put/Get/Delete with index maintenance

**Files:**
- Create: `internal/alertstore/store.go`
- Test: `internal/alertstore/store_test.go`

**Interfaces:**
- Consumes: Task 1 (`Alert`, `State`, `decodeAlert`, `appendAlert`, `idxKey`, `statePrefix`, `dirPrefix`)
- Produces (used by Tasks 3–6):
  - `type Store struct{ … }`
  - `func Open(path string) (*Store, error)` — creates buckets, fails hard on corrupt/locked files
  - `func (s *Store) Close() error`
  - `func (s *Store) Put(a Alert) error` — replace-safe, maintains all six indexes atomically
  - `func (s *Store) Get(id engine.AlertID) (Alert, bool, error)`
  - `func (s *Store) Delete(id engine.AlertID) error`
  - Bucket names (package-private): `bktAlerts`, `bktIdxState`, `bktIdxSymbol`, `bktIdxVenue`, `bktIdxTier`, `bktIdxDirection`, `bktIdxCreated`

- [ ] **Step 1: Write the failing test**

`internal/alertstore/store_test.go`:

```go
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
	s := openStore(t)
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
```

(The 5 s timeout is the fsync-discipline pin in practice: `Open` passes `Timeout` and never `NoSync`; this test pins the options we *can* observe. `putN` in later tasks uses `s, _ := openStore(t)` for the two-value form.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/alertstore/ -v`
Expected: FAIL — `undefined: Open` (store.go does not exist).

- [ ] **Step 3: Write `internal/alertstore/store.go`**

```go
package alertstore

import (
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/emir/chrono-tree/engine"
)

var (
	bktAlerts      = []byte("alerts")
	bktIdxState    = []byte("idx_state")
	bktIdxSymbol   = []byte("idx_symbol")
	bktIdxVenue    = []byte("idx_venue")
	bktIdxTier     = []byte("idx_tier")
	bktIdxDirection = []byte("idx_direction")
	bktIdxCreated  = []byte("idx_created") // ~createdAt|id over ALL records: the no-filter inquiry walk
	bktAll         = [][]byte{bktAlerts, bktIdxState, bktIdxSymbol, bktIdxVenue, bktIdxTier, bktIdxDirection, bktIdxCreated}
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/alertstore/ -v`
Expected: PASS. (Adjust `openStore` call sites in this task's tests to the two-value form if not already done.)

- [ ] **Step 5: Commit**

```bash
git add internal/alertstore/
git commit -m "feat(alertstore): store core — Open/Put/Get/Delete with atomic index maintenance

Every write maintains the record bucket plus six secondary indexes
(state, symbol, venue, tier, direction, created) in one synced tx;
replacement removes the previous record's index entries first so
re-registered terminal IDs cannot orphan them.

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 3: State flips — Cancel, CancelBatch, MarkTriggeredBatch, BatchGet

**Files:**
- Modify: `internal/alertstore/store.go` (append)
- Test: `internal/alertstore/store_test.go` (append)

**Interfaces:**
- Consumes: Task 2 (`Store`, `putIndexed`, `bktIdxState`, index helpers)
- Produces (used by Tasks 5–6):
  - `func (s *Store) BatchGet(ids []engine.AlertID) (map[engine.AlertID]Alert, error)` — one view tx
  - `func (s *Store) Cancel(id engine.AlertID) (bool, error)` — flips active→cancelled, false for unknown/terminal
  - `func (s *Store) CancelBatch(ids []engine.AlertID) (int, error)` — one write tx, returns flips
  - `type Fired struct { ID engine.AlertID; Price engine.Price; At int64 }`
  - `func (s *Store) MarkTriggeredBatch(fired []Fired) (int, error)` — check-and-flip inside one write tx, writes fired price/time, returns flips

- [ ] **Step 1: Write the failing tests**

Append to `internal/alertstore/store_test.go` (add `"time"` to the file's imports — `TestConcurrentReadWrite` needs it):

```go
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
		if i == 0 || i == 2 { // never requested (ids holds 1 and 3)
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
	for i := range alerts {
		ids[i] = alerts[i].ID
	}
	done := make(chan struct{})
	go func() { // writer: flip everything triggered, then re-Put active, forever
		flip := true
		for {
			select {
			case <-done:
				return
			default:
			}
			if flip {
				fired := make([]Fired, len(ids))
				for i := range ids {
					fired[i] = Fired{ID: ids[i], Price: 1, At: 1}
				}
				_, _ = s.MarkTriggeredBatch(fired)
			} else {
				putN(t, s, 0) // no-op; re-Put path exercised by CancelBatch below
				_, _ = s.CancelBatch(ids)
			}
			flip = !flip
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
```

(The goroutine's error returns are deliberately discarded: the reader contract is "never errors, always consistent", and the writer just churns.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/alertstore/ -run 'Cancel|MarkTriggered|BatchGet|Concurrent' -v`
Expected: FAIL — `s.Cancel undefined`, `s.BatchGet undefined`, `Fired undefined`.

- [ ] **Step 3: Append to `internal/alertstore/store.go`**

```go
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
		flipped = 0 // Batch may re-run this fn; each run's count is the truth
		alerts := tx.Bucket(bktAlerts)
		for _, id := range ids {
			a, ok, err := getInTx(alerts, id)
			if err != nil {
				return err
			}
			if !ok || a.State != StateActive {
				continue // terminal/unknown member is skipped, never aborts the batch
			}
			if err := flipState(tx, &a, StateCancelled, 0, 0); err != nil {
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
		flipped = 0 // Batch may re-run this fn; each run's count is the truth
		alerts := tx.Bucket(bktAlerts)
		for i := range fired {
			f := &fired[i]
			a, ok, err := getInTx(alerts, f.ID)
			if err != nil {
				return err
			}
			if !ok || a.State != StateActive {
				continue // terminal/unknown member is skipped, never aborts the batch
			}
			if err := flipState(tx, &a, StateTriggered, f.Price, f.At); err != nil {
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/alertstore/ -race -v`
Expected: PASS, race detector clean (including `TestConcurrentReadWrite`).

- [ ] **Step 5: Commit**

```bash
git add internal/alertstore/
git commit -m "feat(alertstore): atomic state flips and bulk reads

Cancel/CancelBatch and MarkTriggeredBatch re-read state inside bolt's
single-writer tx — the check-and-flip IS the CAS, no service-side lock
choreography. MarkTriggeredBatch writes fired_price/fired_at in the
same tx; BatchGet serves the pump's per-ring-batch enrichment fetch.

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4: Inquiry — Query via index intersection

**Files:**
- Create: `internal/alertstore/query.go`
- Test: `internal/alertstore/query_test.go`

**Interfaces:**
- Consumes: Tasks 1–3 (`Store`, `idxKey`, `idxTail`, bucket names)
- Produces (used by Task 6): `func (s *Store) Query(f Filter) ([]Alert, int, error)` with

```go
type Filter struct {
	State        State
	HasState     bool
	Symbol       string // "" = unset
	Venue        string
	Tier         string
	Direction    engine.Direction
	HasDirection bool
	Limit, Offset int // Limit <= 0 = no cap; applied after the intersection
}
```

Results are newest-first (createdAt desc, id asc tiebreak); `total` counts the full intersection, not the page.

- [ ] **Step 1: Write the failing tests**

`internal/alertstore/query_test.go`:

```go
package alertstore

import (
	"slices"
	"testing"

	"github.com/emir/chrono-tree/engine"
)

// seedQueryPop writes a deterministic population exercising every index:
// states, symbols, venues, tiers, directions interleave.
func seedQueryPop(t *testing.T, s *Store) []Alert {
	t.Helper()
	symbols := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}
	venues := []string{"ATLAS", "NOVA"}
	tiers := []string{"TOP", "MID"}
	dirs := []engine.Direction{engine.DirGTE, engine.DirLTE}
	states := []State{StateActive, StateTriggered, StateCancelled}
	var pop []Alert
	i := 0
	for _, sym := range symbols {
		for _, venue := range venues {
			for _, tier := range tiers {
				for _, dir := range dirs {
					for _, st := range states {
						a := sampleAlert()
						copy(a.ID[:], []byte{byte(i >> 8), byte(i), 0x55})
						a.Symbol, a.Venue, a.Tier, a.Direction = sym, venue, tier, dir
						a.State = st
						a.CreatedAt = int64(1_000_000 + i) // unique, ordered by i
						if st == StateTriggered {
							a.FiredPrice, a.FiredAt = engine.Price(7), int64(i)
						}
						if err := s.Put(a); err != nil {
							t.Fatalf("Put %d: %v", i, err)
						}
						pop = append(pop, a)
						i++
					}
				}
			}
		}
	}
	return pop
}

// bruteForce is the oracle: filter + sort + paginate in memory.
func bruteForce(pop []Alert, f Filter) ([]Alert, int) {
	match := func(a Alert) bool {
		return (!f.HasState || a.State == f.State) &&
			(f.Symbol == "" || a.Symbol == f.Symbol) &&
			(f.Venue == "" || a.Venue == f.Venue) &&
			(f.Tier == "" || a.Tier == f.Tier) &&
			(!f.HasDirection || a.Direction == f.Direction)
	}
	var hits []Alert
	for _, a := range pop {
		if match(a) {
			hits = append(hits, a)
		}
	}
	slices.SortStableFunc(hits, func(x, y Alert) int {
		if x.CreatedAt != y.CreatedAt {
			return int(y.CreatedAt - x.CreatedAt) // newest first
		}
		return int(x.ID[1]) - int(y.ID[1]) // insertion order tiebreak
	})
	total := len(hits)
	start := min(max(f.Offset, 0), total)
	end := total
	if f.Limit > 0 {
		end = min(start+f.Limit, total)
	}
	return hits[start:end], total
}

func TestQueryOracle(t *testing.T) {
	s, _ := openStore(t)
	pop := seedQueryPop(t, s)
	filters := []Filter{
		{},
		{HasState: true, State: StateTriggered},
		{Symbol: "BTCUSDT"},
		{Venue: "NOVA"},
		{Tier: "MID"},
		{HasDirection: true, Direction: engine.DirLTE},
		{HasState: true, State: StateActive, Symbol: "ETHUSDT"},
		{HasState: true, State: StateTriggered, Venue: "ATLAS", Tier: "TOP"},
		{Symbol: "SOLUSDT", Venue: "NOVA", Tier: "MID", HasDirection: true, Direction: engine.DirGTE},
		{HasState: true, State: StateActive, Symbol: "SOLUSDT", Venue: "ATLAS", Tier: "MID", HasDirection: true, Direction: engine.DirLTE},
		{Symbol: "NOSUCH"}, // empty result
	}
	for _, f := range filters {
		for _, pg := range []Filter{
			f,
			{State: f.State, HasState: f.HasState, Symbol: f.Symbol, Venue: f.Venue, Tier: f.Tier, Direction: f.Direction, HasDirection: f.HasDirection, Limit: 3},
			{State: f.State, HasState: f.HasState, Symbol: f.Symbol, Venue: f.Venue, Tier: f.Tier, Direction: f.Direction, HasDirection: f.HasDirection, Limit: 3, Offset: 4},
		} {
			got, total, err := s.Query(pg)
			if err != nil {
				t.Fatalf("Query(%+v): %v", pg, err)
			}
			want, wantTotal := bruteForce(pop, pg)
			if total != wantTotal {
				t.Fatalf("Query(%+v) total = %d, want %d", pg, total, wantTotal)
			}
			if len(got) != len(want) {
				t.Fatalf("Query(%+v) page len = %d, want %d", pg, len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("Query(%+v)[%d] = %v, want %v", pg, i, got[i], want[i])
				}
			}
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/alertstore/ -run QueryOracle -v`
Expected: FAIL — `s.Query undefined`.

- [ ] **Step 3: Write `internal/alertstore/query.go`**

```go
package alertstore

import (
	"bytes"

	bolt "go.etcd.io/bbolt"

	"github.com/emir/chrono-tree/engine"
)

// Filter selects alerts for the inquiry API. Empty strings and false
// Has* flags match anything. Limit/Offset apply after the intersection;
// Limit <= 0 means no cap. Results are newest-first.
type Filter struct {
	State         State
	HasState      bool
	Symbol        string
	Venue         string
	Tier          string
	Direction     engine.Direction
	HasDirection  bool
	Limit, Offset int
}

type idxPred struct {
	bkt    []byte
	prefix []byte
}

// Query intersects the set filters' index ranges and returns one
// materialized page plus the full match count. The lead range is walked
// newest-first (inverted-timestamp keys); each candidate is verified
// against the remaining indexes by exact key seek — bolt seeks are
// O(log n), so the query touches candidates plus a few tree descents,
// never a full scan.
func (s *Store) Query(f Filter) ([]Alert, int, error) {
	var preds []idxPred
	if f.HasState {
		preds = append(preds, idxPred{bktIdxState, statePrefix(f.State)})
	}
	if f.Symbol != "" {
		preds = append(preds, idxPred{bktIdxSymbol, []byte(f.Symbol)})
	}
	if f.Venue != "" {
		preds = append(preds, idxPred{bktIdxVenue, []byte(f.Venue)})
	}
	if f.Tier != "" {
		preds = append(preds, idxPred{bktIdxTier, []byte(f.Tier)})
	}
	if f.HasDirection {
		preds = append(preds, idxPred{bktIdxDirection, dirPrefix(f.Direction)})
	}
	if len(preds) == 0 {
		preds = append(preds, idxPred{bktIdxCreated, nil}) // no filter: all, newest-first
	}
	lead, rest := preds[0], preds[1:] // state first by construction: the UI's dominant filter

	var out []Alert
	total := 0
	err := s.db.View(func(tx *bolt.Tx) error {
		var page []engine.AlertID
		c := tx.Bucket(lead.bkt).Cursor()
		for k, _ := c.Seek(lead.prefix); k != nil && bytes.HasPrefix(k, lead.prefix); k, _ = c.Next() {
			ts, id, ok := idxTail(k, len(lead.prefix))
			if !ok {
				continue
			}
			if !matchesRest(tx, rest, ts, id) {
				continue
			}
			total++
			start := max(f.Offset, 0)
			if f.Limit <= 0 || (total > start && total <= start+f.Limit) {
				page = append(page, id)
			}
		}
		if len(page) == 0 {
			return nil
		}
		// Materialize the page in the same tx: keys came out sorted
		// newest-first, so the page is already ordered.
		alerts := tx.Bucket(bktAlerts)
		out = make([]Alert, 0, len(page))
		for _, id := range page {
			raw := alerts.Get(id[:])
			if raw == nil {
				continue // index/record skew is impossible; tolerated anyway
			}
			a, err := decodeAlert(raw)
			if err != nil {
				return err
			}
			out = append(out, a)
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

// matchesRest verifies a candidate against the non-lead indexes by
// exact key existence — the index key embeds createdAt, so the seek is
// precise, not a prefix test.
func matchesRest(tx *bolt.Tx, preds []idxPred, ts int64, id engine.AlertID) bool {
	for _, p := range preds {
		if tx.Bucket(p.bkt).Get(idxKey(p.prefix, ts, id)) == nil {
			return false
		}
	}
	return true
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/alertstore/ -race -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/alertstore/
git commit -m "feat(alertstore): index-intersection inquiry queries

Query walks the lead index range newest-first (state when set) and
verifies candidates against the remaining indexes by exact key seek;
only the page's records are materialized. total counts the full
intersection. Verified against a brute-force oracle across all single
filters, pairings, and the full conjunction.

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 5: Replay — EachActive + Counts

**Files:**
- Create: `internal/alertstore/replay.go`
- Test: `internal/alertstore/replay_test.go`

**Interfaces:**
- Consumes: Tasks 1–3 (`Store`, index helpers)
- Produces (used by Task 6):
  - `func (s *Store) EachActive(fn func(Alert) error) error` — newest-first, one read tx; `fn` returning an error stops the walk and is returned
  - `func (s *Store) Counts() (active, triggered, cancelled int, err error)` — one read tx over `idx_state` prefixes

- [ ] **Step 1: Write the failing test**

`internal/alertstore/replay_test.go`:

```go
package alertstore

import (
	"testing"

	"github.com/emir/chrono-tree/engine"
)

func TestEachActiveAndCounts(t *testing.T) {
	s, _ := openStore(t)
	alerts := putN(t, s, 6)

	// Flip half: 2 triggered, 1 cancelled, 3 active.
	fired := []Fired{{ID: alerts[0].ID, Price: 1, At: 1}, {ID: alerts[1].ID, Price: 1, At: 1}}
	if n, err := s.MarkTriggeredBatch(fired); err != nil || n != 2 {
		t.Fatalf("MarkTriggeredBatch = (%d, %v)", n, err)
	}
	if n, err := s.CancelBatch([]engine.AlertID{alerts[2].ID}); err != nil || n != 1 {
		t.Fatalf("CancelBatch = (%d, %v)", n, err)
	}

	var act int
	if a, tr, c, err := s.Counts(); err != nil || a != 3 || tr != 2 || c != 1 {
		t.Fatalf("Counts = (%d, %d, %d, %v), want (3, 2, 1, nil)", a, tr, c, err)
	}

	// EachActive yields only the active set, newest-first.
	var seen []engine.AlertID
	err := s.EachActive(func(a Alert) error {
		if a.State != StateActive {
			t.Fatalf("EachActive yielded state %v", a.State)
		}
		seen = append(seen, a.ID)
		act++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if act != 3 {
		t.Fatalf("EachActive yielded %d, want 3", act)
	}
	for _, want := range []engine.AlertID{alerts[5].ID, alerts[4].ID, alerts[3].ID} {
		if len(seen) == 0 || seen[0] != want {
			t.Fatalf("EachActive order = %v, want newest-first starting at %v", seen, want)
		}
		seen = seen[1:]
	}
}

func TestReplayAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/alerts.bbolt"
	s1, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	pop := seedQueryPop(t, s1) // mixed-state population
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	var wantActive int
	for _, a := range pop {
		if a.State == StateActive {
			wantActive++
		}
	}
	got := 0
	if err := s2.EachActive(func(Alert) error { got++; return nil }); err != nil {
		t.Fatal(err)
	}
	if got != wantActive {
		t.Fatalf("active after restart = %d, want %d", got, wantActive)
	}
	items, total, err := s2.Query(Filter{HasState: true, State: StateTriggered})
	if err != nil {
		t.Fatal(err)
	}
	_ = items
	if total == 0 {
		t.Fatal("triggered history must survive restart")
	}
}
```

(`putN` lives in `store_test.go`, `seedQueryPop` in `query_test.go` — same package, shared freely.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/alertstore/ -run 'EachActive|Replay' -v`
Expected: FAIL — `s.EachActive undefined`, `s.Counts undefined`.

- [ ] **Step 3: Write `internal/alertstore/replay.go`**

```go
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/alertstore/ -race -v`
Expected: PASS (whole package).

- [ ] **Step 5: Commit**

```bash
git add internal/alertstore/
git commit -m "feat(alertstore): boot replay cursor and state counts

EachActive walks the active index newest-first in one read tx; Counts
tallies state index entries for the /stats gauges at boot.

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 6: service.Core rewrite — store-backed catalog, replay, bulk pump

**Files:**
- Modify: `internal/service/core.go` (major rewrite of the catalog parts)
- Modify: `internal/service/errors.go` (add `internalf`)
- Modify: `internal/service/testenv_test.go` (store-backed env)
- Modify: `internal/service/alerts_test.go` (`TestAlertsByStateIncremental.scan`)
- Modify: `internal/server/server_test.go` (three `NewCore` call sites)
- Modify: `cmd/chronoctl/seed_test.go` (`NewCore` call site)
- Test: `internal/service/replay_test.go` (new)

**Interfaces:**
- Consumes: Tasks 1–5 (`alertstore.Store` with `Put/Get/Delete/BatchGet/Cancel/CancelBatch/MarkTriggeredBatch/Query/EachActive/Counts`, `alertstore.Alert`, `alertstore.State`, `alertstore.Fired`, `alertstore.Filter`)
- Produces (used by Tasks 7–8 and existing callers):
  - `func NewCore(cfg engine.Config, cat *catalog.Catalog, m Metrics, st *stats.Stats, p pub.Publisher, store *alertstore.Store) *Core` — **new parameter**; panics if `store == nil` (construction-order fact, like the catalog)
  - `Core.Close`, `Core.AlertCount`, `Core.AlertsByState`, `Core.GetAlert`, `Core.ListAlerts`, `Core.Engine`, `Core.NATSConnected` — signatures unchanged
  - `AlertView` gains `FiredPrice string \`json:"fired_price"\`` and `FiredAtUnixNanos int64 \`json:"fired_at_unix_nanos"\``

- [ ] **Step 1: Write the failing restart test**

`internal/service/replay_test.go`:

```go
package service

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/alertstore"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/pub"
	"github.com/emir/chrono-tree/internal/stats"
)

// newEnvAtPath builds a bufconn env backed by a store at an explicit
// path — the restart story: close, reopen the same file, replay.
func newEnvAtPath(t *testing.T, path string) *testEnv {
	t.Helper()
	rec := &recordingPub{}
	cat := catalog.Default()
	store, err := alertstore.Open(path)
	if err != nil {
		t.Fatalf("alertstore.Open: %v", err)
	}
	core := NewCore(engine.DefaultConfig(), cat, NoopMetrics{}, stats.New(time.Now()), rec, store)
	t.Cleanup(func() {
		core.Close()
		_ = store.Close()
	})
	return newEnvWithCore(t, core, rec)
}

func TestRestartReplayAndFire(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alerts.bbolt")
	e := newEnvAtPath(t, path)

	live := validUpsert() // BTCUSDT ask above 65000
	liveResp, err := e.alerts().UpsertAlert(context.Background(), live)
	if err != nil {
		t.Fatal(err)
	}
	expired := validUpsert()
	expired.Symbol = "ETHUSDT"
	expired.ExpiresUnixNanos = time.Now().Add(-time.Hour).UnixNano() // expired before it was ever seen again
	if _, err := e.alerts().UpsertAlert(context.Background(), expired); err != nil {
		t.Fatal(err)
	}
	cancelled := validUpsert()
	cancelled.Symbol = "SOLUSDT"
	cancelledResp, err := e.alerts().UpsertAlert(context.Background(), cancelled)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.alerts().CancelAlert(context.Background(), &chronov1.CancelAlertRequest{AlertId: cancelledResp.GetAlertId()}); err != nil {
		t.Fatal(err)
	}

	// Shutdown: pump drained, engine closed, store closed.
	e.srv.Stop()
	e.core.Close()

	e2 := newEnvAtPath(t, path)
	defer e2.srv.Stop()
	if got := e2.core.Engine().Stats().Live; got != 1 {
		t.Fatalf("engine live after replay = %d, want 1 (live alert only)", got)
	}
	if byState := e2.core.AlertsByState(); byState["active"] != 1 || byState["cancelled"] != 2 {
		t.Fatalf("states after replay = %v, want 1 active / 2 cancelled", byState)
	}

	// The replayed alert still fires, and fired fields land on the record.
	if _, err := runTicks(t, e2, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65001.00", "65002.00", "ATLAS", "TOP"),
	}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if e2.core.AlertsByState()["triggered"] == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	view, ok := e2.core.GetAlert(liveResp.GetAlertId())
	if !ok {
		t.Fatal("replayed alert missing after fire")
	}
	if view.State != "triggered" || view.FiredPrice == "" || view.FiredAtUnixNanos == 0 {
		t.Fatalf("view = %+v, want triggered with fired price/at", view)
	}
	if view.FiredPrice != "65002.00" { // the ask it fired on (tick's 3rd field)
		t.Fatalf("fired price = %q, want 65002.00", view.FiredPrice)
	}
}
```

Notes for implementers:
- `runTicks` and `tick` (signature `tick(symbol, bid, ask, venue, tier)`) already exist in `feed_test.go` — reuse them; the alert is ASK/ABOVE 65000, the tick's ask is 65002.00, so that is the fired price.
- `newEnvWithCore(t, core, rec)` does not exist yet: factor it out of `newEnvWithPub` in Step 4 (bufconn wiring around an already-built core), so both the normal and restart envs share it.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/service/ -run TestRestartReplayAndFire -v`
Expected: FAIL — compile errors (`NewCore` arity, `newEnvAtPath` undefined, `FiredPrice` unknown field).

- [ ] **Step 3: Rewrite `internal/service/core.go`**

Apply these exact changes (everything else in the file stays byte-identical):

**3a.** Add to `internal/service/errors.go`:

```go
// internalf builds an Internal status — the store broke.
func internalf(format string, args ...any) error {
	return status.Errorf(codes.Internal, format, args...)
}
```

**3b.** Imports in `core.go`: add `"github.com/emir/chrono-tree/internal/alertstore"`; remove `"cmp"`, `"slices"`, `"strings"` once the rewrite below compiles without them (ListAlerts no longer sorts).

**3c.** Replace the `AlertState` constants and `Alert` struct (lines ~57–82) with a thin mapping onto the store:

```go
// AlertState is the service-side lifecycle view (string form for the
// monitoring surface; storage uses alertstore.State).
type AlertState = string

const (
	StateActive    AlertState = "active"
	StateTriggered AlertState = "triggered"
	StateCancelled AlertState = "cancelled"
)

// storeState maps the wire/monitoring state name to the store state.
func storeState(s string) (alertstore.State, bool) {
	switch s {
	case StateActive:
		return alertstore.StateActive, true
	case StateTriggered:
		return alertstore.StateTriggered, true
	case StateCancelled:
		return alertstore.StateCancelled, true
	}
	return 0, false
}
```

(Delete the old `type AlertState string` block and the old `Alert` struct entirely.)

**3d.** Replace the `Core` struct fields `mu`, `alerts`, `stateCounts` with:

```go
	store *alertstore.Store

	// State gauges for /stats and Prometheus — tallies, not a log:
	// maintained alongside store writes, never queried from here.
	active    atomic.Int64
	triggered atomic.Int64
	cancelled atomic.Int64
```

**3e.** Replace `NewCore` (lines ~115–141) with:

```go
// NewCore builds the engine with the catalog's dim vocabulary, replays
// the store's active set into it, and starts the trigger pump. The
// publisher receives every enriched trigger; nil falls back to pub.Noop
// (a nil would otherwise panic at the first deliver, far from the
// cause). A nil store is a programming error — same class as a nil
// catalog — and panics here, before any listener exists.
func NewCore(cfg engine.Config, cat *catalog.Catalog, m Metrics, st *stats.Stats, p pub.Publisher, store *alertstore.Store) *Core {
	if p == nil {
		p = pub.Noop{}
	}
	if store == nil {
		panic("service: alertstore is required")
	}
	cfg.Dims = cat.Dims() // the catalog owns the vocabulary
	c := &Core{
		Cat:     cat,
		eng:     engine.New(cfg),
		store:   store,
		metrics: m,
		stats:   st,
		pub:     p,
		now:     func() time.Time { return time.Now() },
	}
	// Healthy until a publish fails: "recovered" must only ever log after
	// an actual failure, never on the process's first publish.
	c.pubHealthy.Store(true)
	c.venueTicks = make(map[string]*atomic.Uint64, len(cat.DimValues(catalog.DimVenue)))
	for _, v := range cat.DimValues(catalog.DimVenue) {
		c.venueTicks[v] = &atomic.Uint64{}
	}
	c.replay()
	c.pumpCtx, c.pumpCancel = context.WithCancel(context.Background())
	c.pumpDone = make(chan struct{})
	go c.pump()
	return c
}
```

**3f.** Add `replay` and `specFrom` after `NewCore`:

```go
// replay restores the persisted active set into the fresh engine:
// alerts that expired while down flip to cancelled in one batched write;
// the rest re-enter the engine (valid_from still applies — the engine
// gates matching on it). Runs before the pump exists, so nothing can
// fire mid-replay. A store that cannot be read is fatal at construction:
// source of truth or nothing.
func (c *Core) replay() {
	now := c.now()
	var restored, expired, skipped int
	var expiredIDs []engine.AlertID
	err := c.store.EachActive(func(a alertstore.Alert) error {
		if a.Expires != 0 && a.Expires <= now.UnixNano() {
			expired++
			expiredIDs = append(expiredIDs, a.ID)
			return nil
		}
		spec, ok := c.specFrom(a)
		if !ok {
			skipped++ // catalog drifted from the record; loud count, not a boot failure
			return nil
		}
		if err := c.eng.Upsert(spec); err != nil {
			return fmt.Errorf("replay upsert %s: %w", alertIDString(a.ID), err)
		}
		restored++
		return nil
	})
	if err != nil {
		panic(fmt.Sprintf("service: alert store replay: %v", err))
	}
	if len(expiredIDs) > 0 {
		if _, err := c.store.CancelBatch(expiredIDs); err != nil {
			panic(fmt.Sprintf("service: expire while replaying: %v", err))
		}
	}
	c.eng.Sync()
	if a, tr, cn, err := c.store.Counts(); err != nil {
		panic(fmt.Sprintf("service: alert store counts: %v", err))
	} else {
		c.active.Store(int64(a))
		c.triggered.Store(int64(tr))
		c.cancelled.Store(int64(cn))
	}
	slog.Info("alert store replay",
		"restored", restored, "expired", expired, "skipped", skipped)
}

// specFrom rebuilds the engine submission for a persisted record.
// Dims are re-interned through the catalog (the engine owns uint16
// values; the store owns names).
func (c *Core) specFrom(a alertstore.Alert) (engine.AlertSpec, bool) {
	sym, ok := c.Cat.Symbol(a.Symbol)
	if !ok {
		return engine.AlertSpec{}, false
	}
	vv, ok := c.Cat.Value(catalog.DimVenue, a.Venue)
	if !ok {
		return engine.AlertSpec{}, false
	}
	tv, ok := c.Cat.Value(catalog.DimTier, a.Tier)
	if !ok {
		return engine.AlertSpec{}, false
	}
	return engine.AlertSpec{
		ID: a.ID, Symbol: sym.Name, PriceType: a.PriceType, Direction: a.Direction,
		TargetPrice:    a.TargetPrice,
		ValidFrom:      a.ValidFrom,
		Expires:        a.Expires,
		AutoDeactivate: a.AutoDeactivate,
		Dims:           engine.Dims(vv, tv),
		Meta: engine.AlertMeta{
			ID: a.ID, Symbol: sym.Name, PriceType: a.PriceType, Direction: a.Direction,
			TargetPrice: a.TargetPrice, CreatedAt: a.CreatedAt,
		},
	}, true
}
```

**3g.** Replace `AlertCount`/`AlertsByState`/`countState` (lines ~154–170, ~290–297) with:

```go
func (c *Core) AlertCount() int {
	return int(c.active.Load() + c.triggered.Load() + c.cancelled.Load())
}

func (c *Core) AlertsByState() map[string]int {
	out := map[string]int{}
	for s, n := range map[string]*atomic.Int64{
		StateActive: &c.active, StateTriggered: &c.triggered, StateCancelled: &c.cancelled,
	} {
		if v := n.Load(); v > 0 {
			out[s] = int(v)
		}
	}
	return out
}
```

(Delete `countState`; `c.metrics.AlertsActive(c.countState(StateActive))` call sites become `c.metrics.AlertsActive(int(c.active.Load()))`.)

**3h.** Rewrite the persistence tail of `UpsertAlert` (from the `// Service catalog first` comment through the `mapEngineErr` return, lines ~241–284) as:

```go
	// Store first (pump enrichment must never miss), remembering the
	// previous record so an engine failure restores it.
	prev, found, err := c.store.Get(id)
	if err != nil {
		return nil, internalf("alert store: %v", err)
	}
	rec := alertstore.Alert{
		ID: id, Symbol: sym.Name, Decimals: sym.Decimals,
		Venue: req.GetVenue(), Tier: req.GetTier(),
		PriceType: pt, Direction: dir, TargetPrice: engine.Price(base),
		ValidFrom: req.GetValidFromUnixNanos(), Expires: req.GetExpiresUnixNanos(),
		State: alertstore.StateActive, AutoDeactivate: req.GetAutoDeactivate(),
		CreatedAt: c.now().UnixNano(),
	}
	if err := c.store.Put(rec); err != nil {
		return nil, internalf("alert store: %v", err)
	}

	spec := engine.AlertSpec{
		ID: id, Symbol: sym.Name, PriceType: pt, Direction: dir,
		TargetPrice:    engine.Price(base),
		ValidFrom:      req.GetValidFromUnixNanos(),
		Expires:        req.GetExpiresUnixNanos(),
		AutoDeactivate: req.GetAutoDeactivate(),
		Dims:           engine.Dims(vv, tv),
		Meta: engine.AlertMeta{
			ID: id, Symbol: sym.Name, PriceType: pt, Direction: dir,
			TargetPrice: engine.Price(base), CreatedAt: rec.CreatedAt,
		},
	}
	if err := c.eng.Upsert(spec); err != nil {
		// Roll the store back to the pre-upsert truth.
		if found {
			if err2 := c.store.Put(prev); err2 != nil {
				slog.Error("alert store rollback failed", "id", alertIDString(id), "err", err2)
			}
		} else if err2 := c.store.Delete(id); err2 != nil {
			slog.Error("alert store rollback failed", "id", alertIDString(id), "err", err2)
		}
		return nil, mapEngineErr(err)
	}
	c.eng.Sync()
	if found {
		c.decState(prev.State)
	}
	c.active.Add(1)
	c.metrics.AlertsActive(int(c.active.Load()))
	return &chronov1.UpsertAlertResponse{AlertId: alertIDString(id)}, nil
```

And add the gauge helper next to `storeState`:

```go
// decState decrements the gauge for a state the catalog left.
func (c *Core) decState(s alertstore.State) {
	switch s {
	case alertstore.StateActive:
		c.active.Add(-1)
	case alertstore.StateTriggered:
		c.triggered.Add(-1)
	case alertstore.StateCancelled:
		c.cancelled.Add(-1)
	}
}
```

**3i.** Rewrite `CancelAlert` (lines ~301–318):

```go
// CancelAlert retires an alert: the store flips active→cancelled, then
// the engine drops its refs. Terminal/unknown alerts answer NotFound.
// A trigger racing the cancel loses the state flip (MarkTriggered sees
// a terminal record and skips) — the engine may still publish one
// in-flight trigger, same observable behavior as the map era.
func (c *Core) CancelAlert(ctx context.Context, req *chronov1.CancelAlertRequest) (*chronov1.CancelAlertResponse, error) {
	id, err := parseAlertID(req.GetAlertId())
	if err != nil {
		return nil, invalidf("%v", err)
	}
	flipped, err := c.store.Cancel(id)
	if err != nil {
		return nil, internalf("alert store: %v", err)
	}
	if !flipped {
		return nil, status.Error(codes.NotFound, "alert not found")
	}
	if err := c.eng.Cancel(id); err != nil &&
		!errors.Is(err, engine.ErrNotFound) && !errors.Is(err, engine.ErrInvalidTransition) {
		return nil, mapEngineErr(err) // fired-and-removed meanwhile: expected, ignored above
	}
	c.active.Add(-1)
	c.cancelled.Add(1)
	c.metrics.AlertsActive(int(c.active.Load()))
	return &chronov1.CancelAlertResponse{}, nil
}
```

(`codes` and `status` are already imported in `core.go` — no import changes.)

**3j.** Rewrite the pump and `deliver` (lines ~446–518):

```go
// pump drains the engine's trigger ring and hands each batch to
// deliverBatch. It is the ONLY Triggers() consumer.
func (c *Core) pump() {
	defer close(c.pumpDone)
	buf := make([]engine.Trigger, 64)
	for {
		n := c.eng.Triggers().PopBatch(buf)
		if n == 0 {
			select {
			case <-c.pumpCtx.Done():
				return
			case <-time.After(500 * time.Microsecond):
			}
			continue
		}
		c.deliverBatch(buf[:n])
	}
}

// deliverBatch enriches a drained ring batch in bulk: one read tx
// resolves every alert, each active alert's trigger is published, then
// ONE write tx flips the published ones to triggered (writing
// fired_price/fired_at). A store failure at either end leaves the
// alerts active — they re-fire on a later tick: visible duplication
// beats silent loss.
func (c *Core) deliverBatch(batch []engine.Trigger) {
	ids := make([]engine.AlertID, len(batch))
	for i := range batch {
		ids[i] = batch[i].ID
	}
	recs, err := c.store.BatchGet(ids)
	now := c.now()
	if err != nil {
		for range batch {
			c.stats.TriggersFired.Add(1)
		}
		slog.Warn("trigger enrichment failed; batch lost, alerts stay active", "err", err)
		return
	}
	var fired []alertstore.Fired
	for i := range batch {
		tr := &batch[i]
		c.stats.TriggersFired.Add(1)
		c.stats.FireRate.Add(1, now)
		a, ok := recs[tr.ID]
		if !ok || a.State != alertstore.StateActive {
			continue // unknown or no-longer-active: count, don't ship
		}
		out := pub.Trigger{
			AlertID:          alertIDString(tr.ID),
			Symbol:           a.Symbol,
			Venue:            a.Venue,
			Tier:             a.Tier,
			FiredPrice:       price.Format(int64(tr.Price), a.Decimals),
			FiredAtUnixNanos: tr.TS,
			Direction:        directionOf(a.Direction),
			TargetPrice:      price.Format(int64(a.TargetPrice), a.Decimals),
		}
		c.metrics.TriggerFired(a.Symbol, a.Venue, a.Tier)
		if err := c.pub.Publish(out); err != nil {
			// Drop-and-count: the pump never blocks, never retries (spec §4).
			c.stats.TriggersPublishDropped.Add(1)
			c.metrics.TriggerPublishDropped()
			if c.pubHealthy.CompareAndSwap(true, false) {
				slog.Warn("trigger publish failing; dropping until NATS recovers", "err", err)
			}
			continue
		}
		if !c.pubHealthy.Swap(true) {
			slog.Info("trigger publishing recovered")
		}
		c.stats.TriggersPublished.Add(1)
		c.metrics.TriggerPublished()
		fired = append(fired, alertstore.Fired{ID: a.ID, Price: engine.Price(tr.Price), At: tr.TS})
	}
	if len(fired) == 0 {
		return
	}
	flipped, err := c.store.MarkTriggeredBatch(fired)
	if err != nil {
		slog.Warn("mark triggered failed; alerts stay active and may re-fire", "err", err)
		return
	}
	c.active.Add(-int64(flipped))
	c.triggered.Add(int64(flipped))
}
```

**3k.** Extend `AlertView` and replace `view`/`GetAlert`/`ListAlerts` (lines ~533–653):

```go
// AlertView is the read-only shape of a stored alert for the monitoring
// surface. Prices are decimal strings, ready to render.
type AlertView struct {
	ID                 string `json:"id"`
	Symbol             string `json:"symbol"`
	Venue              string `json:"venue"`
	Tier               string `json:"tier"`
	PriceType          string `json:"price_type"`
	Direction          string `json:"direction"`
	TargetPrice        string `json:"target_price"`
	State              string `json:"state"`
	ValidFromUnixNanos int64  `json:"valid_from_unix_nanos"`
	ExpiresUnixNanos   int64  `json:"expires_unix_nanos"`
	CreatedAtUnixNanos int64  `json:"created_at_unix_nanos"`
	FiredPrice         string `json:"fired_price"`        // "" until triggered
	FiredAtUnixNanos   int64  `json:"fired_at_unix_nanos"` // 0 until triggered
}
```

```go
func viewOf(a *alertstore.Alert) AlertView {
	return AlertView{
		ID:                 alertIDString(a.ID),
		Symbol:             a.Symbol,
		Venue:              a.Venue,
		Tier:               a.Tier,
		PriceType:          priceTypeString(a.PriceType),
		Direction:          directionOf(a.Direction),
		TargetPrice:        price.Format(int64(a.TargetPrice), a.Decimals),
		State:              a.State.String(),
		ValidFromUnixNanos: a.ValidFrom,
		ExpiresUnixNanos:   a.Expires,
		CreatedAtUnixNanos: a.CreatedAt,
		FiredPrice:         firedPriceString(a),
		FiredAtUnixNanos:   a.FiredAt,
	}
}

func firedPriceString(a *alertstore.Alert) string {
	if a.State != alertstore.StateTriggered {
		return ""
	}
	return price.Format(int64(a.FiredPrice), a.Decimals)
}

// GetAlert resolves one alert by its string id (the form UpsertAlert
// returned). ok is false for unknown or malformed ids.
func (c *Core) GetAlert(id string) (AlertView, bool) {
	raw, err := parseAlertID(id)
	if err != nil {
		return AlertView{}, false
	}
	a, found, err := c.store.Get(raw)
	if err != nil || !found {
		return AlertView{}, false
	}
	return viewOf(&a), true
}

// ListAlerts returns one page of the filtered catalog, newest first,
// plus the total match count. Unknown state/direction strings match
// nothing (empty page), preserving the previous contract.
func (c *Core) ListAlerts(f AlertFilter) ([]AlertView, int) {
	sf := alertstore.Filter{
		Symbol: f.Symbol, Venue: f.Venue, Tier: f.Tier,
		Limit: f.Limit, Offset: f.Offset,
	}
	if f.State != "" {
		st, ok := storeState(f.State)
		if !ok {
			return nil, 0
		}
		sf.State, sf.HasState = st, true
	}
	switch f.Direction {
	case "":
	case "ABOVE":
		sf.Direction, sf.HasDirection = engine.DirGTE, true
	case "BELOW":
		sf.Direction, sf.HasDirection = engine.DirLTE, true
	default:
		return nil, 0
	}
	items, total, err := c.store.Query(sf)
	if err != nil {
		slog.Error("alert query failed", "err", err)
		return nil, 0
	}
	views := make([]AlertView, 0, len(items))
	for i := range items {
		views = append(views, viewOf(&items[i]))
	}
	return views, total
}
```

(Delete the old `func (a *Alert) view()`, the old `GetAlert` body that read `c.alerts`, and the old `ListAlerts` scan+sort body. `AlertFilter` itself is unchanged.)

- [ ] **Step 4: Update the test environments**

`internal/service/testenv_test.go` — factor the bufconn wiring and back the default env with a temp-file store:

```go
import (
	// add:
	"path/filepath"

	"github.com/emir/chrono-tree/internal/alertstore"
)

// openStore opens a throwaway store for one test.
func openStore(t *testing.T) *alertstore.Store {
	t.Helper()
	s, err := alertstore.Open(filepath.Join(t.TempDir(), "alerts.bbolt"))
	if err != nil {
		t.Fatalf("alertstore.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
```

Replace the `newEnvWithPub` body's `NewCore` line with:

```go
	core := NewCore(engine.DefaultConfig(), cat, NoopMetrics{}, stats.New(time.Now()), p, openStore(t))
```

and split out the wiring (used by `replay_test.go`'s `newEnvAtPath`):

```go
// newEnvWithCore wires bufconn gRPC around an already-built core and
// registers the same cleanups as newEnvWithPub (minus core/store, which
// the caller owns).
func newEnvWithCore(t *testing.T, core *Core, p pub.Publisher) *testEnv {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(NewValidateInterceptor()))
	chronov1.RegisterAlertServiceServer(srv, core)
	chronov1.RegisterFeedServiceServer(srv, core)
	go func() { _ = srv.Serve(lis) }()
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() {
		_ = cc.Close()
		srv.Stop()
	})
	var rec *recordingPub
	if r, ok := p.(*recordingPub); ok {
		rec = r
	}
	return &testEnv{core: core, cc: cc, srv: srv, rec: rec}
}
```

(`newEnvWithPub` becomes: build core via `openStore`, then `return newEnvWithCore(t, core, p)`. The store's `t.Cleanup` close runs after `core.Close()` — cleanup order within the same test is registration order, and `openStore` registers before the env's core cleanup only if called before; call `openStore` first, then `NewCore`, then `newEnvWithCore`, and register `t.Cleanup(core.Close)` explicitly between them:

```go
func newEnvWithPub(t *testing.T, p pub.Publisher, natsURL string) *testEnv {
	t.Helper()
	cat := catalog.Default()
	store := openStore(t) // store cleanup registered FIRST → runs LAST, after core.Close
	core := NewCore(engine.DefaultConfig(), cat, NoopMetrics{}, stats.New(time.Now()), p, store)
	t.Cleanup(core.Close)
	return newEnvWithCore(t, core, p)
}
```

)

`internal/service/alerts_test.go` — replace `TestAlertsByStateIncremental`'s `scan()` closure (lines ~281–289) with a store-backed scan (a stronger pin: atomics vs. the store's own index):

```go
	scan := func() map[string]int {
		out := map[string]int{}
		for s, st := range map[string]alertstore.State{
			"active": alertstore.StateActive, "triggered": alertstore.StateTriggered, "cancelled": alertstore.StateCancelled,
		} {
			_, total, err := e.core.store.Query(alertstore.Filter{State: st, HasState: true})
			if err != nil {
				t.Fatal(err)
			}
			if total > 0 {
				out[s] = total
			}
		}
		return out
	}
```

(add the `alertstore` import to `alerts_test.go`).

`internal/server/server_test.go` — the three `service.NewCore(engine.DefaultConfig(), catalog.Default(), service.NoopMetrics{}, stats.New(now), pub.Noop{})` call sites gain a final `, openStore(t)` argument, with this helper added once to the file:

```go
func openStore(t *testing.T) *alertstore.Store {
	t.Helper()
	s, err := alertstore.Open(filepath.Join(t.TempDir(), "alerts.bbolt"))
	if err != nil {
		t.Fatalf("alertstore.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
```

(plus `"path/filepath"` and the `alertstore` import). **Store must be opened BEFORE `NewCore` and its cleanup registers before any `core.Close` cleanup** so the store closes after the core.

`cmd/chronoctl/seed_test.go` — same pattern for its single `NewCore` call site.

- [ ] **Step 5: Run the full service + server + chronoctl suites**

Run: `go test ./internal/service/ ./internal/server/ ./cmd/chronoctl/ -race`
Expected: PASS, race-clean — including the existing oracle property test, stress test, `TestAlertsByStateIncremental` (now atomics-vs-store), and the new `TestRestartReplayAndFire`.

Run: `go vet ./... && go build ./...`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add internal/service/ internal/server/server_test.go cmd/chronoctl/seed_test.go
git commit -m "feat(service): store-backed alert catalog with replay and bulk pump

The RAM map is gone: upserts persist before engine submission (rollback
on engine rejection), cancels and trigger flips are check-and-flips
inside bolt's single-writer tx, the pump enriches each ring batch with
one BatchGet and marks fired records (fired_price/fired_at) in one
write tx, and NewCore replays the active set into the engine at boot —
expired-while-down alerts land as cancelled. /stats gauges are
incremental atomics pinned against store queries in tests.

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 7: chronod — `-db` flag and store lifecycle

**Files:**
- Modify: `cmd/chronod/main.go`

**Interfaces:**
- Consumes: Task 6 (`service.NewCore(…, store)`, `alertstore.Open`)
- Produces: `-db` flag (default `chrono.bbolt`); store opened before the core, closed after it.

- [ ] **Step 1: Wire the flag and lifecycle**

In `main()`, after the `natsURL` flag:

```go
	dbPath := flag.String("db", "chrono.bbolt", "alert store path (bbolt); source of truth across restarts")
```

and pass `*dbPath` through `run` — change its signature to `run(ctx context.Context, grpcAddr, httpAddr, natsURL, dbPath string) error` and the call site to `run(ctx, *grpcAddr, *httpAddr, *natsURL, *dbPath)`.

In `run`, after the publisher block and **before** `service.NewCore`:

```go
	// The alert store is the source of truth: opened before the engine,
	// closed after it. A corrupt or locked file is fatal — no engine
	// without its catalog. (Timeout inside Open prevents flock hangs.)
	store, err := alertstore.Open(dbPath)
	if err != nil {
		return fmt.Errorf("alert store: %w", err)
	}
	// Backstop ordering note: this defer registers BEFORE core.Close's,
	// so it runs AFTER it (LIFO) — the store always outlives the pump.
	defer func() { _ = store.Close() }()
	core := service.NewCore(engine.DefaultConfig(), catalog.Default(), pm, st, publisher, store)
```

(delete the old `core := service.NewCore(engine.DefaultConfig(), catalog.Default(), pm, st, publisher)` line, keep `defer core.Close()`).

In the explicit shutdown sequence, after `core.Close()` (the `// stops the pump...` line):

```go
	if err := store.Close(); err != nil { // pump drained, engine down; the defer is now a no-op (Close is idempotent for our flow: bolt returns ErrDatabaseNotOpen, ignored)
		slog.Warn("alert store close", "err", err)
	}
```

Add the import `"github.com/emir/chrono-tree/internal/alertstore"`.

(bbolt's `DB.Close` on an already-closed DB returns `bbolt.ErrDatabaseNotOpen`; wrap the backspot defer as shown with `_ =` which already swallows it — no extra code needed.)

- [ ] **Step 2: Build and smoke**

Run: `go build ./cmd/chronod/ && ./chronod -h 2>&1 | head -20` (binary lands in the repo root as `chronod`; if the help output shows `-db`, the flag is wired)

Expected: usage lists `-db string … chrono.bbolt`.

Run: `go vet ./cmd/chronod/ && go test ./cmd/chronod/... -race`
Expected: clean (no chronod package tests currently; the build is the gate).

- [ ] **Step 3: Commit**

```bash
git add cmd/chronod/main.go
git commit -m "feat(chronod): -db flag wires the persistent alert store

Store opens before the engine (corrupt/locked file is fatal) and closes
after the pump and engine have drained.

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 8: chronoctl — offline `compact` subcommand

**Files:**
- Modify: `cmd/chronoctl/main.go`
- Test: `cmd/chronoctl/compact_test.go`

**Interfaces:**
- Consumes: bbolt `Open(ReadOnly)` + `bolt.Compact(dst, src, txMaxSize)`
- Produces: `chronoctl compact [-out path] <db-path>` — compacts into a fresh file via temp + rename, prints before/after sizes. No server dial.

- [ ] **Step 1: Write the failing test**

`cmd/chronoctl/compact_test.go`:

```go
package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunCompact(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "alerts.bbolt")
	// Build a churning store so freelist fragmentation is real.
	s, err := openTestStore(t, src)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "compact.bbolt")
	before, _ := os.Stat(src)
	if err := runCompact(src, out); err != nil {
		t.Fatalf("runCompact: %v", err)
	}
	after, err := os.Stat(out)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() == 0 || after.Size() > before.Size() {
		t.Fatalf("compacted size = %d, before = %d", after.Size(), before.Size())
	}
	if _, err := os.Stat(out + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file must be renamed away")
	}
}

func TestRunCompactMissingSource(t *testing.T) {
	if err := runCompact(filepath.Join(t.TempDir(), "nope.bbolt"), "out.bbolt"); err == nil {
		t.Fatal("missing source must be an error")
	}
}
```

With this helper in the same test file (avoids importing alertstore internals from the CLI package test — it just uses the public API):

```go
func openTestStore(t *testing.T, path string) (*alertstore.Store, error) {
	s, err := alertstore.Open(path)
	if err != nil {
		return nil, err
	}
	// Some churn: puts + cancels so the freelist has reclaimable pages.
	for i := range 200 {
		a := alertstore.Alert{ID: engine.AlertID{byte(i >> 8), byte(i), 1}, Symbol: "BTCUSDT", Decimals: 8,
			Venue: "ATLAS", Tier: "TOP", PriceType: engine.PriceAsk, Direction: engine.DirGTE,
			TargetPrice: 65000000000000, CreatedAt: int64(i)}
		if err := s.Put(a); err != nil {
			return nil, err
		}
		if i%2 == 0 {
			if _, err := s.Cancel(a.ID); err != nil {
				return nil, err
			}
		}
	}
	return s, nil
}
```

(imports: `"github.com/emir/chrono-tree/engine"`, `"github.com/emir/chrono-tree/internal/alertstore"`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/chronoctl/ -run Compact -v`
Expected: FAIL — `runCompact` undefined.

- [ ] **Step 3: Implement in `cmd/chronoctl/main.go`**

Add the subcommand flags next to the others:

```go
	compactCmd := flag.NewFlagSet("compact", flag.ExitOnError)
	compactOut := compactCmd.String("out", "", "output path (default: <db>.compact)")
```

Restructure `main` so the dial happens only for server-backed subcommands — replace the `switch flag.Arg(0)` block and move the dial below a compact early-path:

```go
	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: chronoctl [flags] <alert|seed|compact> ...")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// compact is offline: it never touches a server.
	if flag.Arg(0) == "compact" {
		if err := compactCmd.Parse(flag.Args()[1:]); err != nil {
			os.Exit(2)
		}
		if compactCmd.NArg() != 1 {
			fmt.Fprintln(os.Stderr, "usage: chronoctl compact [-out path] <db-path>")
			os.Exit(2)
		}
		out := *compactOut
		if out == "" {
			out = compactCmd.Arg(0) + ".compact"
		}
		if err := runCompact(compactCmd.Arg(0), out); err != nil {
			slog.Error("compact", "err", err)
			os.Exit(1)
		}
		return
	}

	conn, err := dial(*server)
	// … unchanged from here (the alert/seed switch, minus a new compact case)
```

And add at the bottom of the file:

```go
// runCompact rewrites a bbolt file with bolt.Compact: read-only source,
// fresh temp destination, atomic rename. Offline only — never run
// against a file chronod holds (the source flock is respected by the
// read-only open; a live writer would be an operator error anyway).
func runCompact(dbPath, outPath string) error {
	src, err := bolt.Open(dbPath, 0o600, &bolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer func() { _ = src.Close() }()
	tmp := outPath + ".tmp"
	dst, err := bolt.Open(tmp, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return fmt.Errorf("open dest: %w", err)
	}
	if err := bolt.Compact(dst, src, 1<<16); err != nil {
		_ = dst.Close()
		return fmt.Errorf("compact: %w", err)
	}
	if err := dst.Close(); err != nil {
		return err
	}
	before, after := fileSizes(dbPath, tmp)
	if err := os.Rename(tmp, outPath); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	fmt.Printf("%s: %d -> %d bytes\n", outPath, before, after)
	return nil
}

func fileSizes(paths ...string) (int64, int64) {
	var out [2]int64
	for i, p := range paths {
		if st, err := os.Stat(p); err == nil {
			out[i] = st.Size()
		}
	}
	return out[0], out[1]
}
```

Add imports: `bolt "go.etcd.io/bbolt"`, `"time"`.

- [ ] **Step 4: Run tests**

Run: `go test ./cmd/chronoctl/ -race -v`
Expected: PASS (compact tests + existing seed test).

Run: `go build ./cmd/chronoctl/ && ./chronoctl compact -h`
Expected: usage for the compact flag set.

- [ ] **Step 5: Commit**

```bash
git add cmd/chronoctl/
git commit -m "feat(chronoctl): offline compact subcommand

bbolt.Compact into a temp file, atomic rename, before/after sizes.
The alert log is a permanent audit store; compaction is an operator's
manual choice, never background machinery.

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 9: Perf gates + full verification

**Files:**
- Create: `internal/alertstore/bench_test.go`
- No production code changes unless a gate fails.

**Interfaces:**
- Consumes: Tasks 1–8.
- Produces: the acceptance evidence for spec §10.

- [ ] **Step 1: Write the inquiry benchmark**

`internal/alertstore/bench_test.go`:

```go
package alertstore

import (
	"testing"

	"github.com/emir/chrono-tree/engine"
)

// benchSeed bulk-loads n records in chunked write txs (the bench setup
// is not the system under test; chunking just keeps setup fast).
func benchSeed(b *testing.B, s *Store, n int) []Alert {
	b.Helper()
	alerts := make([]Alert, n)
	for i := range n {
		alerts[i] = Alert{
			Symbol: []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}[i%3], Decimals: 8,
			Venue: []string{"ATLAS", "NOVA", "ZENITH"}[i%3], Tier: []string{"TOP", "MID"}[i%2],
			PriceType: engine.PriceAsk, Direction: engine.Direction(i % 2),
			TargetPrice: engine.Price(i), CreatedAt: int64(1_700_000_000_000_000_000 + i),
			State:       StateActive,
		}
		if i%10 == 0 { // 10% of the population is triggered: the inquiry worst case
			alerts[i].State = StateTriggered
			alerts[i].FiredPrice, alerts[i].FiredAt = engine.Price(i), int64(i)
		}
		// AFTER the literal: an unnamed field in the composite would zero
		// a pre-copied ID, collapsing all records onto the zero key.
		copy(alerts[i].ID[:], []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i), 0x99})
	}
	const chunk = 10000
	for start := 0; start < n; start += chunk {
		end := min(start+chunk, n)
		err := s.db.Batch(func(tx *bolt.Tx) error {
			for i := start; i < end; i++ {
				a := alerts[i]
				if err := putIndexed(tx, &a); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	return alerts
}

// BenchmarkQueryTriggeredPage1 is the spec's acceptance gate:
// state=triggered among 1M alerts, first page of 50, < 10 ms.
func BenchmarkQueryTriggeredPage1(b *testing.B) {
	s, _ := openStoreB(b)
	benchSeed(b, s, 1_000_000)
	b.ResetTimer()
	for b.Loop() {
		items, total, err := s.Query(Filter{HasState: true, State: StateTriggered, Limit: 50})
		if err != nil {
			b.Fatal(err)
		}
		if total != 100_000 || len(items) != 50 {
			b.Fatalf("total=%d len=%d", total, len(items))
		}
	}
}

func openStoreB(b *testing.B) (*Store, string) {
	b.Helper()
	path := b.TempDir() + "/bench.bbolt"
	s, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s, path
}
```

(`openStore` in `store_test.go` takes `*testing.T`; benchmark files cannot reuse it — hence `openStoreB`. Imports: `testing`, `bolt "go.etcd.io/bbolt"`, `"github.com/emir/chrono-tree/engine"`.)

- [ ] **Step 2: Run the gate**

Run: `go test ./internal/alertstore/ -bench BenchmarkQueryTriggeredPage1 -benchtime 10x -run xxx`
Expected: `ok … ~<10ms/op` — the per-op time reported must be under 10 ms. If it is not, stop and investigate before proceeding (the whole design's acceptance is this number).

- [ ] **Step 3: Full verification sweep**

```bash
go build ./... && go vet ./...
go test ./... -race
git diff --stat main -- engine/ price/   # on the working branch vs main: must be empty
```

Expected: all green, `engine/` and `price/` zero diffs, goleak clean (existing `VerifyTestMain` in each binary package).

- [ ] **Step 4: End-to-end restart smoke (manual, evidence for the report)**

```bash
go build ./cmd/chronod/ ./cmd/chronoctl/ ./cmd/chronofeed/
./chronod -db /tmp/smoke.bbolt -http-addr :18080 -grpc-addr :19090 &
./chronoctl -server localhost:19090 alert -pair BTCUSDT -price 65000.00
kill %1   # graceful SIGTERM
./chronod -db /tmp/smoke.bbolt -http-addr :18080 -grpc-addr :19090 &
curl -s localhost:18080/api/alerts | head -c 400
kill %1
```

Expected: the curl output lists the alert with `"state":"active"` and `"target_price":"65000.00"` — persisted across the restart and replayed. Record the curl output in the task report.

- [ ] **Step 5: Commit**

```bash
git add internal/alertstore/bench_test.go
git commit -m "test(alertstore): inquiry benchmark — 1M-record triggered page gate

state=triggered among 1M alerts, page 1 of 50, must stay < 10ms/op:
the acceptance number for the whole store design.

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Post-Plan Self-Review (already applied)

1. **Spec coverage:** §3 layout (Tasks 1–8), §4 data model/keys (T1–2), §5 service integration (T6), §6 write paths + race (T3, T6), §7 read paths (T4, T6), §8 replay/lifecycle/compact (T5–8), §9 error handling (T6 `internalf`, pump failure paths), §10 testing + perf gates (T1–9), §11 out-of-scope respected (no new states, no compound indexes, no deletion).
2. **Placeholders:** none — every step carries code or an exact command.
3. **Type consistency:** `NewCore(…, store *alertstore.Store)` used identically in T6–T8; `Fired{ID, Price, At}` defined T3, consumed T6; `Filter` fields defined T4, consumed T6's `ListAlerts`; `openStore(t)` helper duplicated per test package with identical shape. Verified engine accepts `Expires` in the past when `ValidFrom` is 0 (`AlertSpec.validate` only rejects `Expires <= ValidFrom`) — the restart test's expired fixture is valid.
