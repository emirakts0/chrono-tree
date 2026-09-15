package engine

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// TestChurnInvariants replaces the integrity sweep's repair guarantee with
// an end-state assertion. After mixed churn — upserts, same-ID replacements,
// cancels, expiry-driven removals, and Match fires over mixed TTLs — every
// structural census must agree exactly:
//
//  1. no terminal-status entry remains indexed in any tree
//  2. e.refs, the trees, and the live counter hold the same alert set
//  3. every live expiring alert has exactly one expiry registration;
//     registrations for dead handouts may linger until their deadline
//     (accepted design trade) but must all belong to known dead ids
//  4. no slot stays parked in the retire list after the grace period
//
// Deterministic via a fixed seed; timing margins are wide (shortest TTL
// 20ms vs 500ms settle, reaper tick 5ms).
func TestChurnInvariants(t *testing.T) {
	defer goleak.VerifyNone(t)
	rng := rand.New(rand.NewSource(42))
	cfg := DefaultConfig()
	cfg.ReaperInterval = 5 * time.Millisecond
	e := New(cfg)
	defer e.Close()

	const syms, perSym = 8, 128
	base := time.Now().Add(20 * time.Millisecond)
	mk := func(id uint32, sym string, price Price, dir Direction, exp int64) AlertSpec {
		return AlertSpec{ID: mkID(id), Symbol: sym, PriceType: PriceLast,
			Direction: dir, TargetPrice: price, ValidFrom: 1, Expires: exp,
			AutoDeactivate: true}
	}
	for s := 0; s < syms; s++ {
		sym := fmt.Sprintf("CH%02d", s)
		for i := 0; i < perSym; i++ {
			id := uint32(s*perSym + i)
			var exp int64
			switch rng.Intn(4) {
			case 0:
				exp = 0 // never
			case 1:
				exp = base.Add(20 * time.Millisecond).UnixNano() // expires during churn
			case 2:
				exp = base.Add(150 * time.Millisecond).UnixNano() // expires during churn
			case 3:
				exp = base.Add(time.Hour).UnixNano() // outlives the test
			}
			if err := e.Upsert(mk(id, sym, Price(100+i%80), Direction(i&1), exp)); err != nil {
				t.Fatal(err)
			}
		}
	}
	e.Sync()

	// Churn while the short TTLs are firing: cancel a third, replace a
	// sixth with far-future deadlines, fire the rest via ticks that cross
	// part of the band.
	for s := 0; s < syms; s++ {
		sym := fmt.Sprintf("CH%02d", s)
		for i := 0; i < perSym; i++ {
			id := uint32(s*perSym + i)
			switch {
			case i%3 == 0:
				_ = e.Cancel(mkID(id)) // may already have expired: ignore error
			case i%6 == 4:
				if err := e.Upsert(mk(id, sym, Price(100+i%80), Direction(i&1),
					base.Add(time.Hour).UnixNano())); err != nil {
					t.Fatal(err)
				}
			}
		}
		e.Match(&Tick{Symbol: sym, Last: 140, Present: 1 << uint(PriceLast),
			TS: time.Now().UnixNano()})
		e.Match(&Tick{Symbol: sym, Last: 60, Present: 1 << uint(PriceLast),
			TS: time.Now().UnixNano()})
	}

	// Settle: every deadline <= base+150ms sweeps, every removal flushes,
	// every retired slot passes the 2x5ms grace and recycles.
	time.Sleep(500 * time.Millisecond)
	e.Sync()
	time.Sleep(50 * time.Millisecond)
	e.Sync()

	// Census 1+2: trees hold only live-status entries; per-symbol counts
	// match refs and live.
	e.mu.Lock()
	refsSnapshot := make(map[AlertID]*alertRef, len(e.refs))
	for id, r := range e.refs {
		refsSnapshot[id] = r
	}
	live := e.live
	e.mu.Unlock()
	indexed := 0
	for s := 0; s < len(e.states); s++ {
		snap := e.states[s].snap.Load()
		if snap == nil {
			continue
		}
		for ti := range snap.trees {
			for en := range snap.trees[ti].All() {
				indexed++
				st := e.slots.status(en.idx)
				if st != StatusActive && st != StatusPaused {
					t.Fatalf("tree entry %x indexed with status %v", en.id, st)
				}
				r, ok := refsSnapshot[en.id]
				if !ok {
					t.Fatalf("tree entry %x missing from refs", en.id)
				}
				if r.e.idx != en.idx {
					t.Fatalf("refs/tree idx disagree for %x: %d vs %d", en.id, r.e.idx, en.idx)
				}
			}
		}
	}
	if indexed != len(refsSnapshot) {
		t.Fatalf("indexed=%d refs=%d — sets disagree", indexed, len(refsSnapshot))
	}
	if live != uint64(len(refsSnapshot)) {
		t.Fatalf("live=%d refs=%d — counter disagrees", live, len(refsSnapshot))
	}

	// Census 4: retire list drained.
	e.slots.mu.Lock()
	parked := len(e.slots.retired)
	e.slots.mu.Unlock()
	if parked != 0 {
		t.Fatalf("%d slots still parked after settle+grace", parked)
	}

	// Census 3 (over the quiesced table — Close stops the reaper, and Close
	// is idempotent with the deferred call): every live expiring alert is
	// registered exactly once; registrations for dead handouts linger until
	// their deadline (accepted trade) but never duplicate a ref.
	e.Close()
	regs := make(map[*alertRef]int)
	for x := range e.expiry.All() {
		regs[x.ref]++
	}
	liveExpiring := 0
	for _, r := range refsSnapshot {
		if r.e.expires == 0 {
			continue
		}
		liveExpiring++
		if regs[r] != 1 {
			t.Fatalf("live expiring %x has %d registrations, want 1", r.e.id, regs[r])
		}
	}
	for r, n := range regs {
		if n != 1 {
			t.Fatalf("ref %x has %d registrations, want 1", r.e.id, n)
		}
	}
	t.Logf("census: live=%d liveExpiring=%d regs=%d", live, liveExpiring, len(regs))
}
