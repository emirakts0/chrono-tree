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
