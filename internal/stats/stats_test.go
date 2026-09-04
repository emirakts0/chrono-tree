package stats

import (
	"math"
	"testing"
	"time"
)

// base builds a Rate with two completed seconds of history. now must be
// second-truncated so the "current" second stays empty.
func base(now time.Time) *Rate {
	var r Rate
	r.Add(10, now.Add(-2*time.Second))
	r.Add(15, now.Add(-2*time.Second)) // bucket t-2 holds 25
	r.Add(25, now.Add(-1*time.Second)) // bucket t-1 holds 25
	return &r
}

func TestRateBasics(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	r := base(now)
	if got := r.PerSecond(now); got != 25 { // (25+25)/2 complete seconds
		t.Fatalf("PerSecond = %v, want 25", got)
	}
	r.Add(100, now.Add(-1*time.Second)) // completes into t-1 → 150/2
	if got := r.PerSecond(now); got != 75 {
		t.Fatalf("PerSecond after add = %v, want 75", got)
	}
}

func TestRateRolloverClearsOldBuckets(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	r := base(now)
	// Jump far ahead: everything old must be cleared, not summed.
	future := now.Add(5 * time.Minute)
	r.Add(7, future)
	if got := r.PerSecond(future); got > 0.001 {
		t.Fatalf("PerSecond after 5min gap = %v, want ~0 (the 7 landed in future's own, excluded second)", got)
	}
}

func TestSnapshotFields(t *testing.T) {
	now := time.Now()
	s := New(now)
	s.Ticks.Add(3)
	s.TicksDropped.Add(1)
	s.TriggersFired.Add(2)
	s.TriggersDelivered.Add(2)
	s.WatcherDrops.Add(1)
	snap := s.Snapshot(now.Add(2 * time.Second))
	if snap.Ticks != 3 || snap.TicksDropped != 1 || snap.TriggersFired != 2 ||
		snap.TriggersDelivered != 2 || snap.WatcherDrops != 1 {
		t.Fatalf("snapshot counters wrong: %+v", snap)
	}
	if math.Abs(snap.UptimeSec-2) > 0.01 {
		t.Fatalf("uptime = %v, want 2", snap.UptimeSec)
	}
}
