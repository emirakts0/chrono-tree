package engine

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

// waitForExpLen polls the reaper-written expiry gauge until it reaches want.
// The table itself is reaper-owned; the atomic gauge is the race-free view.
func waitForExpLen(t *testing.T, e *Engine, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if int(e.expLen.Load()) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("expiry table length = %d, want %d", e.expLen.Load(), want)
}

// TestExpiryQueueUnbounded pins the lossless expQ: a burst of expiring
// upserts far deeper than the old channel capacity must not block Upsert,
// and every registration must land in the reaper's table.
func TestExpiryQueueUnbounded(t *testing.T) {
	defer goleak.VerifyNone(t)
	cfg := DefaultConfig()
	cfg.ReaperInterval = time.Hour // sweeps never interfere
	e := New(cfg)
	defer e.Close()

	const n = 3*expChunk + 17 // crosses several chunk boundaries
	exp := time.Now().Add(time.Hour).UnixNano()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			if err := e.Upsert(AlertSpec{ID: mkID(uint32(i)), Symbol: "UB",
				PriceType: PriceLast, Direction: DirGTE, TargetPrice: 100,
				ValidFrom: 1, Expires: exp}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Upsert blocked on a full expiry queue")
	}
	waitForExpLen(t, e, n)
}
