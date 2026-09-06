package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
)

// upsertCluster seeds n ABOVE alerts on symbol at targets 10.00..10.(n-1),
// all fired by an ask at 20.00.
func upsertCluster(t *testing.T, e *testEnv, symbol string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
			Symbol: symbol, PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
			Direction:   chronov1.Direction_DIRECTION_ABOVE,
			TargetPrice: fmt.Sprintf("10.%02d", i), Venue: "ATLAS", Tier: "TOP",
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// waitFlips polls store truth until n records sit in triggered.
func waitFlips(t *testing.T, e *testEnv, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, tr, _, err := e.store.Counts(); err == nil && int(tr) == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("store flips did not reach %d within 3s", n)
}

// TestPumpPipelineFlushesOnClose: Close must not return until the flipper
// has written every in-flight flip — a shutdown mid-pipeline loses nothing.
func TestPumpPipelineFlushesOnClose(t *testing.T) {
	e := newEnv(t)
	const n = 30
	upsertCluster(t, e, "FLUSH", n)
	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("FLUSH", "9.99", "20.00", "ATLAS", "TOP"),
	}}); err != nil {
		t.Fatal(err)
	}
	waitTriggers(t, e, n) // enriched + published; flips may still be in flight
	e.core.Close()        // cleanup closes again — idempotent by contract
	a, tr, cn, err := e.store.Counts()
	if err != nil {
		t.Fatal(err)
	}
	if tr != n || a != 0 || cn != 0 {
		t.Fatalf("after close: active=%d triggered=%d cancelled=%d, want 0/%d/0", a, tr, cn, n)
	}
	if got := e.core.AlertsByState()[StateTriggered]; got != n {
		t.Fatalf("triggered gauge = %d, want %d (gauges must match store truth)", got, n)
	}
}

// TestPumpGaugesTrackStore: gauges update on the flip stage; after the
// pipeline quiesces across multiple fire waves they equal store truth.
func TestPumpGaugesTrackStore(t *testing.T) {
	e := newEnv(t)
	const n = 10
	upsertCluster(t, e, "GAUGE", n)
	// Wave 1: ask 10.02 fires targets 10.00-10.02 (3); wave 2: ask 10.09
	// fires the rest (7) — two flip batches.
	for _, ask := range []string{"10.02", "10.09"} {
		if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
			tick("GAUGE", "9.99", ask, "ATLAS", "TOP"),
		}}); err != nil {
			t.Fatal(err)
		}
	}
	waitTriggers(t, e, n)
	waitFlips(t, e, n)
	a, tr, _, err := e.store.Counts()
	if err != nil {
		t.Fatal(err)
	}
	got := e.core.AlertsByState()
	if got[StateTriggered] != int(tr) || got[StateActive] != int(a) {
		t.Fatalf("gauges (%v) != store (active=%d triggered=%d)", got, a, tr)
	}
	if tr != n {
		t.Fatalf("store triggered = %d, want %d", tr, n)
	}
}
