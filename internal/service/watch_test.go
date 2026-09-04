package service

import (
	"context"
	"testing"
	"time"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
)

// watchOne opens a WatchTriggers stream, runs a tick batch, and returns the
// first trigger received or nil after the timeout.
func watchOne(t *testing.T, e *testEnv, batches ...*chronov1.TickBatch) *chronov1.Trigger {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := e.alerts().WatchTriggers(ctx, &chronov1.WatchTriggersRequest{})
	if err != nil {
		t.Fatalf("WatchTriggers: %v", err)
	}
	// Wait for the watcher to be registered server-side (stream creation
	// returns before the handler runs) before asserting and firing.
	deadline := time.Now().Add(2 * time.Second)
	for e.core.WatcherCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := e.core.WatcherCount(); got != 1 {
		t.Fatalf("watcher count = %d, want 1", got)
	}
	if _, err := runTicks(t, e, batches...); err != nil {
		t.Fatal(err)
	}
	done := make(chan *chronov1.Trigger, 1)
	go func() {
		tr, err := stream.Recv()
		if err == nil {
			done <- tr
		}
		close(done)
	}()
	select {
	case tr := <-done:
		return tr
	case <-time.After(3 * time.Second):
		return nil
	}
}

func TestWatchReceivesEnrichedTrigger(t *testing.T) {
	e := newEnv(t)
	resp, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
		Symbol: "BTCUSDT", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction: chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "65000.00", Venue: "ATLAS", Tier: "TOP",
	})
	if err != nil {
		t.Fatal(err)
	}
	tr := watchOne(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65001.00", "ATLAS", "TOP"),
	}})
	if tr == nil {
		t.Fatal("no trigger received")
	}
	if tr.GetAlertId() != resp.GetAlertId() {
		t.Fatalf("id = %q, want %q", tr.GetAlertId(), resp.GetAlertId())
	}
	if tr.GetSymbol() != "BTCUSDT" || tr.GetVenue() != "ATLAS" || tr.GetTier() != "TOP" {
		t.Fatalf("dims wrong: %v", tr)
	}
	if tr.GetFiredPrice() != "65001.00" {
		t.Fatalf("fired price = %q, want 65001.00", tr.GetFiredPrice())
	}
	if tr.GetTargetPrice() != "65000.00" {
		t.Fatalf("target price = %q, want 65000.00", tr.GetTargetPrice())
	}
	if tr.GetDirection() != chronov1.Direction_DIRECTION_ABOVE {
		t.Fatalf("direction = %v", tr.GetDirection())
	}
	if tr.GetFiredAtUnixNanos() == 0 {
		t.Fatal("fired_at not set")
	}
	if got := e.core.AlertsByState()["triggered"]; got != 1 {
		t.Fatalf("triggered state count = %d, want 1", got)
	}
	if e.core.stats.TriggersDelivered.Load() != 1 {
		t.Fatal("delivery not counted")
	}
}

func TestWatchCancelRemovesWatcher(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := e.alerts().WatchTriggers(ctx, &chronov1.WatchTriggersRequest{})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for e.core.WatcherCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if _, err := stream.Recv(); err == nil {
		t.Fatal("stream should end on cancel")
	}
	deadline = time.Now().Add(2 * time.Second)
	for e.core.WatcherCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := e.core.WatcherCount(); got != 0 {
		t.Fatalf("watcher count after cancel = %d, want 0", got)
	}
}
