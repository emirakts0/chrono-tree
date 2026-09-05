package service

import (
	"context"
	"encoding/json/v2"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/pub"
)

func TestPublishesEnrichedTrigger(t *testing.T) {
	e := newNATSEnv(t)
	nc := e.natsClient(t)
	raw := make(chan *nats.Msg, 16)
	if _, err := nc.ChanSubscribe("chrono.triggers.>", raw); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	resp, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
		Symbol: "BTCUSDT", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction:   chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "65000.00", Venue: "ATLAS", Tier: "TOP",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65001.00", "ATLAS", "TOP"),
	}}); err != nil {
		t.Fatal(err)
	}

	var msg *nats.Msg
	select {
	case msg = <-raw:
	case <-time.After(3 * time.Second):
		t.Fatal("no trigger published")
	}
	if msg.Subject != pub.Subject("ATLAS", "TOP") {
		t.Fatalf("subject = %q", msg.Subject)
	}
	var tr pub.Trigger
	if err := json.Unmarshal(msg.Data, &tr); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if tr.AlertID != resp.GetAlertId() || tr.Symbol != "BTCUSDT" ||
		tr.Venue != "ATLAS" || tr.Tier != "TOP" {
		t.Fatalf("trigger dims/id wrong: %+v", tr)
	}
	if tr.FiredPrice != "65001.00" || tr.TargetPrice != "65000.00" {
		t.Fatalf("prices wrong: %+v", tr)
	}
	if tr.Direction != "ABOVE" || tr.FiredAtUnixNanos == 0 {
		t.Fatalf("direction/timestamp wrong: %+v", tr)
	}
	if got := msg.Header.Get("Nats-Msg-Id"); got != resp.GetAlertId() {
		t.Fatalf("Nats-Msg-Id = %q", got)
	}
	// The state flip is a separate store write after the publish — poll
	// for it rather than racing the pump's MarkTriggeredBatch.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && e.core.AlertsByState()["triggered"] != 1 {
		time.Sleep(time.Millisecond)
	}
	if got := e.core.AlertsByState()["triggered"]; got != 1 {
		t.Fatalf("triggered state count = %d, want 1", got)
	}
	if e.core.stats.TriggersPublished.Load() != 1 {
		t.Fatal("publish not counted")
	}
}

func TestPublishDropWhenPublisherFails(t *testing.T) {
	e := newEnv(t)
	e.rec.fail = true
	if _, err := e.alerts().UpsertAlert(context.Background(), &chronov1.UpsertAlertRequest{
		Symbol: "BTCUSDT", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction:   chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "65000.00", Venue: "ATLAS", Tier: "TOP",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := runTicks(t, e, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65000.00", "65001.00", "ATLAS", "TOP"),
	}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && e.core.stats.TriggersPublishDropped.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	if got := e.core.stats.TriggersPublishDropped.Load(); got != 1 {
		t.Fatalf("TriggersPublishDropped = %d, want 1", got)
	}
	if got := e.core.stats.TriggersPublished.Load(); got != 0 {
		t.Fatalf("TriggersPublished = %d, want 0", got)
	}
	if len(e.rec.triggers()) != 0 {
		t.Fatal("failing publisher should not record triggers")
	}
	// The alert stays active: only published triggers are marked fired,
	// so a delivery failure leaves it armed — it re-fires on a later tick
	// (visible duplication beats silent loss).
	if got := e.core.AlertsByState()["active"]; got != 1 {
		t.Fatalf("active state count = %d, want 1", got)
	}
	if got := e.core.AlertsByState()["triggered"]; got != 0 {
		t.Fatalf("triggered state count = %d, want 0", got)
	}
}
