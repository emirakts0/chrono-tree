package pub

import (
	"encoding/json/v2"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"

	"github.com/emir/chrono-tree/internal/pub/pubtest"
)

func sampleTrigger() Trigger {
	return Trigger{
		AlertID: "0192ced1-4a1e-7abc-8def-0123456789ab", Symbol: "BTCUSDT",
		Venue: "ATLAS", Tier: "TOP",
		FiredPrice: "65001.00", FiredAtUnixNanos: 1_700_000_000_000_000_001,
		Direction: "ABOVE", TargetPrice: "65000.00",
	}
}

func TestSubject(t *testing.T) {
	if got := Subject("ATLAS", "TOP"); got != "chrono.triggers.ATLAS.TOP" {
		t.Fatalf("Subject = %q", got)
	}
}

func TestPublishSubjectPayloadHeaders(t *testing.T) {
	url := pubtest.Start(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close() // nats.go v1.53: Conn.Close returns no value
	raw := make(chan *nats.Msg, 16)
	if _, err := nc.ChanSubscribe("chrono.triggers.>", raw); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	p, err := NewNATS(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if !p.Connected() {
		t.Fatal("publisher should report connected")
	}
	want := sampleTrigger()
	if err := p.Publish(want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	var msg *nats.Msg
	select {
	case msg = <-raw:
	case <-time.After(3 * time.Second):
		t.Fatal("no message received")
	}
	if msg.Subject != "chrono.triggers.ATLAS.TOP" {
		t.Fatalf("subject = %q", msg.Subject)
	}
	if got := msg.Header.Get("Nats-Msg-Id"); got != want.AlertID {
		t.Fatalf("Nats-Msg-Id = %q, want %q", got, want.AlertID)
	}
	if got := msg.Header.Get("Symbol"); got != want.Symbol {
		t.Fatalf("Symbol header = %q, want %q", got, want.Symbol)
	}
	var got Trigger
	if err := json.Unmarshal(msg.Data, &got); err != nil {
		t.Fatalf("payload not JSON: %v", err)
	}
	if got != want {
		t.Fatalf("payload = %+v, want %+v", got, want)
	}
}

func TestPublishWildcardTier(t *testing.T) {
	url := pubtest.Start(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close() // nats.go v1.53: Conn.Close returns no value
	raw := make(chan *nats.Msg, 16)
	if _, err := nc.ChanSubscribe("chrono.triggers.*.MID", raw); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	p, err := NewNATS(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	top := sampleTrigger() // ATLAS/TOP — must NOT arrive
	if err := p.Publish(top); err != nil {
		t.Fatal(err)
	}
	mid := sampleTrigger()
	mid.Tier = "MID"
	if err := p.Publish(mid); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-raw:
		if msg.Subject != "chrono.triggers.ATLAS.MID" {
			t.Fatalf("subject = %q", msg.Subject)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("MID trigger not received on *.MID")
	}
	select {
	case msg := <-raw:
		t.Fatalf("unexpected extra message on subject %q", msg.Subject)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestSubscribeTriggersRoundTrip(t *testing.T) {
	url := pubtest.Start(t)
	p, err := NewNATS(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()

	got := make(chan Trigger, 4)
	if err := p.SubscribeTriggers(func(tr Trigger) { got <- tr }); err != nil {
		t.Fatal(err)
	}
	if err := p.SubscribeTriggers(func(Trigger) {}); err == nil {
		t.Fatal("second SubscribeTriggers should error")
	}

	// Publish on the publisher's own connection: the broker delivers it
	// back to the subscription on the same connection (the loopback the
	// dashboard uses is same-connection too).
	if err := p.Publish(sampleTrigger()); err != nil {
		t.Fatal(err)
	}

	select {
	case tr := <-got:
		if tr != sampleTrigger() {
			t.Fatalf("received = %+v, want %+v", tr, sampleTrigger())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("subscription did not receive the published trigger")
	}
}

func TestNewNATSBootFails(t *testing.T) {
	// Port 1 is never a NATS server; RetryOnFailedConnect is off.
	if _, err := NewNATS("nats://127.0.0.1:1"); err == nil {
		t.Fatal("NewNATS should fail on unreachable server")
	}
}

func TestPublishAfterCloseErrors(t *testing.T) {
	url := pubtest.Start(t)
	p, err := NewNATS(url)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := p.Publish(sampleTrigger()); err == nil {
		t.Fatal("Publish after Close should error (drop-and-count path)")
	}
	if p.Connected() {
		t.Fatal("Connected should be false after Close")
	}
	// Close is safe to call twice (chronod defer + shutdown path).
	_ = p.Close()
}
