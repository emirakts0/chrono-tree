// Package pub owns the trigger wire contract for NATS: enriched triggers
// as JSON on chrono.triggers.{venue}.{tier} subjects with Nats-Msg-Id and
// Symbol headers. Core NATS only — at-most-once, fire-and-forget; the
// Publisher interface is the seam a JetStream implementation slots into.
package pub

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// Trigger is the enriched, wire-ready trigger (spec §3).
type Trigger struct {
	AlertID          string `json:"alert_id"`
	Symbol           string `json:"symbol"`
	Venue            string `json:"venue"`
	Tier             string `json:"tier"`
	FiredPrice       string `json:"fired_price"` // decimal string
	FiredAtUnixNanos int64  `json:"fired_at_unix_nanos"`
	Direction        string `json:"direction"`    // "ABOVE" | "BELOW"
	TargetPrice      string `json:"target_price"` // decimal string
}

// Subject maps a dim combo to its NATS subject.
func Subject(venue, tier string) string {
	return fmt.Sprintf("chrono.triggers.%s.%s", venue, tier)
}

// Publisher is the delivery seam. Publish must never block and never
// retry: an error means the caller drops the trigger and counts it.
// Connected is for observability only, never gating.
type Publisher interface {
	Publish(tr Trigger) error
	Connected() bool
	Close() error
}

// drainBound caps how long Close waits for the connection's pending
// publishes to flush before cutting it (spec §6).
const drainBound = 3 * time.Second

// NATSPublisher publishes over one core NATS connection.
type NATSPublisher struct {
	nc  *nats.Conn
	sub *nats.Subscription
}

// NewNATS connects to url and fails if the server is unreachable at call
// time (chronod treats that as a fatal boot error). Later disconnects are
// handled by nats.go's reconnect: infinite attempts, 100ms base wait with
// up to 2s jitter. While disconnected, nats.go buffers pending publishes
// (default 8MB); Publish errors once the buffer overflows or the
// connection is closed — those errors are the drop-and-count path.
func NewNATS(url string) (*NATSPublisher, error) {
	nc, err := nats.Connect(url,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(100*time.Millisecond),
		nats.ReconnectJitter(2*time.Second, 2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect %s: %w", url, err)
	}
	return &NATSPublisher{nc: nc}, nil
}

// Publish marshals tr and publishes it on Subject(tr.Venue, tr.Tier) with
// the Nats-Msg-Id (JetStream dedup hook, free today) and Symbol headers.
func (p *NATSPublisher) Publish(tr Trigger) error {
	payload, err := json.Marshal(tr)
	if err != nil {
		return fmt.Errorf("marshal trigger: %w", err)
	}
	msg := nats.NewMsg(Subject(tr.Venue, tr.Tier))
	msg.Data = payload
	msg.Header.Set("Nats-Msg-Id", tr.AlertID)
	msg.Header.Set("Symbol", tr.Symbol)
	return p.nc.PublishMsg(msg)
}

// SubscribeTriggers re-consumes our own published trigger stream — the
// NATS loopback the monitoring dashboard uses. fn runs on nats.go's
// reader goroutine and must never block. The subscription rides the
// publisher's connection: re-established across reconnects, gone at
// Close. Only one subscription per publisher.
func (p *NATSPublisher) SubscribeTriggers(fn func(Trigger)) error {
	if p.sub != nil {
		return errors.New("pub: triggers already subscribed")
	}
	sub, err := p.nc.Subscribe("chrono.triggers.>", func(m *nats.Msg) {
		var tr Trigger
		if err := json.Unmarshal(m.Data, &tr); err != nil {
			return // not a trigger payload; ignore
		}
		fn(tr)
	})
	if err != nil {
		return fmt.Errorf("subscribe chrono.triggers.>: %w", err)
	}
	p.sub = sub
	return nil
}

// Connected reports the live connection state.
func (p *NATSPublisher) Connected() bool {
	return p.nc.Status() == nats.CONNECTED
}

// Close drains (flushes pending publishes, bounded by drainBound) and
// closes the connection. Safe to call more than once.
func (p *NATSPublisher) Close() error {
	// Unsubscribe before draining: with a live subscription, nats.go's
	// drain spins an UNSUB+FlushTimeout goroutine that can outlive Close
	// (goleak in cmd/chronod). SubscribeTriggers and Close are both called
	// from chronod's run goroutine chain, so this unsynchronized read is
	// safe today.
	if p.sub != nil {
		_ = p.sub.Unsubscribe()
	}
	done := make(chan error, 1)
	go func() { done <- p.nc.Drain() }()
	select {
	case err := <-done:
		p.nc.Close()
		return err
	case <-time.After(drainBound):
		p.nc.Close()
		return fmt.Errorf("nats drain did not finish within %s", drainBound)
	}
}

// Noop is the zero-dependency publisher for tests and call sites that
// have not wired NATS yet. Connected reports true: to /stats and the
// gauge, a Noop publisher presents as a healthy sink.
type Noop struct{}

func (Noop) Publish(Trigger) error { return nil }
func (Noop) Connected() bool       { return true }
func (Noop) Close() error          { return nil }
