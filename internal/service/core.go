// Package service implements the chrono.v1 gRPC services on top of the
// engine: reference-data validation, price conversion at the boundary,
// a service-side alert catalog, and (Task 6) trigger fan-out.
package service

import (
	"context"
	"sync"
	"time"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/stats"
	"github.com/emir/chrono-tree/price"
)

// Metrics is the observability hook; implementations must be safe for
// concurrent use. NoopMetrics covers tests.
type Metrics interface {
	Tick(venue, tier string)
	TickDropped()
	TickBatch(n int)
	TickLatency(d time.Duration)
	TriggerFired(symbol, venue, tier string)
	TriggerDelivered()
	WatcherDrop()
	AlertsActive(n int)
	Watchers(n int)
	FeedConnected(b bool)
}

// NoopMetrics discards everything.
type NoopMetrics struct{}

func (NoopMetrics) Tick(string, string)                 {}
func (NoopMetrics) TickDropped()                        {}
func (NoopMetrics) TickBatch(int)                       {}
func (NoopMetrics) TickLatency(time.Duration)           {}
func (NoopMetrics) TriggerFired(string, string, string) {}
func (NoopMetrics) TriggerDelivered()                   {}
func (NoopMetrics) WatcherDrop()                        {}
func (NoopMetrics) AlertsActive(int)                    {}
func (NoopMetrics) Watchers(int)                        {}
func (NoopMetrics) FeedConnected(bool)                  {}

// AlertState is the service-side lifecycle view.
type AlertState string

const (
	StateActive    AlertState = "active"
	StateTriggered AlertState = "triggered"
	StateCancelled AlertState = "cancelled"
)

// Alert is the service-side catalog entry: everything needed to enrich a
// raw engine Trigger into a chrono.v1.Trigger, kept AFTER the engine drops
// its own refs (fire removes them) — that removal race is why this map
// exists.
type Alert struct {
	ID          engine.AlertID
	Symbol      string
	Decimals    uint8
	Venue, Tier string
	PriceType   engine.PriceType
	Direction   engine.Direction
	TargetPrice engine.Price
	State       AlertState
	CreatedAt   time.Time
}

// Core implements both chrono.v1 services.
type Core struct {
	chronov1.UnimplementedAlertServiceServer
	chronov1.UnimplementedFeedServiceServer

	Cat     *catalog.Catalog
	eng     *engine.Engine
	metrics Metrics
	stats   *stats.Stats
	now     func() time.Time

	mu     sync.RWMutex
	alerts map[engine.AlertID]*Alert
}

// NewCore builds the engine with the catalog's dim vocabulary.
func NewCore(cfg engine.Config, cat *catalog.Catalog, m Metrics, st *stats.Stats, now time.Time) *Core {
	cfg.Dims = cat.Dims() // the catalog owns the vocabulary
	return &Core{
		Cat:     cat,
		eng:     engine.New(cfg),
		metrics: m,
		stats:   st,
		now:     func() time.Time { return time.Now() },
		alerts:  make(map[engine.AlertID]*Alert),
	}
}

// Close shuts the engine down. Call only after all servers have stopped
// (the engine contract requires submitters to stop first).
func (c *Core) Close() { c.eng.Close() }

// Engine exposes the engine for tests and /stats.
func (c *Core) Engine() *engine.Engine { return c.eng }

func (c *Core) AlertCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.alerts)
}

func (c *Core) AlertsByState() map[string]int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := map[string]int{}
	for _, a := range c.alerts {
		out[string(a.State)]++
	}
	return out
}

func toEnginePriceType(pt chronov1.PriceType) (engine.PriceType, bool) {
	switch pt {
	case chronov1.PriceType_PRICE_TYPE_BID:
		return engine.PriceBid, true
	case chronov1.PriceType_PRICE_TYPE_ASK:
		return engine.PriceAsk, true
	case chronov1.PriceType_PRICE_TYPE_MID:
		return engine.PriceMid, true
	case chronov1.PriceType_PRICE_TYPE_LAST:
		return engine.PriceLast, true
	default:
		return 0, false
	}
}

func toEngineDirection(d chronov1.Direction) (engine.Direction, bool) {
	switch d {
	case chronov1.Direction_DIRECTION_ABOVE:
		return engine.DirGTE, true
	case chronov1.Direction_DIRECTION_BELOW:
		return engine.DirLTE, true
	default:
		return 0, false
	}
}

// UpsertAlert validates against the catalog, converts the price exactly,
// records the alert service-side BEFORE engine.Upsert (so the trigger pump
// can always enrich), then submits and Syncs — the alert is visible to
// Match when the response returns.
func (c *Core) UpsertAlert(ctx context.Context, req *chronov1.UpsertAlertRequest) (*chronov1.UpsertAlertResponse, error) {
	sym, ok := c.Cat.Symbol(req.GetSymbol())
	if !ok {
		return nil, invalidf("unknown symbol %q", req.GetSymbol())
	}
	pt, ok := toEnginePriceType(req.GetPriceType())
	if !ok {
		return nil, invalidf("price_type is required")
	}
	dir, ok := toEngineDirection(req.GetDirection())
	if !ok {
		return nil, invalidf("direction is required")
	}
	base, err := price.Parse(req.GetTargetPrice(), sym.Decimals)
	if err != nil {
		return nil, invalidf("target price %q: %v (symbol quotes %d decimals)", req.GetTargetPrice(), err, sym.Decimals)
	}
	vv, ok := c.Cat.Value(catalog.DimVenue, req.GetVenue())
	if !ok {
		return nil, invalidf("unknown venue %q (valid: %v)", req.GetVenue(), c.Cat.DimValues(catalog.DimVenue))
	}
	tv, ok := c.Cat.Value(catalog.DimTier, req.GetTier())
	if !ok {
		return nil, invalidf("unknown tier %q (valid: %v)", req.GetTier(), c.Cat.DimValues(catalog.DimTier))
	}
	if req.GetExpiresUnixNanos() != 0 && req.GetExpiresUnixNanos() <= req.GetValidFromUnixNanos() {
		return nil, invalidf("expires at or before valid_from")
	}

	var id engine.AlertID
	if raw := req.GetAlertId(); len(raw) > 0 {
		if len(raw) != 16 {
			return nil, invalidf("alert_id must be 16 bytes, got %d", len(raw))
		}
		copy(id[:], raw)
	} else {
		id = newAlertID()
	}

	// Service catalog first (pump enrichment must never miss), remembering
	// the previous entry so an engine failure restores it.
	entry := &Alert{
		ID: id, Symbol: sym.Name, Decimals: sym.Decimals,
		Venue: req.GetVenue(), Tier: req.GetTier(),
		PriceType: pt, Direction: dir, TargetPrice: engine.Price(base),
		State: StateActive, CreatedAt: c.now(),
	}
	c.mu.Lock()
	prev := c.alerts[id]
	c.alerts[id] = entry
	c.mu.Unlock()

	spec := engine.AlertSpec{
		ID: id, Symbol: sym.Name, PriceType: pt, Direction: dir,
		TargetPrice:    engine.Price(base),
		ValidFrom:      req.GetValidFromUnixNanos(),
		Expires:        req.GetExpiresUnixNanos(),
		AutoDeactivate: req.GetAutoDeactivate(),
		Dims:           engine.Dims(vv, tv),
		Meta: engine.AlertMeta{
			ID: id, Symbol: sym.Name, PriceType: pt, Direction: dir,
			TargetPrice: engine.Price(base), CreatedAt: entry.CreatedAt.UnixNano(),
		},
	}
	if err := c.eng.Upsert(spec); err != nil {
		c.mu.Lock()
		if prev != nil {
			c.alerts[id] = prev
		} else {
			delete(c.alerts, id)
		}
		c.mu.Unlock()
		return nil, mapEngineErr(err)
	}
	c.eng.Sync()
	c.metrics.AlertsActive(c.countState(StateActive))
	return &chronov1.UpsertAlertResponse{AlertId: alertIDString(id)}, nil
}

func (c *Core) countState(s AlertState) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n := 0
	for _, a := range c.alerts {
		if a.State == s {
			n++
		}
	}
	return n
}

// CancelAlert retires an alert. Terminal alerts answer NotFound (the
// engine has already dropped its refs).
func (c *Core) CancelAlert(ctx context.Context, req *chronov1.CancelAlertRequest) (*chronov1.CancelAlertResponse, error) {
	id, err := parseAlertID(req.GetAlertId())
	if err != nil {
		return nil, invalidf("%v", err)
	}
	if err := c.eng.Cancel(id); err != nil {
		return nil, mapEngineErr(err)
	}
	c.mu.Lock()
	if a, ok := c.alerts[id]; ok {
		a.State = StateCancelled
	}
	c.mu.Unlock()
	c.metrics.AlertsActive(c.countState(StateActive))
	return &chronov1.CancelAlertResponse{}, nil
}
