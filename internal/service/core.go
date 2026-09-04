// Package service implements the chrono.v1 gRPC services on top of the
// engine: reference-data validation, price conversion at the boundary,
// a service-side alert catalog, and (Task 6) trigger fan-out.
package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

	pumpCtx    context.Context
	pumpCancel context.CancelFunc
	pumpDone   chan struct{}

	hubMu    sync.RWMutex
	watchers map[*watcher]struct{}

	feedEver     atomic.Bool
	feedLastSeen atomic.Int64 // unix nanos
}

// watcher is one connected WatchTriggers stream. ch is buffered; a full
// channel at broadcast time disconnects the watcher (drop-and-log — the
// pump must never block, same philosophy as the engine's trigger ring).
type watcher struct {
	ch chan *chronov1.Trigger
}

func newWatcher() *watcher { return &watcher{ch: make(chan *chronov1.Trigger, 256)} }

// NewCore builds the engine with the catalog's dim vocabulary.
func NewCore(cfg engine.Config, cat *catalog.Catalog, m Metrics, st *stats.Stats, now time.Time) *Core {
	cfg.Dims = cat.Dims() // the catalog owns the vocabulary
	c := &Core{
		Cat:     cat,
		eng:     engine.New(cfg),
		metrics: m,
		stats:   st,
		now:     func() time.Time { return time.Now() },
		alerts:  make(map[engine.AlertID]*Alert),
	}
	c.pumpCtx, c.pumpCancel = context.WithCancel(context.Background())
	c.pumpDone = make(chan struct{})
	c.watchers = make(map[*watcher]struct{})
	go c.pump()
	return c
}

// Close stops the pump first (it is the only Triggers() consumer, and the
// engine contract requires submitters to stop before Close), then the engine.
func (c *Core) Close() {
	c.pumpCancel()
	<-c.pumpDone
	c.eng.Close()
}

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

// errUnknownRefdata marks the batch-reject class (spec §8): a tick naming
// a symbol/venue/tier outside the catalog means the feed is buggy — the
// whole batch is rejected loudly.
var errUnknownRefdata = errors.New("unknown reference data")

// presentBidAskMid is the Present mask for ticks carrying bid, ask and a
// derived mid.
func presentBidAskMid() uint8 {
	return uint8(1)<<uint(engine.PriceBid) | uint8(1)<<uint(engine.PriceAsk) | uint8(1)<<uint(engine.PriceMid)
}

// ingestTick converts one wire tick to an engine tick and Matches it.
// Errors: wrapped errUnknownRefdata (batch reject) or a price.Parse error
// (drop this tick only).
func (c *Core) ingestTick(t *chronov1.Tick, now time.Time) error {
	sym, ok := c.Cat.Symbol(t.GetSymbol())
	if !ok {
		return fmt.Errorf("%q: %w", t.GetSymbol(), errUnknownRefdata)
	}
	vv, ok := c.Cat.Value(catalog.DimVenue, t.GetVenue())
	if !ok {
		return fmt.Errorf("venue %q: %w", t.GetVenue(), errUnknownRefdata)
	}
	tv, ok := c.Cat.Value(catalog.DimTier, t.GetTier())
	if !ok {
		return fmt.Errorf("tier %q: %w", t.GetTier(), errUnknownRefdata)
	}
	bid, err := price.Parse(t.GetBid(), sym.Decimals)
	if err != nil {
		return fmt.Errorf("bid %q: %v", t.GetBid(), err)
	}
	ask, err := price.Parse(t.GetAsk(), sym.Decimals)
	if err != nil {
		return fmt.Errorf("ask %q: %v", t.GetAsk(), err)
	}
	ts := t.GetTsUnixNanos()
	c.eng.Match(&engine.Tick{
		Symbol:  sym.Name,
		Bid:     engine.Price(bid),
		Ask:     engine.Price(ask),
		Mid:     engine.Price((bid + ask) / 2),
		Present: presentBidAskMid(),
		TS:      ts,
		Dims:    engine.Dims(vv, tv),
	})
	c.stats.Ticks.Add(1)
	c.stats.TickRate.Add(1, now)
	c.metrics.Tick(t.GetVenue(), t.GetTier())
	if ts > 0 {
		c.metrics.TickLatency(now.Sub(time.Unix(0, ts)))
	}
	return nil
}

// StreamTicks ingests a client-stream of tick batches until EOF, then
// reports per-stream totals.
func (c *Core) StreamTicks(ss chronov1.FeedService_StreamTicksServer) error {
	var accepted, dropped uint64
	first := true
	for {
		batch, err := ss.Recv()
		if err == io.EOF {
			return ss.SendAndClose(&chronov1.FeedStatus{Accepted: accepted, Dropped: dropped})
		}
		if err != nil {
			return err
		}
		now := c.now()
		if first {
			c.feedEver.Store(true)
			c.metrics.FeedConnected(true)
			first = false
		}
		c.feedLastSeen.Store(now.UnixNano())
		c.metrics.TickBatch(len(batch.GetTicks()))
		for _, tk := range batch.GetTicks() {
			if err := c.ingestTick(tk, now); err != nil {
				if errors.Is(err, errUnknownRefdata) {
					return status.Errorf(codes.InvalidArgument, "batch rejected: %v", err)
				}
				dropped++
				c.stats.TicksDropped.Add(1)
				c.metrics.TickDropped()
				continue
			}
			accepted++
		}
	}
}

// FeedLastSeen reports the last accepted-batch time (zero before any).
func (c *Core) FeedLastSeen() time.Time {
	ns := c.feedLastSeen.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func (c *Core) FeedEverConnected() bool { return c.feedEver.Load() }

// GetCatalog serves the reference data.
func (c *Core) GetCatalog(ctx context.Context, _ *chronov1.CatalogRequest) (*chronov1.CatalogReply, error) {
	syms := make([]*chronov1.SymbolInfo, 0, len(c.Cat.Symbols()))
	for _, s := range c.Cat.Symbols() {
		syms = append(syms, &chronov1.SymbolInfo{Symbol: s.Name, Decimals: uint32(s.Decimals), ReferencePrice: s.Reference})
	}
	dims := make([]*chronov1.DimInfo, 0, len(c.Cat.Dims()))
	for _, d := range c.Cat.Dims() {
		dims = append(dims, &chronov1.DimInfo{Name: d, Values: c.Cat.DimValues(d)})
	}
	return &chronov1.CatalogReply{Symbols: syms, Dims: dims}, nil
}

func (c *Core) WatcherCount() int {
	c.hubMu.RLock()
	defer c.hubMu.RUnlock()
	return len(c.watchers)
}

// pump drains the engine's trigger ring and fans enriched triggers out to
// every watcher. It is the ONLY Triggers() consumer.
func (c *Core) pump() {
	defer close(c.pumpDone)
	buf := make([]engine.Trigger, 64)
	for {
		n := c.eng.Triggers().PopBatch(buf)
		if n == 0 {
			select {
			case <-c.pumpCtx.Done():
				return
			case <-time.After(500 * time.Microsecond):
			}
			continue
		}
		now := c.now()
		for i := range buf[:n] {
			c.deliver(&buf[i], now)
		}
	}
}

// deliver enriches one engine trigger from the service catalog and
// broadcasts it. The catalog entry survives the engine's own fire-time
// refs cleanup — that removal race is why the service catalog exists.
func (c *Core) deliver(tr *engine.Trigger, now time.Time) {
	c.mu.RLock()
	a := c.alerts[tr.ID]
	var state AlertState
	if a != nil {
		state = a.State
	}
	c.mu.RUnlock()
	c.stats.TriggersFired.Add(1)
	c.stats.FireRate.Add(1, now)
	if a == nil || state != StateActive {
		// Fired for an alert we no longer consider active (cancelled
		// concurrently, or a terminal replacement race). Count, don't ship.
		return
	}
	c.mu.Lock()
	a.State = StateTriggered
	c.mu.Unlock()
	out := &chronov1.Trigger{
		AlertId:          alertIDString(tr.ID),
		Symbol:           a.Symbol,
		Venue:            a.Venue,
		Tier:             a.Tier,
		FiredPrice:       price.Format(int64(tr.Price), a.Decimals),
		FiredAtUnixNanos: tr.TS,
		Direction:        fromEngineDirection(a.Direction),
		TargetPrice:      price.Format(int64(a.TargetPrice), a.Decimals),
	}
	c.metrics.TriggerFired(a.Symbol, a.Venue, a.Tier)
	c.broadcast(out)
}

// broadcast fans out to all watchers; a full watcher is disconnected, not
// blocking the pump.
func (c *Core) broadcast(tr *chronov1.Trigger) {
	c.hubMu.RLock()
	targets := make([]*watcher, 0, len(c.watchers))
	for w := range c.watchers {
		targets = append(targets, w)
	}
	c.hubMu.RUnlock()
	for _, w := range targets {
		select {
		case w.ch <- tr:
		default:
			c.hubMu.Lock()
			delete(c.watchers, w)
			n := len(c.watchers)
			c.hubMu.Unlock()
			close(w.ch)
			c.stats.WatcherDrops.Add(1)
			c.metrics.WatcherDrop()
			c.metrics.Watchers(n)
		}
	}
}

func fromEngineDirection(d engine.Direction) chronov1.Direction {
	if d == engine.DirLTE {
		return chronov1.Direction_DIRECTION_BELOW
	}
	return chronov1.Direction_DIRECTION_ABOVE
}

// WatchTriggers registers a watcher and streams until the client goes or
// the watcher is evicted for being too slow.
func (c *Core) WatchTriggers(_ *chronov1.WatchTriggersRequest, ss chronov1.AlertService_WatchTriggersServer) error {
	w := newWatcher()
	c.hubMu.Lock()
	c.watchers[w] = struct{}{}
	n := len(c.watchers)
	c.hubMu.Unlock()
	c.metrics.Watchers(n)
	defer func() {
		c.hubMu.Lock()
		if _, ok := c.watchers[w]; ok {
			delete(c.watchers, w)
			n = len(c.watchers)
		} else {
			n = -1 // already evicted by broadcast
		}
		c.hubMu.Unlock()
		if n >= 0 {
			c.metrics.Watchers(n)
		}
	}()
	for {
		select {
		case <-ss.Context().Done():
			return nil
		case tr, ok := <-w.ch:
			if !ok {
				return status.Error(codes.ResourceExhausted, "watcher too slow; disconnected")
			}
			if err := ss.Send(tr); err != nil {
				return err
			}
			c.stats.TriggersDelivered.Add(1)
			c.metrics.TriggerDelivered()
		}
	}
}
