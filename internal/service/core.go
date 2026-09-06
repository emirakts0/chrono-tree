// Package service implements the chrono.v1 gRPC services on top of the
// engine: reference-data validation, price conversion at the boundary,
// a service-side alert catalog, and trigger publishing to NATS.
package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/alertstore"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/pub"
	"github.com/emir/chrono-tree/internal/stats"
	"github.com/emir/chrono-tree/internal/price"
)

// Metrics is the observability hook; implementations must be safe for
// concurrent use. NoopMetrics covers tests.
type Metrics interface {
	Tick(venue, tier string)
	TickDropped()
	TickBatch(n int)
	TickLatency(d time.Duration)
	TriggerFired(symbol, venue, tier string)
	TriggerPublished()
	TriggerPublishDropped()
	AlertsActive(n int)
	FeedConnected(b bool)
}

// NoopMetrics discards everything.
type NoopMetrics struct{}

func (NoopMetrics) Tick(string, string)                 {}
func (NoopMetrics) TickDropped()                        {}
func (NoopMetrics) TickBatch(int)                       {}
func (NoopMetrics) TickLatency(time.Duration)           {}
func (NoopMetrics) TriggerFired(string, string, string) {}
func (NoopMetrics) TriggerPublished()                   {}
func (NoopMetrics) TriggerPublishDropped()              {}
func (NoopMetrics) AlertsActive(int)                    {}
func (NoopMetrics) FeedConnected(bool)                  {}

// AlertState is the service-side lifecycle view (string form for the
// monitoring surface; storage uses alertstore.State).
type AlertState = string

const (
	StateActive    AlertState = "active"
	StateTriggered AlertState = "triggered"
	StateCancelled AlertState = "cancelled"
)

// storeState maps the wire/monitoring state name to the store state.
func storeState(s string) (alertstore.State, bool) {
	switch s {
	case StateActive:
		return alertstore.StateActive, true
	case StateTriggered:
		return alertstore.StateTriggered, true
	case StateCancelled:
		return alertstore.StateCancelled, true
	}
	return 0, false
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

	store *alertstore.Store

	// State gauges for /stats and Prometheus — tallies, not a log:
	// maintained alongside store writes, never queried from here.
	active    atomic.Int64
	triggered atomic.Int64
	cancelled atomic.Int64

	pumpCtx    context.Context
	pumpCancel context.CancelFunc
	pumpDone   chan struct{}

	flipCh   chan []alertstore.Fired // enrich → flipper; capacity 2 = backpressure
	flipDone chan struct{}

	pub        pub.Publisher
	pubHealthy atomic.Bool // log publish failures on transition only

	feedEver     atomic.Bool
	feedLastSeen atomic.Int64 // unix nanos

	venueTicks sync.Map // venue name → *atomic.Uint64, interned on first sight
}

// NewCore builds the engine with the catalog's dim vocabulary, replays
// the store's active set into it, and starts the trigger pump. The
// publisher receives every enriched trigger; nil falls back to pub.Noop
// (a nil would otherwise panic at the first deliver, far from the
// cause). A nil store is a programming error — same class as a nil
// catalog — and panics here, before any listener exists.
func NewCore(cfg engine.Config, cat *catalog.Catalog, m Metrics, st *stats.Stats, p pub.Publisher, store *alertstore.Store) *Core {
	if p == nil {
		p = pub.Noop{}
	}
	if store == nil {
		panic("service: alertstore is required")
	}
	cfg.Dims = cat.Dims() // the catalog owns the vocabulary
	c := &Core{
		Cat:     cat,
		eng:     engine.New(cfg),
		store:   store,
		metrics: m,
		stats:   st,
		pub:     p,
		now:     func() time.Time { return time.Now() },
	}
	// Healthy until a publish fails: "recovered" must only ever log after
	// an actual failure, never on the process's first publish.
	c.pubHealthy.Store(true)
	c.replay()
	c.pumpCtx, c.pumpCancel = context.WithCancel(context.Background())
	c.pumpDone = make(chan struct{})
	c.flipCh = make(chan []alertstore.Fired, 2)
	c.flipDone = make(chan struct{})
	go c.pump()
	go c.flipper()
	return c
}

// replay restores the persisted active set into the fresh engine:
// alerts that expired while down flip to cancelled in one batched write;
// the rest re-enter the engine (valid_from still applies — the engine
// gates matching on it). Runs before the pump exists, so nothing can
// fire mid-replay. A store that cannot be read is fatal at construction:
// source of truth or nothing.
func (c *Core) replay() {
	now := c.now()
	var restored, expired int
	var expiredIDs []engine.AlertID
	err := c.store.EachActive(func(a alertstore.Alert) error {
		if a.Expires != 0 && a.Expires <= now.UnixNano() {
			expired++
			expiredIDs = append(expiredIDs, a.ID)
			return nil
		}
		spec := c.specFrom(a)
		if err := c.eng.Upsert(spec); err != nil {
			return fmt.Errorf("replay upsert %s: %w", alertIDString(a.ID), err)
		}
		restored++
		return nil
	})
	if err != nil {
		panic(fmt.Sprintf("service: alert store replay: %v", err))
	}
	if len(expiredIDs) > 0 {
		if _, err := c.store.CancelBatch(expiredIDs); err != nil {
			panic(fmt.Sprintf("service: expire while replaying: %v", err))
		}
	}
	c.eng.Sync()
	if a, tr, cn, err := c.store.Counts(); err != nil {
		panic(fmt.Sprintf("service: alert store counts: %v", err))
	} else {
		c.active.Store(int64(a))
		c.triggered.Store(int64(tr))
		c.cancelled.Store(int64(cn))
	}
	slog.Info("alert store replay",
		"restored", restored, "expired", expired)
}

// specFrom rebuilds the engine submission for a persisted record. The
// record itself re-interns its symbol (at the store's persisted decimals)
// and its venue/tier — replay can never drift from the registry.
func (c *Core) specFrom(a alertstore.Alert) engine.AlertSpec {
	sym, _ := c.Cat.EnsureSymbol(a.Symbol, a.Decimals)
	vv, _ := c.Cat.EnsureValue(catalog.DimVenue, a.Venue)
	tv, _ := c.Cat.EnsureValue(catalog.DimTier, a.Tier)
	return engine.AlertSpec{
		ID: a.ID, Symbol: sym.Name, PriceType: a.PriceType, Direction: a.Direction,
		TargetPrice:    a.TargetPrice,
		ValidFrom:      a.ValidFrom,
		Expires:        a.Expires,
		AutoDeactivate: a.AutoDeactivate,
		Dims:           engine.Dims(vv, tv),
	}
}

// Close stops the pump first (it is the only Triggers() consumer, and the
// engine contract requires submitters to stop before Close), waits for the
// flipper to write every in-flight batch, then stops the engine.
func (c *Core) Close() {
	c.pumpCancel()
	<-c.pumpDone
	<-c.flipDone
	c.eng.Close()
}

// Engine exposes the engine for tests and /stats.
func (c *Core) Engine() *engine.Engine { return c.eng }

func (c *Core) AlertCount() int {
	return int(c.active.Load() + c.triggered.Load() + c.cancelled.Load())
}

func (c *Core) AlertsByState() map[string]int {
	out := map[string]int{}
	for s, n := range map[string]*atomic.Int64{
		StateActive: &c.active, StateTriggered: &c.triggered, StateCancelled: &c.cancelled,
	} {
		if v := n.Load(); v > 0 {
			out[s] = int(v)
		}
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
//
// Scale contract: the target price must be written at the symbol's pinned
// scale exactly (trailing-zero variants are included in the check). For a
// symbol not yet in the catalog, the target's fractional-digit count pins
// the scale for the boot — a coarsely-written target (e.g. "65000" → 0
// decimals) makes every finer tick for that symbol drop until restart.
func (c *Core) UpsertAlert(ctx context.Context, req *chronov1.UpsertAlertRequest) (*chronov1.UpsertAlertResponse, error) {
	dec, serr := price.ScaleOf(req.GetTargetPrice())
	if serr != nil {
		return nil, invalidf("target price %q: %v", req.GetTargetPrice(), serr)
	}
	sym, ok := c.Cat.EnsureSymbol(req.GetSymbol(), dec)
	if !ok {
		return nil, invalidf("symbol %q quotes %d decimals", req.GetSymbol(), sym.Decimals)
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
	vv, ok := c.Cat.EnsureValue(catalog.DimVenue, req.GetVenue())
	if !ok {
		return nil, invalidf("venue vocabulary exhausted")
	}
	tv, ok := c.Cat.EnsureValue(catalog.DimTier, req.GetTier())
	if !ok {
		return nil, invalidf("tier vocabulary exhausted")
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

	// Store first (pump enrichment must never miss), remembering the
	// previous record so an engine failure restores it.
	prev, found, err := c.store.Get(id)
	if err != nil {
		return nil, internalf("alert store: %v", err)
	}
	rec := alertstore.Alert{
		ID: id, Symbol: sym.Name, Decimals: sym.Decimals,
		Venue: req.GetVenue(), Tier: req.GetTier(),
		PriceType: pt, Direction: dir, TargetPrice: engine.Price(base),
		ValidFrom: req.GetValidFromUnixNanos(), Expires: req.GetExpiresUnixNanos(),
		State: alertstore.StateActive, AutoDeactivate: req.GetAutoDeactivate(),
		CreatedAt: c.now().UnixNano(),
	}
	if err := c.store.Put(rec); err != nil {
		return nil, internalf("alert store: %v", err)
	}

	spec := engine.AlertSpec{
		ID: id, Symbol: sym.Name, PriceType: pt, Direction: dir,
		TargetPrice:    engine.Price(base),
		ValidFrom:      req.GetValidFromUnixNanos(),
		Expires:        req.GetExpiresUnixNanos(),
		AutoDeactivate: req.GetAutoDeactivate(),
		Dims:           engine.Dims(vv, tv),
	}
	if err := c.eng.Upsert(spec); err != nil {
		// Roll the store back to the pre-upsert truth.
		if found {
			if err2 := c.store.Put(prev); err2 != nil {
				slog.Error("alert store rollback failed", "id", alertIDString(id), "err", err2)
			}
		} else if err2 := c.store.Delete(id); err2 != nil {
			slog.Error("alert store rollback failed", "id", alertIDString(id), "err", err2)
		}
		return nil, mapEngineErr(err)
	}
	c.eng.Sync()
	if found {
		c.decState(prev.State)
	}
	c.active.Add(1)
	c.metrics.AlertsActive(int(c.active.Load()))
	return &chronov1.UpsertAlertResponse{AlertId: alertIDString(id)}, nil
}

// decState decrements the gauge for a state the catalog left.
func (c *Core) decState(s alertstore.State) {
	switch s {
	case alertstore.StateActive:
		c.active.Add(-1)
	case alertstore.StateTriggered:
		c.triggered.Add(-1)
	case alertstore.StateCancelled:
		c.cancelled.Add(-1)
	}
}

// CancelAlert retires an alert: the store flips active→cancelled, then
// the engine drops its refs. Terminal/unknown alerts answer NotFound.
// A trigger racing the cancel loses the state flip (MarkTriggered sees
// a terminal record and skips) — the engine may still publish one
// in-flight trigger, same observable behavior as the map era.
func (c *Core) CancelAlert(ctx context.Context, req *chronov1.CancelAlertRequest) (*chronov1.CancelAlertResponse, error) {
	id, err := parseAlertID(req.GetAlertId())
	if err != nil {
		return nil, invalidf("%v", err)
	}
	flipped, err := c.store.Cancel(id)
	if err != nil {
		return nil, internalf("alert store: %v", err)
	}
	if !flipped {
		return nil, status.Error(codes.NotFound, "alert not found")
	}
	if err := c.eng.Cancel(id); err != nil &&
		!errors.Is(err, engine.ErrNotFound) && !errors.Is(err, engine.ErrInvalidTransition) {
		return nil, mapEngineErr(err) // fired-and-removed meanwhile: expected, ignored above
	}
	c.active.Add(-1)
	c.cancelled.Add(1)
	c.metrics.AlertsActive(int(c.active.Load()))
	return &chronov1.CancelAlertResponse{}, nil
}

// presentBidAskMid is the Present mask for ticks carrying bid, ask and a
// derived mid.
func presentBidAskMid() uint8 {
	return uint8(1)<<uint(engine.PriceBid) | uint8(1)<<uint(engine.PriceAsk) | uint8(1)<<uint(engine.PriceMid)
}

// maxNameLen bounds what the tick path may intern, mirroring the
// UpsertAlert symbol pattern's length. StreamTicks is client-streaming,
// so the protovalidate interceptor never sees its ticks — without this
// bound an empty or megabyte-scale name would be interned for the boot
// and served via GetCatalog//stats.
const maxNameLen = 20

// nameOK reports whether a tick field may be interned as a name.
func nameOK(s string) bool { return len(s) > 0 && len(s) <= maxNameLen }

// ingestTick converts one wire tick to an engine tick and Matches it.
// Unknown symbol/venue/tier are interned on first sight — the feed
// defines the vocabulary. Errors (all drop this tick only): malformed
// names, price/scale problems, or dim exhaustion.
func (c *Core) ingestTick(t *chronov1.Tick, now time.Time) error {
	if !nameOK(t.GetSymbol()) || !nameOK(t.GetVenue()) || !nameOK(t.GetTier()) {
		return fmt.Errorf("symbol/venue/tier must be 1..%d bytes", maxNameLen)
	}
	sym, ok := c.Cat.Symbol(t.GetSymbol())
	if !ok {
		dec, serr := price.ScaleOf(t.GetBid())
		if serr != nil {
			return fmt.Errorf("bid %q: %v", t.GetBid(), serr)
		}
		if sym, ok = c.Cat.EnsureSymbol(t.GetSymbol(), dec); !ok {
			return fmt.Errorf("symbol %q quotes %d decimals", t.GetSymbol(), sym.Decimals)
		}
	}
	vv, ok := c.Cat.Value(catalog.DimVenue, t.GetVenue())
	if !ok {
		if vv, ok = c.Cat.EnsureValue(catalog.DimVenue, t.GetVenue()); !ok {
			return fmt.Errorf("venue vocabulary exhausted")
		}
	}
	tv, ok := c.Cat.Value(catalog.DimTier, t.GetTier())
	if !ok {
		if tv, ok = c.Cat.EnsureValue(catalog.DimTier, t.GetTier()); !ok {
			return fmt.Errorf("tier vocabulary exhausted")
		}
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
	if anyCtr, ok := c.venueTicks.Load(t.GetVenue()); ok {
		anyCtr.(*atomic.Uint64).Add(1)
	} else {
		ctr, _ := c.venueTicks.LoadOrStore(t.GetVenue(), &atomic.Uint64{})
		ctr.(*atomic.Uint64).Add(1)
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

// VenueTicks reports accepted ticks per venue.
func (c *Core) VenueTicks() map[string]uint64 {
	out := map[string]uint64{}
	c.venueTicks.Range(func(v, ctr any) bool {
		out[v.(string)] = ctr.(*atomic.Uint64).Load()
		return true
	})
	return out
}

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

// pumpBatch is the ring drain width: one BatchGet read tx per 4096
// triggers. The old 64-wide batches made store-transaction overhead the
// pump's dominant cost (~1.7 ms/trigger, ~4.4k flips/s — 2026-09-06
// campaign). 4096 keeps a read tx well under a second of work while
// amortizing per-tx cost across three orders of magnitude more triggers.
const pumpBatch = 4096

// pump drains the engine's trigger ring, enriches and publishes each
// batch (enrichBatch), and hands the flips to the flipper goroutine over
// a capacity-2 channel: enriching batch N+1 overlaps flipping batch N.
// It is the ONLY Triggers() consumer. Publish order is this goroutine's
// order; flip order is free (writes are idempotent).
func (c *Core) pump() {
	defer close(c.pumpDone)
	buf := make([]engine.Trigger, pumpBatch)
	for {
		n := c.eng.Triggers().PopBatch(buf)
		if n == 0 {
			select {
			case <-c.pumpCtx.Done():
				close(c.flipCh)
				return
			case <-time.After(500 * time.Microsecond):
			}
			continue
		}
		if fired := c.enrichBatch(buf[:n]); len(fired) > 0 {
			c.flipCh <- fired
		}
	}
}

// flipper is the pipeline's write stage: one MarkTriggeredBatch per
// enriched batch, then the active/triggered gauges — they track store
// truth, which lands here. Runs until the pump closes the channel and
// every in-flight batch is written.
func (c *Core) flipper() {
	defer close(c.flipDone)
	for fired := range c.flipCh {
		flipped, err := c.store.MarkTriggeredBatch(fired)
		if err != nil {
			// Same contract as the synchronous era: records stay active
			// and re-arm via replay at the next restart.
			slog.Warn("mark triggered failed; records stay active until restart", "err", err)
			continue
		}
		c.active.Add(-int64(flipped))
		c.triggered.Add(int64(flipped))
	}
}

// enrichBatch is the pipeline's read stage: one read tx resolves every
// alert in a drained batch, each active alert's trigger is published,
// and the published IDs return for the flipper to write. A store read
// failure counts the batch as fired and leaves the records active — the
// engine has already dropped its refs at fire time, so they re-enter via
// replay at the next restart: visible duplication beats silent loss.
func (c *Core) enrichBatch(batch []engine.Trigger) []alertstore.Fired {
	ids := make([]engine.AlertID, len(batch))
	for i := range batch {
		ids[i] = batch[i].ID
	}
	recs, err := c.store.BatchGet(ids)
	now := c.now()
	if err != nil {
		for range batch {
			c.stats.TriggersFired.Add(1)
			c.stats.FireRate.Add(1, now)
		}
		slog.Warn("trigger enrichment failed; batch lost, records stay active until restart", "err", err)
		return nil
	}
	fired := make([]alertstore.Fired, 0, len(batch))
	for i := range batch {
		tr := &batch[i]
		c.stats.TriggersFired.Add(1)
		c.stats.FireRate.Add(1, now)
		a, ok := recs[tr.ID]
		if !ok || a.State != alertstore.StateActive {
			continue // unknown or no-longer-active: count, don't ship
		}
		out := pub.Trigger{
			AlertID:          alertIDString(tr.ID),
			Symbol:           a.Symbol,
			Venue:            a.Venue,
			Tier:             a.Tier,
			FiredPrice:       price.Format(int64(tr.Price), a.Decimals),
			FiredAtUnixNanos: tr.TS,
			Direction:        directionOf(a.Direction),
			TargetPrice:      price.Format(int64(a.TargetPrice), a.Decimals),
		}
		c.metrics.TriggerFired(a.Symbol, a.Venue, a.Tier)
		if err := c.pub.Publish(out); err != nil {
			// Drop-and-count: the pump never blocks, never retries (spec §4).
			c.stats.TriggersPublishDropped.Add(1)
			c.metrics.TriggerPublishDropped()
			if c.pubHealthy.CompareAndSwap(true, false) {
				slog.Warn("trigger publish failing; dropping until NATS recovers", "err", err)
			}
			continue
		}
		if !c.pubHealthy.Swap(true) {
			slog.Info("trigger publishing recovered")
		}
		c.stats.TriggersPublished.Add(1)
		c.metrics.TriggerPublished()
		fired = append(fired, alertstore.Fired{ID: a.ID, Price: engine.Price(tr.Price), At: tr.TS})
	}
	return fired
}

func directionOf(d engine.Direction) string {
	if d == engine.DirLTE {
		return "BELOW"
	}
	return "ABOVE"
}

// NATSConnected reports the publisher's connection state for /stats and
// the chrono_nats_connected gauge. Observability only — never readiness.
func (c *Core) NATSConnected() bool { return c.pub.Connected() }

// AlertView is the read-only shape of a stored alert for the monitoring
// surface. Prices are decimal strings, ready to render.
type AlertView struct {
	ID                 string `json:"id"`
	Symbol             string `json:"symbol"`
	Venue              string `json:"venue"`
	Tier               string `json:"tier"`
	PriceType          string `json:"price_type"`
	Direction          string `json:"direction"`
	TargetPrice        string `json:"target_price"`
	State              string `json:"state"`
	ValidFromUnixNanos int64  `json:"valid_from_unix_nanos"`
	ExpiresUnixNanos   int64  `json:"expires_unix_nanos"`
	CreatedAtUnixNanos int64  `json:"created_at_unix_nanos"`
	FiredPrice         string `json:"fired_price"`         // "" until triggered
	FiredAtUnixNanos   int64  `json:"fired_at_unix_nanos"` // 0 until triggered
}

// AlertFilter selects catalog alerts for the inquiry API. Empty string
// fields match anything; Direction is "ABOVE" or "BELOW" (anything else
// matches nothing). Limit/Offset are applied after sorting; Limit <= 0
// means no cap.
type AlertFilter struct {
	State, Symbol, Venue, Tier, Direction string
	Limit, Offset                         int
}

func priceTypeString(pt engine.PriceType) string {
	switch pt {
	case engine.PriceBid:
		return "BID"
	case engine.PriceAsk:
		return "ASK"
	case engine.PriceMid:
		return "MID"
	default:
		return "LAST"
	}
}

func viewOf(a *alertstore.Alert) AlertView {
	return AlertView{
		ID:                 alertIDString(a.ID),
		Symbol:             a.Symbol,
		Venue:              a.Venue,
		Tier:               a.Tier,
		PriceType:          priceTypeString(a.PriceType),
		Direction:          directionOf(a.Direction),
		TargetPrice:        price.Format(int64(a.TargetPrice), a.Decimals),
		State:              a.State.String(),
		ValidFromUnixNanos: a.ValidFrom,
		ExpiresUnixNanos:   a.Expires,
		CreatedAtUnixNanos: a.CreatedAt,
		FiredPrice:         firedPriceString(a),
		FiredAtUnixNanos:   a.FiredAt,
	}
}

func firedPriceString(a *alertstore.Alert) string {
	if a.State != alertstore.StateTriggered {
		return ""
	}
	return price.Format(int64(a.FiredPrice), a.Decimals)
}

// GetAlert resolves one alert by its string id (the form UpsertAlert
// returned). ok is false for unknown or malformed ids.
func (c *Core) GetAlert(id string) (AlertView, bool) {
	raw, err := parseAlertID(id)
	if err != nil {
		return AlertView{}, false
	}
	a, found, err := c.store.Get(raw)
	if err != nil || !found {
		return AlertView{}, false
	}
	return viewOf(&a), true
}

// ListAlerts returns one page of the filtered catalog, newest first,
// plus the total match count. Unknown state/direction strings match
// nothing (empty page), preserving the previous contract.
func (c *Core) ListAlerts(f AlertFilter) ([]AlertView, int) {
	sf := alertstore.Filter{
		Symbol: f.Symbol, Venue: f.Venue, Tier: f.Tier,
		Limit: f.Limit, Offset: f.Offset,
	}
	if f.State != "" {
		st, ok := storeState(f.State)
		if !ok {
			return nil, 0
		}
		sf.State, sf.HasState = st, true
	}
	switch f.Direction {
	case "":
	case "ABOVE":
		sf.Direction, sf.HasDirection = engine.DirGTE, true
	case "BELOW":
		sf.Direction, sf.HasDirection = engine.DirLTE, true
	default:
		return nil, 0
	}
	items, total, err := c.store.Query(sf)
	if err != nil {
		slog.Error("alert query failed", "err", err)
		return nil, 0
	}
	views := make([]AlertView, 0, len(items))
	for i := range items {
		views = append(views, viewOf(&items[i]))
	}
	return views, total
}
