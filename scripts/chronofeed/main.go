// Command chronofeed is the demo/test driver for chronod. It does two
// things, in order:
//
//  1. Seeds the bbolt alert store with a price ladder of fake alerts:
//     N rungs spread uniformly across ±band around each symbol's
//     reference price, alternating ABOVE/BELOW, across every
//     symbol × venue × tier combination. Skipped when the store is
//     already populated or locked by a running chronod.
//  2. Streams a synthetic market into chronod over gRPC, one
//     client-stream per venue. The walk is mean-reverting around the
//     same reference prices the ladder was built from, so price keeps
//     wandering through the rungs and triggers fire at a steady pace —
//     a regular trickle, not one burst.
//
// Alerts are terminal once triggered, so the ladder is a finite trigger
// budget: -alerts and -band set how long the trickle lasts. Delete the
// db file and restart to reseed.
//
// Typical session (feeder first: it creates and seeds the db):
//
//	go run ./scripts/chronofeed -db demo.bbolt &
//	go run ./cmd/chronod -db demo.bbolt
package main

import (
	"context"
	crand "crypto/rand"
	"flag"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/alertstore"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/price"
)

const (
	batchMax   = 64
	flushEvery = 10 * time.Millisecond
	seedChunk  = 5000 // alerts per write tx: one fsync per chunk

	// reversion pulls each tick 0.2% of the way back toward the symbol's
	// reference price. With per-symbol vol sigma (below), the walk
	// hovers in a sigma/√(2·reversion) ≈ 16·sigma band around the
	// reference — the same neighborhood the alert ladder covers.
	reversion = 0.002
)

func main() {
	server := flag.String("server", "localhost:9090", "chronod gRPC address")
	dbPath := flag.String("db", "chrono.bbolt", "alert store path (bbolt); must match chronod's -db")
	rate := flag.Float64("rate", 20000, "target ticks per second, split across venue streams")
	streams := flag.Int("venue-streams", 3, "parallel gRPC streams (one per venue)")
	seed := flag.Uint64("seed", 1, "market RNG seed")
	nAlerts := flag.Int("alerts", 100000, "alert-ladder size to seed (0 skips seeding)")
	band := flag.Float64("band", 0.05, "ladder spread: ±fraction around each symbol's reference price")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if *streams < 1 || *streams > len(catalog.Venues) {
		slog.Error("venue-streams out of range", "got", *streams)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	seedAlerts(*dbPath, *nAlerts, *band)
	if ctx.Err() != nil {
		return
	}
	if err := feed(ctx, *server, *rate, *seed, *streams); err != nil && ctx.Err() == nil {
		slog.Error("feed exit", "err", err)
		os.Exit(1)
	}
	slog.Info("feed stopped")
}

// seedAlerts populates an empty store with the alert ladder. Both skip
// paths (already populated, store locked) are warnings, not errors:
// seeding is best-effort — feeding works against any store content.
func seedAlerts(path string, n int, band float64) {
	if n <= 0 {
		return
	}
	store, err := alertstore.Open(path)
	if err != nil {
		slog.Warn("seed skipped: cannot open alert store (chronod already running with this db?)", "path", path, "err", err)
		return
	}
	defer func() { _ = store.Close() }()

	active, triggered, cancelled, err := store.Counts()
	if err != nil {
		slog.Warn("seed skipped: count failed", "err", err)
		return
	}
	if total := active + triggered + cancelled; total > 0 {
		slog.Info("seed skipped: alert store already populated (delete the db file to reseed)",
			"active", active, "triggered", triggered, "cancelled", cancelled)
		return
	}

	now := time.Now().UnixNano()
	alerts := ladder(n, band, now)
	for i := 0; i < len(alerts); i += seedChunk {
		end := min(i+seedChunk, len(alerts))
		if err := store.PutBatch(alerts[i:end]); err != nil {
			slog.Error("seed failed", "seeded", i, "err", err)
			os.Exit(1)
		}
		if end%200_000 == 0 || end == len(alerts) {
			slog.Info("seeding", "done", end, "total", len(alerts))
		}
	}
	slog.Info("seeded alert ladder", "alerts", len(alerts), "band", band, "db", path)
}

// ladder builds n rungs per symbol uniformly spread through
// (ref·(1−b), ref·(1+b)), alternating ABOVE/BELOW, rotating through
// every symbol × venue × tier combination. ABOVE rungs above the walk's
// anchor fire on the way up, BELOW rungs below it on the way down.
// The spread b scales with the symbol's tick volatility (majors keep
// the -band fraction, memecoins get proportionally wider), so volatile
// symbols don't burn their whole ladder within one standard deviation
// and every symbol depletes its budget at the same pace.
func ladder(n int, band float64, now int64) []alertstore.Alert {
	syms := catalog.Default().Symbols()
	venues, tiers := catalog.Venues, catalog.Tiers
	alerts := make([]alertstore.Alert, 0, n)
	for i := 0; i < n; i++ {
		sym := syms[i%len(syms)]
		venue := venues[(i/len(syms))%len(venues)]
		tier := tiers[(i/(len(syms)*len(venues)))%len(tiers)]
		ref, err := price.Parse(sym.Reference, sym.Decimals)
		if err != nil {
			slog.Error("reference unparseable", "symbol", sym.Name, "err", err)
			os.Exit(1)
		}
		spread := band * sigmaFor(sym.Decimals) / sigmaFor(2)
		frac := (float64(i) + 0.5) / float64(n) // strictly inside (0,1)
		dir, target := engine.DirGTE, ref+int64(float64(ref)*spread*frac)
		if i%2 == 1 {
			dir, target = engine.DirLTE, ref-int64(float64(ref)*spread*frac)
		}
		if target < 1 {
			continue // sub-unit rung on a cheap symbol: not representable
		}
		alerts = append(alerts, alertstore.Alert{
			ID: newID(), Symbol: sym.Name, Decimals: sym.Decimals,
			Venue: venue, Tier: tier,
			PriceType: engine.PriceAsk, Direction: dir,
			TargetPrice: engine.Price(target),
			ValidFrom:   now, State: alertstore.StateActive, CreatedAt: now,
		})
	}
	return alerts
}

// newID returns a UUIDv7: time-ordered like the ones the service hands out.
func newID() engine.AlertID {
	var id engine.AlertID
	_, _ = crand.Read(id[:])
	ms := time.Now().UnixMilli()
	id[0], id[1], id[2], id[3], id[4], id[5] =
		byte(ms>>40), byte(ms>>32), byte(ms>>24), byte(ms>>16), byte(ms>>8), byte(ms)
	id[6] = (id[6] & 0x0f) | 0x70
	id[8] = (id[8] & 0x3f) | 0x80
	return id
}

// feed dials chronod, waits for its health, then runs one stream per venue.
func feed(ctx context.Context, server string, rate float64, seed uint64, streams int) error {
	conn, err := grpc.NewClient(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	// No deadline: the intended order is feeder first, chronod second.
	hc := healthpb.NewHealthClient(conn)
	slog.Info("waiting for chronod", "server", server)
	for {
		check, err := hc.Check(ctx, &healthpb.HealthCheckRequest{})
		if err == nil && check.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			break
		}
		if ctx.Err() != nil {
			return nil // shut down while still waiting
		}
		time.Sleep(250 * time.Millisecond)
	}

	fc := chronov1.NewFeedServiceClient(conn)
	slog.Info("feeding", "server", server, "rate", rate, "seed", seed, "streams", streams)
	errCh := make(chan error, streams)
	var wg sync.WaitGroup
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errCh <- feedVenue(ctx, fc, catalog.Venues[i], seed+uint64(i), rate/float64(streams))
		}(i)
	}
	go func() { wg.Wait(); close(errCh) }()
	for err := range errCh {
		if err != nil && ctx.Err() == nil {
			return err
		}
	}
	return nil
}

// feedVenue streams one venue's synthetic market until ctx is done. Each
// venue walks its own copy of the symbol table (seeded per venue), so
// streams are independent — no shared state, no routing.
func feedVenue(ctx context.Context, fc chronov1.FeedServiceClient, venue string, seed uint64, rate float64) error {
	stream, err := fc.StreamTicks(ctx)
	if err != nil {
		return err
	}
	m := newMarket(venue, seed)
	em := emitter{target: rate}
	batch := make([]*chronov1.Tick, 0, batchMax)
	send := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := stream.Send(&chronov1.TickBatch{Ticks: batch}); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	tick := time.NewTicker(flushEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			_, _ = stream.CloseAndRecv()
			return nil
		case <-tick.C:
			now := time.Now()
			for n := em.take(flushEvery); n > 0; n-- {
				batch = append(batch, m.tick(now))
				if len(batch) >= batchMax {
					if err := send(); err != nil {
						return err
					}
				}
			}
			if err := send(); err != nil {
				return err
			}
		}
	}
}

// market is a per-venue synthetic market: a mean-reverting walk over the
// catalog's symbols, tier-alternating top/deep quotes. All prices are
// exact integer base units rendered by price.Format — floats never leave
// this struct.
type market struct {
	venue     string
	syms      []catalog.Symbol
	base, ref []int64
	sigma     []float64
	rng       *rand.Rand
	symI, tI  int // round-robin cursors
}

// sigmaFor maps quote precision to per-tick volatility: majors are calm,
// memecoins are not.
func sigmaFor(dec uint8) float64 {
	switch dec {
	case 2:
		return 0.0005
	case 4:
		return 0.001
	default:
		return 0.003
	}
}

// halfSpreadFrac: TOP quotes top-of-book (0.5bp), MID a deeper level (5bp).
func halfSpreadFrac(tier string) float64 {
	if tier == catalog.Tiers[0] {
		return 0.00005
	}
	return 0.0005
}

func newMarket(venue string, seed uint64) *market {
	syms := catalog.Default().Symbols()
	m := &market{
		venue: venue,
		syms:  syms,
		base:  make([]int64, len(syms)),
		ref:   make([]int64, len(syms)),
		sigma: make([]float64, len(syms)),
		rng:   rand.New(rand.NewPCG(seed, seed*2654435761+1)),
	}
	for i, s := range syms {
		ref, err := price.Parse(s.Reference, s.Decimals)
		if err != nil {
			panic("chronofeed: reference unparseable: " + err.Error())
		}
		m.base[i], m.ref[i], m.sigma[i] = ref, ref, sigmaFor(s.Decimals)
	}
	return m
}

// tick advances one symbol (round-robin, so every symbol streams at an
// equal rate) and returns the wire-ready quote.
func (m *market) tick(now time.Time) *chronov1.Tick {
	i := m.symI % len(m.syms)
	m.symI++
	// Mean-reverting step: pull toward the reference, then add noise.
	step := reversion*float64(m.ref[i]-m.base[i]) + m.sigma[i]*float64(m.ref[i])*m.rng.NormFloat64()
	b := m.base[i] + int64(step)
	if b < 1 {
		b = 1
	}
	m.base[i] = b

	tier := catalog.Tiers[m.tI%len(catalog.Tiers)]
	m.tI++
	half := int64(float64(b) * halfSpreadFrac(tier))
	if half < 1 {
		half = 1 // sub-cent symbols: a fraction-of-a-base-unit spread rounds to zero
	}
	if half >= b {
		half = b / 2
	}
	return &chronov1.Tick{
		Symbol: m.syms[i].Name,
		Bid:    price.Format(b-half, m.syms[i].Decimals),
		Ask:    price.Format(b+half, m.syms[i].Decimals),
		Venue:  m.venue, Tier: tier, TsUnixNanos: now.UnixNano(),
	}
}

// emitter shapes emission toward a target rate: accumulate the fractional
// budget, hand out whole ticks per interval.
type emitter struct {
	target, acc float64
}

func (e *emitter) take(interval time.Duration) int {
	e.acc += e.target * interval.Seconds()
	n := int(e.acc)
	e.acc -= float64(n)
	return n
}
