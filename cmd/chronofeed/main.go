// Command chronofeed is a synthetic crypto market-data feed: it walks the
// catalog's symbols and streams ticks into chronod over gRPC, one
// client-stream per venue, at a configurable target rate.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/feed"
)

const (
	batchMax     = 64
	flushEvery   = 10 * time.Millisecond
	backoffStart = 100 * time.Millisecond
	backoffMax   = 2 * time.Second
)

func main() {
	server := flag.String("server", "localhost:9090", "chronod gRPC address")
	rate := flag.Float64("rate", 20000, "target ticks per second")
	seed := flag.Uint64("seed", 1, "market RNG seed")
	streams := flag.Int("venue-streams", 3, "parallel gRPC streams (one per venue, 1..3)")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if *streams < 1 || *streams > len(catalog.Venues) {
		slog.Error("venue-streams out of range", "got", *streams)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := grpc.NewClient(*server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("dial", "err", err)
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	// Wait for chronod health before generating.
	hc := healthpb.NewHealthClient(conn)
	deadline := time.Now().Add(30 * time.Second)
	for {
		check, err := hc.Check(ctx, &healthpb.HealthCheckRequest{})
		if err == nil && check.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			slog.Error("chronod never became healthy", "err", err)
			os.Exit(1)
		}
		time.Sleep(250 * time.Millisecond)
	}

	slog.Info("feeding", "server", *server, "rate", *rate, "seed", *seed, "streams", *streams)
	if err := run(ctx, conn, *rate, *seed, *streams); err != nil && ctx.Err() == nil {
		slog.Error("feed exit", "err", err)
		os.Exit(1)
	}
	slog.Info("feed stopped")
}

// run drives the market and the per-venue streams until ctx is done.
func run(ctx context.Context, conn *grpc.ClientConn, rate float64, seed uint64, streams int) error {
	fc := chronov1.NewFeedServiceClient(conn)

	// Cross-check the server's reference data against ours — both are
	// compiled from the same table; drift means version skew.
	if err := verifyCatalog(ctx, fc); err != nil {
		return err
	}

	market := feed.NewMarket(catalog.Default().Symbols(), catalog.Venues[:streams], catalog.Tiers, seed)
	emitter := feed.NewEmitter(rate)

	// One buffered channel per venue; the dispatcher routes ticks by msg.Venue.
	type venueCh struct {
		name string
		ch   chan *chronov1.Tick
	}
	chs := make([]venueCh, streams)
	for i := range chs {
		chs[i] = venueCh{name: catalog.Venues[i], ch: make(chan *chronov1.Tick, batchMax*2)}
	}
	byVenue := make(map[string]chan *chronov1.Tick, streams)
	for _, vc := range chs {
		byVenue[vc.name] = vc.ch
	}

	// Venue stream workers.
	errCh := make(chan error, streams)
	for i := range chs {
		go func(name string, ch chan *chronov1.Tick) {
			errCh <- streamVenue(ctx, fc, name, ch)
		}(chs[i].name, chs[i].ch)
	}

	// Producer: emit at the target rate, stamp, route. Note this may block
	// writing to a venue channel whose worker is in backoff — the buffer is
	// 2×batch and backoff is capped, so bursts self-limit; this is
	// intentional pressure shaping.
	tick := time.NewTicker(flushEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			for _, vc := range chs { // drain signal: close inputs, workers finish
				close(vc.ch)
			}
			for range chs { // wait for workers; they return nil on closed input
				if err := <-errCh; err != nil && err != context.Canceled {
					return err
				}
			}
			return nil // graceful drain after cancellation is not an error
		case <-tick.C:
			now := time.Now()
			for n := emitter.Take(flushEvery); n > 0; n-- {
				msg := market.Tick(now)
				byVenue[msg.Venue] <- &chronov1.Tick{
					Symbol: msg.Symbol, Bid: msg.Bid, Ask: msg.Ask,
					Venue: msg.Venue, Tier: msg.Tier, TsUnixNanos: now.UnixNano(),
				}
			}
		}
	}
}

// streamVenue maintains one StreamTicks RPC: batch ≤64, flush on full or
// every flushEvery, reconnect with capped backoff on error. Live-only —
// nothing is replayed after a reconnect.
func streamVenue(ctx context.Context, fc chronov1.FeedServiceClient, venue string, ch chan *chronov1.Tick) error {
	backoff := backoffStart
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := pumpStream(ctx, fc, venue, ch)
		if ctx.Err() != nil {
			return nil
		}
		slog.Warn("stream ended; reconnecting", "venue", venue, "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > backoffMax {
			backoff = backoffMax
		}
	}
}

func pumpStream(ctx context.Context, fc chronov1.FeedServiceClient, venue string, ch chan *chronov1.Tick) error {
	stream, err := fc.StreamTicks(ctx)
	if err != nil {
		return err
	}
	batch := make([]*chronov1.Tick, 0, batchMax)
	flush := time.NewTicker(flushEvery)
	defer flush.Stop()
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
	for {
		select {
		case <-ctx.Done():
			_, _ = stream.CloseAndRecv()
			return nil
		case <-flush.C:
			if err := send(); err != nil {
				return err
			}
		case tk, ok := <-ch:
			if !ok {
				_, err := stream.CloseAndRecv() // FeedStatus discarded
				return err
			}
			batch = append(batch, tk)
			if len(batch) >= batchMax {
				if err := send(); err != nil {
					return err
				}
			}
		}
	}
}

// verifyCatalog fails fast if server and feed disagree on reference data.
func verifyCatalog(ctx context.Context, fc chronov1.FeedServiceClient) error {
	reply, err := fc.GetCatalog(ctx, &chronov1.CatalogRequest{})
	if err != nil {
		return err
	}
	want := catalog.Default()
	if got := len(reply.GetSymbols()); got != len(want.Symbols()) {
		return fmt.Errorf("catalog skew: server has %d symbols, local has %d", got, len(want.Symbols()))
	}
	for _, s := range reply.GetSymbols() {
		w, ok := want.Symbol(s.GetSymbol())
		if !ok || uint8(s.GetDecimals()) != w.Decimals || s.GetReferencePrice() != w.Reference {
			return fmt.Errorf("catalog skew at %s: server=%d/%s local=%d/%s",
				s.GetSymbol(), s.GetDecimals(), s.GetReferencePrice(), w.Decimals, w.Reference)
		}
	}
	return nil
}
