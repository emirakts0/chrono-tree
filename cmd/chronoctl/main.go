// Command chronoctl is the control client: register alerts or seed the
// alert set. Triggers are observed on NATS, not through this client.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/feed"
	"github.com/emir/chrono-tree/price"
)

type Clients struct {
	Alerts chronov1.AlertServiceClient
	Feed   chronov1.FeedServiceClient
}

func dial(server string) (*grpc.ClientConn, error) {
	return grpc.NewClient(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func main() {
	server := flag.String("server", "localhost:9090", "chronod gRPC address")
	// Subcommand flags.
	alertCmd := flag.NewFlagSet("alert", flag.ExitOnError)
	alertPair := alertCmd.String("pair", "BTCUSDT", "symbol")
	alertVenue := alertCmd.String("venue", "ATLAS", "venue dim value")
	alertTier := alertCmd.String("tier", "TOP", "tier dim value")
	alertDir := alertCmd.String("dir", "above", "above|below")
	alertType := alertCmd.String("type", "ask", "bid|ask|mid|last")
	alertPrice := alertCmd.String("price", "", "target price (decimal string)")
	seedCmd := flag.NewFlagSet("seed", flag.ExitOnError)
	seedN := seedCmd.Int("n", 1000, "alerts to seed")
	seedSeed := seedCmd.Uint64("seed", 1, "RNG seed")
	seedWorkers := seedCmd.Int("workers", 8, "concurrent upsert streams")
	// Global flags come before the subcommand: Parse stops at the first
	// non-flag argument, which is the subcommand name.
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: chronoctl [flags] <alert|seed> ...")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := dial(*server)
	if err != nil {
		slog.Error("dial", "err", err)
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()
	c := Clients{Alerts: chronov1.NewAlertServiceClient(conn), Feed: chronov1.NewFeedServiceClient(conn)}

	switch flag.Arg(0) {
	case "alert":
		if err := alertCmd.Parse(flag.Args()[1:]); err != nil {
			os.Exit(2)
		}
		if err := runAlert(ctx, c, *alertPair, *alertVenue, *alertTier, *alertDir, *alertType, *alertPrice); err != nil {
			slog.Error("alert", "err", err)
			os.Exit(1)
		}
	case "seed":
		if err := seedCmd.Parse(flag.Args()[1:]); err != nil {
			os.Exit(2)
		}
		if err := runSeed(ctx, c, os.Stdout, *seedSeed, *seedN, *seedWorkers); err != nil {
			slog.Error("seed", "err", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand", flag.Arg(0))
		os.Exit(2)
	}
}

func priceTypeOf(s string) (chronov1.PriceType, error) {
	switch s {
	case "bid":
		return chronov1.PriceType_PRICE_TYPE_BID, nil
	case "ask":
		return chronov1.PriceType_PRICE_TYPE_ASK, nil
	case "mid":
		return chronov1.PriceType_PRICE_TYPE_MID, nil
	case "last":
		return chronov1.PriceType_PRICE_TYPE_LAST, nil
	}
	return 0, fmt.Errorf("unknown price type %q", s)
}

func dirOf(s string) (chronov1.Direction, error) {
	switch s {
	case "above":
		return chronov1.Direction_DIRECTION_ABOVE, nil
	case "below":
		return chronov1.Direction_DIRECTION_BELOW, nil
	}
	return 0, fmt.Errorf("unknown direction %q", s)
}

func runAlert(ctx context.Context, c Clients, pair, venue, tier, dir, ptype, priceStr string) error {
	pt, err := priceTypeOf(ptype)
	if err != nil {
		return err
	}
	d, err := dirOf(dir)
	if err != nil {
		return err
	}
	resp, err := c.Alerts.UpsertAlert(ctx, &chronov1.UpsertAlertRequest{
		Symbol: pair, PriceType: pt, Direction: d, TargetPrice: priceStr,
		Venue: venue, Tier: tier,
	})
	if err != nil {
		return err
	}
	fmt.Println(resp.GetAlertId())
	return nil
}

// runSeed registers nAlerts dim-scoped ABOVE alerts at reference ±2%
// across random pairs/venues/tiers, plus one deliberate per-venue
// fan-out set on BTCUSDT, then returns. Triggers are observed on NATS
// (`nats sub chrono.triggers.>`), never through this client.
func runSeed(ctx context.Context, c Clients, out io.Writer, seed uint64, nAlerts, workers int) error {
	cat := catalog.Default()
	syms := cat.Symbols()
	rng := rand.New(rand.NewChaCha8(*feed.SeedBytes(seed)))

	seedOne := func(sym catalog.Symbol, venue, tier string, target int64) error {
		_, err := c.Alerts.UpsertAlert(ctx, &chronov1.UpsertAlertRequest{
			Symbol: sym.Name, PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
			Direction:   chronov1.Direction_DIRECTION_ABOVE,
			TargetPrice: price.Format(target, sym.Decimals),
			Venue:       venue, Tier: tier,
		})
		return err
	}

	venues, tiers := cat.DimValues(catalog.DimVenue), cat.DimValues(catalog.DimTier)
	if workers < 1 {
		workers = 1
	}
	// The RNG is not goroutine-safe, so specs are generated sequentially in
	// bounded batches (determinism unchanged) and dispatched across workers:
	// per-alert UpsertAlert latency (a mutation-queue Sync each) only
	// pipelines when several are in flight.
	type spec struct {
		sym    catalog.Symbol
		venue  string
		tier   string
		target int64
	}
	batch := make([]spec, 0, 4096)
	done := uint64(0)
	nextProgress := uint64(50_000)
	errCh := make(chan error, workers)
	dispatch := func() error {
		var wg sync.WaitGroup
		wg.Add(len(batch))
		for i := range batch {
			s := &batch[i]
			go func() {
				defer wg.Done()
				if err := seedOne(s.sym, s.venue, s.tier, s.target); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			}()
		}
		wg.Wait()
		done += uint64(len(batch))
		if done >= nextProgress {
			fmt.Fprintf(os.Stderr, "seeding: %d/%d\n", done, nAlerts)
			for done >= nextProgress {
				nextProgress += 50_000
			}
		}
		select {
		case err := <-errCh:
			return err
		default:
			return nil
		}
	}
	for generated := 0; generated < nAlerts; {
		batch = batch[:0]
		for len(batch) < 4096 && generated < nAlerts {
			sym := syms[rng.IntN(len(syms))]
			venue, tier := venues[rng.IntN(len(venues))], tiers[rng.IntN(len(tiers))]
			ref, err := price.Parse(sym.Reference, sym.Decimals)
			if err != nil {
				return err
			}
			f := 0.98 + 0.04*rng.Float64() // ±2% of reference
			batch = append(batch, spec{sym: sym, venue: venue, tier: tier, target: int64(float64(ref)*f + 0.5)})
			generated++
		}
		if err := dispatch(); err != nil {
			return err
		}
	}
	// Fan-out set: same target on BTCUSDT ask, one alert per venue — the
	// "any venue" pattern is the caller's fan-out, one alert per value.
	var btc catalog.Symbol
	for _, s := range syms {
		if s.Name == "BTCUSDT" {
			btc = s
		}
	}
	for _, v := range venues {
		ref, err := price.Parse(btc.Reference, btc.Decimals)
		if err != nil {
			return err
		}
		f := 0.98 + 0.04*rng.Float64()
		if err := seedOne(btc, v, catalog.Tiers[0], int64(float64(ref)*f+0.5)); err != nil {
			return err
		}
	}
	fmt.Fprintf(out, "seeded %d alerts (incl. %d-venue BTCUSDT fan-out)\n", nAlerts+len(venues), len(venues))
	return nil
}
