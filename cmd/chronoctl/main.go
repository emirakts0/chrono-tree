// Command chronoctl is the demo client: register alerts, watch triggers,
// or run the full demo loop.
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
	"syscall"
	"time"

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
	demoCmd := flag.NewFlagSet("demo", flag.ExitOnError)
	demoN := demoCmd.Int("n", 1000, "alerts to seed")
	demoSeed := demoCmd.Uint64("seed", 1, "demo RNG seed")
	// Global flags come before the subcommand: Parse stops at the first
	// non-flag argument, which is the subcommand name.
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: chronoctl [flags] <alert|watch|demo> ...")
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
	case "watch":
		if err := runWatch(ctx, c, os.Stdout); err != nil && ctx.Err() == nil {
			slog.Error("watch", "err", err)
			os.Exit(1)
		}
	case "demo":
		if err := demoCmd.Parse(flag.Args()[1:]); err != nil {
			os.Exit(2)
		}
		if err := runDemo(ctx, c, os.Stdout, *demoSeed, *demoN); err != nil && ctx.Err() == nil {
			slog.Error("demo", "err", err)
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

func printTrigger(w io.Writer, tr *chronov1.Trigger) {
	fmt.Fprintf(w, "FIRED %s %s %s/%s price=%s target=%s dir=%s at=%s\n",
		tr.GetAlertId(), tr.GetSymbol(), tr.GetVenue(), tr.GetTier(),
		tr.GetFiredPrice(), tr.GetTargetPrice(),
		tr.GetDirection().String(),
		time.Unix(0, tr.GetFiredAtUnixNanos()).UTC().Format(time.RFC3339Nano))
}

func runWatch(ctx context.Context, c Clients, out io.Writer) error {
	stream, err := c.Alerts.WatchTriggers(ctx, &chronov1.WatchTriggersRequest{})
	if err != nil {
		return err
	}
	for {
		tr, err := stream.Recv()
		if err != nil {
			return err
		}
		printTrigger(out, tr)
	}
}

// runDemo seeds nAlerts dim-scoped ABOVE alerts at reference ±2% across
// random pairs/venues/tiers (plus one deliberate per-venue fan-out set on
// BTCUSDT), then watches and prints triggers until ctx is done.
func runDemo(ctx context.Context, c Clients, out io.Writer, seed uint64, nAlerts int) error {
	cat := catalog.Default()
	syms := cat.Symbols()
	rng := rand.New(rand.NewChaCha8(*feed.SeedBytes(seed)))

	seedOne := func(sym catalog.Symbol, venue, tier string) error {
		ref, err := price.Parse(sym.Reference, sym.Decimals)
		if err != nil {
			return err
		}
		f := 0.98 + 0.04*rng.Float64() // ±2% of reference
		target := int64(float64(ref)*f + 0.5)
		_, err = c.Alerts.UpsertAlert(ctx, &chronov1.UpsertAlertRequest{
			Symbol: sym.Name, PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
			Direction: chronov1.Direction_DIRECTION_ABOVE,
			TargetPrice: price.Format(target, sym.Decimals),
			Venue:       venue, Tier: tier,
		})
		if err != nil {
			return err
		}
		return nil
	}

	venues, tiers := cat.DimValues(catalog.DimVenue), cat.DimValues(catalog.DimTier)
	for i := 0; i < nAlerts; i++ {
		sym := syms[rng.IntN(len(syms))]
		if err := seedOne(sym, venues[rng.IntN(len(venues))], tiers[rng.IntN(len(tiers))]); err != nil {
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
		if err := seedOne(btc, v, catalog.Tiers[0]); err != nil {
			return err
		}
	}
	fmt.Fprintf(out, "seeded %d alerts (incl. %d-venue BTCUSDT fan-out); watching\n", nAlerts+len(venues), len(venues))
	return runWatch(ctx, c, out)
}
