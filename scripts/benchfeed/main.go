// Command benchfeed drives the service layer for performance scenarios:
// seed mode writes a scenario's alert layout into the bbolt store (before
// chronod opens it); feed mode streams the paced tick walk at a running
// chronod, fires the gap tick for burst layouts, and times the store-flip
// drain by polling /stats.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/alertstore"
	"github.com/emir/chrono-tree/internal/bench"
	"github.com/emir/chrono-tree/internal/price"
)

const seedChunk = 5000 // alerts per write tx, chronofeed's bulk-seed size

// statsHTTP bounds each /stats poll: the drain loop's deadline is only
// checked between polls, so a hung request must not outlast it.
var statsHTTP = &http.Client{Timeout: 2 * time.Second}

func main() {
	mode := flag.String("mode", "feed", "seed | feed")
	server := flag.String("server", "localhost:9090", "chronod gRPC address")
	dbPath := flag.String("db", "", "bbolt path (seed mode)")
	statsAddr := flag.String("stats", "localhost:8080", "chronod HTTP address for /stats drain polling")
	scenario := flag.String("scenario", "baseline", "scenario name")
	layout := flag.String("layout", "parked", "parked | trickle | gap")
	alerts := flag.Int("alerts", 1_000_000, "total alerts")
	symbols := flag.Int("symbols", 500, "symbol universe size")
	cluster := flag.Int("cluster", 0, "gap layout: cluster size k")
	rate := flag.Float64("rate", 20000, "target ticks/sec")
	duration := flag.Duration("duration", 4*time.Minute, "steady-load phase")
	seed := flag.Uint64("seed", 1, "market RNG seed")
	out := flag.String("out", ".", "results directory")
	flag.Parse()
	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}
	if *layout == "gap" && (*cluster <= 0 || *cluster >= *alerts) {
		log.Fatalf("gap layout needs 0 < cluster(%d) < alerts(%d)", *cluster, *alerts)
	}
	if *mode == "seed" {
		seedStore(*dbPath, *scenario, *layout, *alerts, *symbols, *cluster, *out, *rate)
		return
	}
	if err := feed(*server, *statsAddr, *scenario, *layout, *alerts, *symbols, *cluster, *rate, *duration, *seed, *out); err != nil {
		log.Fatal(err)
	}
}

func buildLayout(layout string, alerts, symbols, cluster int) ([]bench.Spec, bench.Quote) {
	syms := bench.Symbols(symbols)
	switch layout {
	case "parked":
		return bench.Parked(alerts, syms), bench.Quote{}
	case "trickle":
		return bench.Trickle(alerts, syms, 0.002), bench.Quote{}
	case "gap":
		return bench.GapCluster(alerts, cluster, syms)
	}
	log.Fatalf("unknown layout %q", layout)
	return nil, bench.Quote{}
}

func seedStore(dbPath, scenario, layout string, alerts, symbols, cluster int, out string, rate float64) {
	if dbPath == "" {
		log.Fatal("seed mode needs -db")
	}
	specs, _ := buildLayout(layout, alerts, symbols, cluster)
	store, err := alertstore.Open(dbPath)
	if err != nil {
		log.Fatalf("open store: %v (chronod already running with this db?)", err)
	}
	now := time.Now().UnixNano()
	t0 := time.Now()
	for i := 0; i < len(specs); i += seedChunk {
		end := min(i+seedChunk, len(specs))
		chunk := make([]alertstore.Alert, 0, end-i)
		for _, s := range specs[i:end] {
			chunk = append(chunk, bench.StoreAlert(s, now))
		}
		if err := store.PutBatch(chunk); err != nil {
			log.Fatalf("seed %d: %v", end, err)
		}
		if end%200_000 == 0 || end == len(specs) {
			log.Printf("seeded %d/%d (%.1fs)", end, len(specs), time.Since(t0).Seconds())
		}
	}
	if err := store.Close(); err != nil {
		log.Fatal(err)
	}
	meta := map[string]any{
		"scenario": scenario, "layout": layout, "alerts": len(specs),
		"symbols": symbols, "cluster": cluster, "rate": rate, "venue": bench.VenueName,
	}
	writeJSON(out+"/seed.json", meta)
	log.Printf("seed done: %d alerts into %s", len(specs), dbPath)
}

func feed(server, statsAddr, scenario, layout string, alerts, symbols, cluster int, rate float64, duration time.Duration, seed uint64, out string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := grpc.NewClient(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	hc := healthpb.NewHealthClient(conn)
	log.Printf("waiting for chronod %s", server)
	for {
		check, err := hc.Check(ctx, &healthpb.HealthCheckRequest{})
		if err == nil && check.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			break
		}
		if ctx.Err() != nil {
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}

	fc := chronov1.NewFeedServiceClient(conn)
	stream, err := fc.StreamTicks(ctx)
	if err != nil {
		return err
	}
	syms := bench.Symbols(symbols)
	_, gap := buildLayout(layout, alerts, symbols, cluster) // specs live in the store; only the gap quote is needed here
	m := bench.NewMarket(syms, seed, layout == "gap")
	em := &bench.Emitter{Target: rate}
	const batchMax, slice = 256, 10*time.Millisecond
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
	dec := func(i int) uint8 { return syms[i].Decimals }
	wire := func(q bench.Quote, now time.Time) *chronov1.Tick {
		d := dec(q.SymIdx)
		return &chronov1.Tick{
			Symbol: syms[q.SymIdx].Name,
			Bid:    price.Format(q.Bid, d), Ask: price.Format(q.Ask, d),
			Venue: bench.VenueName, Tier: bench.Tier(q.TierIdx),
			TsUnixNanos: now.UnixNano(),
		}
	}

	var sent uint64
	slices := int(duration / slice)
	slow := 0
	// Slices are anchored to absolute deadlines (benchengine's pacing fix):
	// a per-slice relative sleep would accumulate timer/GC overshoot across
	// the phase and understate the achieved rate even when chronod keeps up.
	// Anchoring lets a late slice be followed by a shorter wait instead of
	// drifting.
	t0 := time.Now()
	next := t0
	for s := 0; s < slices && ctx.Err() == nil; s++ {
		next = next.Add(slice)
		want := em.Take(slice)
		sliceStart := time.Now()
		now := time.Now()
		for n := want; n > 0; n-- {
			q := m.Step()
			batch = append(batch, wire(q, now))
			sent++
			if len(batch) >= batchMax {
				if err := send(); err != nil {
					return err
				}
			}
		}
		if err := send(); err != nil {
			return err
		}
		if d := time.Since(sliceStart); d > 2*slice {
			slow++
		}
		if wait := time.Until(next); wait > 0 {
			time.Sleep(wait)
		}
	}
	loadWall := time.Since(t0)

	// Burst: one gap tick, then time the drain by polling /stats until the
	// pump has fired the whole cluster (or 120s).
	drainMs, drainTimeout := int64(-1), false
	if layout == "gap" && ctx.Err() == nil {
		now := time.Now()
		if err := stream.Send(&chronov1.TickBatch{Ticks: []*chronov1.Tick{wire(gap, now)}}); err != nil {
			return err
		}
		gapStart := time.Now()
		deadline := gapStart.Add(120 * time.Second)
		for time.Now().Before(deadline) {
			fired := statsFired(statsAddr)
			if fired >= uint64(cluster) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		drainMs = time.Since(gapStart).Milliseconds()
		if statsFired(statsAddr) < uint64(cluster) {
			drainTimeout = true
		}
	}

	fs, err := stream.CloseAndRecv()
	if err != nil && err != io.EOF {
		return fmt.Errorf("close stream: %w", err)
	}
	accepted, dropped := uint64(0), uint64(0)
	if fs != nil {
		accepted, dropped = fs.GetAccepted(), fs.GetDropped()
	}
	if layout == "gap" {
		// The gap tick is not built by the pacing loop, so sent counts it
		// here; accepted already includes it — the server counts every
		// accepted batch before CloseAndRecv returns (verified end-to-end:
		// incrementing both double-counted and reported accepted > sent).
		sent++
	}
	final := statsSnapshot(statsAddr)
	res := map[string]any{
		"ticks_sent": sent, "ticks_accepted": accepted,
		"ticks_dropped": dropped, "saturated": slow*10 > slices,
		"drain_ms": drainMs, "drain_timeout": drainTimeout,
		"load_wall_ms": loadWall.Milliseconds(), "final_stats": final,
	}
	writeJSON(out+"/feed.json", res)
	log.Printf("feed done: sent=%d accepted=%d dropped=%d saturated=%v drain=%dms",
		sent, accepted, dropped, slow*10 > slices, drainMs)
	return nil
}

// statsSnapshot fetches /stats once; nil if unreachable.
func statsSnapshot(addr string) map[string]any {
	r, err := statsHTTP.Get("http://" + addr + "/stats")
	if err != nil {
		return nil
	}
	defer func() { _ = r.Body.Close() }()
	var m map[string]any
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		return nil
	}
	return m
}

// statsFired reads triggers_fired from /stats (0 if unreachable).
func statsFired(addr string) uint64 {
	m := statsSnapshot(addr)
	if m == nil {
		return 0
	}
	f, _ := m["triggers_fired"].(float64)
	return uint64(f)
}

func writeJSON(path string, v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		log.Fatal(err)
	}
}
