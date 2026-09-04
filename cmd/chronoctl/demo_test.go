package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/service"
	"github.com/emir/chrono-tree/internal/stats"
	"github.com/emir/chrono-tree/price"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestRunDemoFiresAndPrints(t *testing.T) {
	now := time.Now()
	core := service.NewCore(engine.DefaultConfig(), catalog.Default(), service.NoopMetrics{}, stats.New(now), now)
	t.Cleanup(core.Close)
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	chronov1.RegisterAlertServiceServer(srv, core)
	chronov1.RegisterFeedServiceServer(srv, core)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := Clients{Alerts: chronov1.NewAlertServiceClient(conn), Feed: chronov1.NewFeedServiceClient(conn)}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var buf guardedBuffer
	done := make(chan error, 1)
	go func() { done <- runDemo(ctx, c, &buf, 7, 30) }()

	// Give the demo a moment to register its alerts, then push prices
	// through every venue×tier combo at +10% of each major's reference.
	time.Sleep(500 * time.Millisecond)
	for _, sym := range []catalog.Symbol{
		{Name: "BTCUSDT", Decimals: 2, Reference: "65000.00"},
		{Name: "ETHUSDT", Decimals: 2, Reference: "3400.00"},
	} {
		for _, v := range catalog.Venues {
			for _, ti := range catalog.Tiers {
				base := parseRef(t, sym.Reference, sym.Decimals)
				ask := base * 110 / 100
				stream, err := c.Feed.StreamTicks(ctx)
				if err != nil {
					t.Fatal(err)
				}
				_ = stream.Send(&chronov1.TickBatch{Ticks: []*chronov1.Tick{{
					Symbol: sym.Name,
					Bid:    fmtPrice(base*109/100, sym.Decimals),
					Ask:    fmtPrice(ask, sym.Decimals),
					Venue:  v, Tier: ti, TsUnixNanos: time.Now().UnixNano(),
				}}})
				if _, err := stream.CloseAndRecv(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && strings.Count(buf.String(), "FIRED") == 0 {
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	<-done
	out := buf.String()
	if !strings.Contains(out, "FIRED") {
		t.Fatalf("no trigger printed:\n%s", out)
	}
}

// guardedBuffer is a mutex-guarded bytes.Buffer (the demo writes from its
// watcher goroutine).
type guardedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (g *guardedBuffer) Write(p []byte) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.buf.Write(p)
}

func (g *guardedBuffer) String() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.buf.String()
}

func parseRef(t *testing.T, ref string, dec uint8) int64 {
	t.Helper()
	v, err := price.Parse(ref, dec)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func fmtPrice(base int64, dec uint8) string { return price.Format(base, dec) }
