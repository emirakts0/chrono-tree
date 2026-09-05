package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/pub"
	"github.com/emir/chrono-tree/internal/service"
	"github.com/emir/chrono-tree/internal/stats"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestRunSeedRegistersAlerts(t *testing.T) {
	now := time.Now()
	core := service.NewCore(engine.DefaultConfig(), catalog.Default(), service.NoopMetrics{}, stats.New(now), pub.Noop{})
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
	var out bytes.Buffer
	if err := runSeed(ctx, c, &out, 7, 30, 8); err != nil {
		t.Fatalf("runSeed: %v", err)
	}
	if got := core.AlertCount(); got != 33 { // 30 random + 3-venue BTCUSDT fan-out
		t.Fatalf("alert count = %d, want 33", got)
	}
	if !strings.Contains(out.String(), "seeded 33") {
		t.Fatalf("output missing seed count: %q", out.String())
	}
}
