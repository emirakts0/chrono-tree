package main

import (
	"context"
	"net"
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
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func newCore(t *testing.T) (*service.Core, *grpc.ClientConn) {
	t.Helper()
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
	return core, conn
}

func TestRunFeedsTicks(t *testing.T) {
	core, conn := newCore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	_ = conn // silence unused if run signature evolves
	done := make(chan error, 1)
	go func() { done <- run(ctx, conn, 5000, 1, 3) }()
	<-ctx.Done()
	// run returns shortly after ctx cancellation.
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancel")
	}
	if got := core.Engine().Stats().Live; got != 0 {
		t.Fatalf("feed created alerts? live=%d", got)
	}
	if core.FeedEverConnected() == false {
		t.Fatal("feed never connected")
	}
	// At 5000/s over ~1.2s (minus startup) several thousand ticks must land.
	if n := core.VenueTicks()["ATLAS"] + core.VenueTicks()["NOVA"] + core.VenueTicks()["ZENITH"]; n < 1000 {
		t.Fatalf("only %d ticks ingested", n)
	}
}
