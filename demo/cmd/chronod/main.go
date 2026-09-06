// Command chronod hosts the chrono-tree engine: the chrono.v1 gRPC
// services on one listener, HTTP status on another.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	chronov1 "github.com/emir/chrono-tree/demo/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/demo/internal/alertstore"
	"github.com/emir/chrono-tree/demo/internal/catalog"
	"github.com/emir/chrono-tree/demo/internal/pub"
	"github.com/emir/chrono-tree/demo/internal/server"
	"github.com/emir/chrono-tree/demo/internal/service"
	"github.com/emir/chrono-tree/demo/internal/stats"
)

// shutdownGrace bounds GracefulStop; connections that outlive it are cut.
const shutdownGrace = 10 * time.Second

func main() {
	grpcAddr := flag.String("grpc-addr", ":9090", "gRPC listen address")
	httpAddr := flag.String("http-addr", ":8080", "HTTP status listen address")
	natsURL := flag.String("nats-url", "nats://localhost:4222", "NATS server URL (trigger publishing); empty = no broker (noop publisher, no live trigger feed)")
	dbPath := flag.String("db", "chrono.bbolt", "alert store path (bbolt); source of truth across restarts")
	ring := flag.Int("ring", 1<<20, "trigger ring capacity, rounded to a power of two (40 B/slot ≈ 40 MiB at default); size for the largest simultaneous burst")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *grpcAddr, *httpAddr, *natsURL, *dbPath, *ring); err != nil {
		slog.Error("chronod exit", "err", err)
		os.Exit(1)
	}
	slog.Info("chronod stopped")
}

func run(ctx context.Context, grpcAddr, httpAddr, natsURL, dbPath string, ringSize int) error {
	// NATS is a boot dependency: unreachable broker is a fatal error.
	// Later outages reconnect forever; publishes during them drop-and-count.
	// An empty URL is the no-broker mode: a noop publisher (publishes
	// succeed instantly) used by the performance campaign and any
	// broker-less deployment — the pump and store flips still run.
	var publisher pub.Publisher
	if natsURL == "" {
		publisher = pub.Noop{}
	} else {
		var err error
		publisher, err = pub.NewNATS(natsURL)
		if err != nil {
			return fmt.Errorf("nats: %w", err)
		}
	}
	defer func() { _ = publisher.Close() }() // backstop; the shutdown path drains first

	now := time.Now()
	reg := prometheus.NewRegistry()
	pm := server.NewPromMetrics(reg)
	st := stats.New(now)
	// The alert store is the source of truth: opened before the engine,
	// closed after it. A corrupt or locked file is fatal — no engine
	// without its catalog. (Timeout inside Open prevents flock hangs.)
	store, err := alertstore.Open(dbPath)
	if err != nil {
		return fmt.Errorf("alert store: %w", err)
	}
	// Backstop ordering note: this defer registers BEFORE core.Close's,
	// so it runs AFTER it (LIFO) — the store always outlives the pump.
	defer func() { _ = store.Close() }()
	cfg := engine.DefaultConfig()
	cfg.RingSize = ringSize // operational knob: burst absorption ceiling
	core := service.NewCore(cfg, catalog.Empty(), pm, st, publisher, store)
	defer core.Close() // stops the pump, then the engine (idempotent)

	statusSrv := server.New(core, st, reg, dbPath)
	defer statusSrv.Close() // stop the 1s tick engine at exit (idempotent)

	// Dashboard live feed: re-consume our own published triggers. The
	// handler fans out to browsers over SSE; it never blocks us.
	// Noop mode has no stream to subscribe to.
	if np, ok := publisher.(*pub.NATSPublisher); ok {
		if err := np.SubscribeTriggers(statusSrv.HandleTrigger); err != nil {
			return fmt.Errorf("subscribe triggers: %w", err)
		}
	}

	httpServer := &http.Server{Addr: httpAddr, Handler: statusSrv.Handler()}
	httpLn, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return fmt.Errorf("http listen: %w", err)
	}
	httpErr := make(chan error, 1)
	go func() { httpErr <- httpServer.Serve(httpLn) }()
	slog.Info("http status listening", "addr", httpAddr)

	kaep := keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}
	kasp := keepalive.ServerParameters{
		MaxConnectionIdle:     time.Minute,
		MaxConnectionAge:      5 * time.Minute,
		MaxConnectionAgeGrace: 30 * time.Second,
		Time:                  30 * time.Second,
		Timeout:               5 * time.Second,
	}
	gs := grpc.NewServer(
		grpc.ChainUnaryInterceptor(service.NewValidateInterceptor()),
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
	)
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(gs, healthSrv)
	chronov1.RegisterAlertServiceServer(gs, core)
	chronov1.RegisterFeedServiceServer(gs, core)
	reflection.Register(gs)
	grpcLn, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	grpcErr := make(chan error, 1)
	go func() { grpcErr <- gs.Serve(grpcLn) }()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	slog.Info("grpc listening", "addr", grpcAddr)

	<-ctx.Done()
	slog.Info("shutting down")
	statusSrv.SetShuttingDown()

	// Bound GracefulStop: in-flight RPCs complete, then we stop accepting;
	// stuck connections are cut at shutdownGrace. The publisher drain below
	// flushes any triggers still pending on the NATS connection.
	stopped := make(chan struct{})
	go func() { gs.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(shutdownGrace):
		gs.Stop()
	}

	core.Close() // stops the pump, then the engine (idempotent; the defer is a backstop)
	// Pump drained, engine down; the backstop defer above is now a no-op
	// (Close on a closed bolt DB returns ErrDatabaseNotOpen, ignored).
	if err := store.Close(); err != nil {
		slog.Warn("alert store close", "err", err)
	}
	if err := publisher.Close(); err != nil { // bounded 3s drain inside
		slog.Warn("nats drain", "err", err)
	}
	shCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	_ = httpServer.Shutdown(shCtx)
	statusSrv.Close() // stop the 1s tick goroutine (SSE relays are gone); the defer is now a no-op

	if err := <-httpErr; err != nil && err != http.ErrServerClosed {
		return err
	}
	if err := <-grpcErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err // ErrServerStopped is the normal GracefulStop exit
	}
	return nil
}
