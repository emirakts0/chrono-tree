package service

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/catalog"
	"github.com/emir/chrono-tree/internal/stats"
)

type testEnv struct {
	core *Core
	cc   *grpc.ClientConn
	srv  *grpc.Server
}

func newEnv(t *testing.T) *testEnv {
	t.Helper()
	cat := catalog.Default()
	core := NewCore(engine.DefaultConfig(), cat, NoopMetrics{}, stats.New(time.Now()))
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(NewValidateInterceptor()))
	chronov1.RegisterAlertServiceServer(srv, core)
	chronov1.RegisterFeedServiceServer(srv, core)
	go func() { _ = srv.Serve(lis) }()
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() {
		_ = cc.Close()
		srv.Stop()
		core.Close()
	})
	return &testEnv{core: core, cc: cc, srv: srv}
}

func (e *testEnv) alerts() chronov1.AlertServiceClient { return chronov1.NewAlertServiceClient(e.cc) }
func (e *testEnv) feed() chronov1.FeedServiceClient    { return chronov1.NewFeedServiceClient(e.cc) }
