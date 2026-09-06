package service

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	chronov1 "github.com/emir/chrono-tree/demo/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/demo/internal/alertstore"
	"github.com/emir/chrono-tree/demo/internal/catalog"
	"github.com/emir/chrono-tree/demo/internal/pub"
	"github.com/emir/chrono-tree/demo/internal/pub/pubtest"
	"github.com/emir/chrono-tree/demo/internal/stats"
)

type testEnv struct {
	core  *Core
	store *alertstore.Store // non-nil for store-backed envs; restart tests close it early
	cc    *grpc.ClientConn
	srv   *grpc.Server
	rec   *recordingPub // non-nil for the fast unit env
	nats  string        // non-empty for the NATS-backed env
}

// recordingPub captures published triggers for unit tests, and can be
// flipped to failing to exercise the drop-and-count path.
type recordingPub struct {
	mu   sync.Mutex
	pubs []pub.Trigger
	fail bool
}

func (r *recordingPub) Publish(tr pub.Trigger) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return errors.New("publisher down")
	}
	r.pubs = append(r.pubs, tr)
	return nil
}
func (r *recordingPub) Connected() bool { return true }
func (r *recordingPub) Close() error    { return nil }
func (r *recordingPub) triggers() []pub.Trigger {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]pub.Trigger(nil), r.pubs...)
}

func newEnv(t *testing.T) *testEnv {
	t.Helper()
	rec := &recordingPub{}
	return newEnvWithPub(t, rec, "")
}

// newNATSEnv builds the env against a real in-process NATS server and a
// real NATSPublisher — the full pump → publish → broker path.
func newNATSEnv(t *testing.T) *testEnv {
	t.Helper()
	url := pubtest.Start(t)
	p, err := pub.NewNATS(url)
	if err != nil {
		t.Fatalf("pub.NewNATS: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return newEnvWithPub(t, p, url)
}

// openStore opens a throwaway store for one test.
func openStore(t *testing.T) *alertstore.Store {
	t.Helper()
	s, err := alertstore.Open(filepath.Join(t.TempDir(), "alerts.bbolt"))
	if err != nil {
		t.Fatalf("alertstore.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func newEnvWithPub(t *testing.T, p pub.Publisher, natsURL string) *testEnv {
	t.Helper()
	cat := catalog.Empty()
	store := openStore(t) // store cleanup registered FIRST → runs LAST, after core.Close
	core := NewCore(engine.DefaultConfig(), cat, NoopMetrics{}, stats.New(time.Now()), p, store)
	t.Cleanup(core.Close)
	e := newEnvWithCore(t, core, p)
	e.store, e.nats = store, natsURL
	return e
}

// newEnvWithCore wires bufconn gRPC around an already-built core and
// registers the same cleanups as newEnvWithPub (minus core/store, which
// the caller owns).
func newEnvWithCore(t *testing.T, core *Core, p pub.Publisher) *testEnv {
	t.Helper()
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
	})
	var rec *recordingPub
	if r, ok := p.(*recordingPub); ok {
		rec = r
	}
	return &testEnv{core: core, cc: cc, srv: srv, rec: rec}
}

// natsClient connects a subscriber to the env's embedded NATS server
// (NATS-backed envs only) and unsubscribes+closes at cleanup.
func (e *testEnv) natsClient(t *testing.T) *nats.Conn {
	t.Helper()
	if e.nats == "" {
		t.Fatal("natsClient requires newNATSEnv")
	}
	nc, err := nats.Connect(e.nats)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func (e *testEnv) alerts() chronov1.AlertServiceClient { return chronov1.NewAlertServiceClient(e.cc) }
func (e *testEnv) feed() chronov1.FeedServiceClient    { return chronov1.NewFeedServiceClient(e.cc) }
