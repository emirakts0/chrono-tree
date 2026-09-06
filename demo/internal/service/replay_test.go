package service

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	chronov1 "github.com/emir/chrono-tree/demo/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/demo/internal/alertstore"
	"github.com/emir/chrono-tree/demo/internal/catalog"
	"github.com/emir/chrono-tree/demo/internal/stats"
)

// newEnvAtPath builds a bufconn env backed by a store at an explicit
// path — the restart story: close, reopen the same file, replay.
func newEnvAtPath(t *testing.T, path string) *testEnv {
	t.Helper()
	rec := &recordingPub{}
	cat := catalog.Empty()
	store, err := alertstore.Open(path)
	if err != nil {
		t.Fatalf("alertstore.Open: %v", err)
	}
	core := NewCore(engine.DefaultConfig(), cat, NoopMetrics{}, stats.New(time.Now()), rec, store)
	t.Cleanup(func() {
		core.Close()
		_ = store.Close()
	})
	e := newEnvWithCore(t, core, rec)
	e.store = store
	return e
}

func TestRestartReplayAndFire(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alerts.bbolt")
	e := newEnvAtPath(t, path)

	live := validUpsert() // BTCUSDT ask above 65000
	liveResp, err := e.alerts().UpsertAlert(context.Background(), live)
	if err != nil {
		t.Fatal(err)
	}
	expired := validUpsert()
	expired.Symbol = "ETHUSDT"
	expired.ExpiresUnixNanos = time.Now().Add(-time.Hour).UnixNano() // expired before it was ever seen again
	if _, err := e.alerts().UpsertAlert(context.Background(), expired); err != nil {
		t.Fatal(err)
	}
	cancelled := validUpsert()
	cancelled.Symbol = "SOLUSDT"
	cancelledResp, err := e.alerts().UpsertAlert(context.Background(), cancelled)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.alerts().CancelAlert(context.Background(), &chronov1.CancelAlertRequest{AlertId: cancelledResp.GetAlertId()}); err != nil {
		t.Fatal(err)
	}

	// Shutdown: pump drained, engine closed, store closed (bbolt holds an
	// exclusive flock — the reopen below needs this).
	e.srv.Stop()
	e.core.Close()
	_ = e.store.Close()

	e2 := newEnvAtPath(t, path)
	defer e2.srv.Stop()
	if got := e2.core.Engine().Stats().Live; got != 1 {
		t.Fatalf("engine live after replay = %d, want 1 (live alert only)", got)
	}
	if byState := e2.core.AlertsByState(); byState["active"] != 1 || byState["cancelled"] != 2 {
		t.Fatalf("states after replay = %v, want 1 active / 2 cancelled", byState)
	}

	// The replayed alert still fires, and fired fields land on the record.
	if _, err := runTicks(t, e2, &chronov1.TickBatch{Ticks: []*chronov1.Tick{
		tick("BTCUSDT", "65001.00", "65002.00", "ATLAS", "TOP"),
	}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if e2.core.AlertsByState()["triggered"] == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	view, ok := e2.core.GetAlert(liveResp.GetAlertId())
	if !ok {
		t.Fatal("replayed alert missing after fire")
	}
	if view.State != "triggered" || view.FiredPrice == "" || view.FiredAtUnixNanos == 0 {
		t.Fatalf("view = %+v, want triggered with fired price/at", view)
	}
	if view.FiredPrice != "65002.00" { // the ask it fired on (tick's 3rd field)
		t.Fatalf("fired price = %q, want 65002.00", view.FiredPrice)
	}
}
