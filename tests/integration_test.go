//go:build integration

// End-to-end smoke: real binaries, real sockets, real network hop.
// Run: go test -tags integration ./tests -run Integration -v -timeout 180s
package tests

import (
	"bufio"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
	"github.com/emir/chrono-tree/internal/pub"
	"github.com/emir/chrono-tree/internal/pub/pubtest"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// repoRoot is this file's parent directory; the test binary's CWD is the
// package dir, but the `go run` invocations need the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root, err := filepath.Abs(filepath.Join(filepath.Dir(thisFile), ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestIntegrationEndToEnd(t *testing.T) {
	grpcPort, httpPort := freePort(t), freePort(t)
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	httpAddr := fmt.Sprintf("127.0.0.1:%d", httpPort)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	natsURL := pubtest.Start(t)

	root := repoRoot(t)
	// Dedicated temp db: the daemon must not inherit whatever chrono.bbolt
	// lies around in the repo root, and the feeder must not win the
	// open-race and seed its ladder into the daemon's store (the test
	// brings its own alert below, and asserts on the FIRST trigger).
	dbPath := filepath.Join(t.TempDir(), "it.bbolt")
	daemon := exec.CommandContext(ctx, "go", "run", "./cmd/chronod",
		"-grpc-addr", grpcAddr, "-http-addr", httpAddr, "-nats-url", natsURL, "-db", dbPath)
	daemon.Dir = root
	feeder := exec.CommandContext(ctx, "go", "run", "./scripts/chronofeed",
		"-server", grpcAddr, "-rate", "20000", "-seed", "1", "-db", dbPath, "-alerts", "0")
	feeder.Dir = root
	// `go run` execs the real binary as a child; kill the whole process
	// group so no daemon survives the test.
	startAndWait := func(cmd *exec.Cmd) {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			t.Fatalf("start %v: %v", cmd.Args, err)
		}
		t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })
	}
	startAndWait(daemon)
	startAndWait(feeder)

	// Wait for readiness.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + httpAddr + "/readyz")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Subscribe before wiring the alert, then register a BELOW alert just
	// above BTC's anchor: the first ATLAS/TOP BTCUSDT ask at ~65000
	// satisfies it, so the trigger fires within moments of the feed's
	// first matching tick.
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("chrono.triggers.>")
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	alerts := chronov1.NewAlertServiceClient(conn)
	resp, err := alerts.UpsertAlert(ctx, &chronov1.UpsertAlertRequest{
		Symbol: "BTCUSDT", PriceType: chronov1.PriceType_PRICE_TYPE_ASK,
		Direction:   chronov1.Direction_DIRECTION_BELOW,
		TargetPrice: "65100.00", Venue: "ATLAS", Tier: "TOP",
	})
	if err != nil {
		t.Fatal(err)
	}
	alertID := resp.GetAlertId()

	msg, err := sub.NextMsg(25 * time.Second)
	if err != nil {
		t.Fatalf("no trigger published to NATS: %v", err)
	}
	if msg.Subject != pub.Subject("ATLAS", "TOP") {
		t.Fatalf("subject = %q", msg.Subject)
	}
	var tr pub.Trigger
	if err := json.Unmarshal(msg.Data, &tr); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if tr.Symbol != "BTCUSDT" || tr.Venue != "ATLAS" || tr.Tier != "TOP" {
		t.Fatalf("trigger dims wrong: %+v", tr)
	}

	// /stats must show NATS connected.
	sresp, err := http.Get("http://" + httpAddr + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	var health map[string]any
	if err := json.UnmarshalRead(sresp.Body, &health); err != nil {
		sresp.Body.Close()
		t.Fatal(err)
	}
	sresp.Body.Close()
	if v, _ := health["nats_connected"].(bool); !v {
		t.Fatalf("nats_connected = %v, want true", health["nats_connected"])
	}

	// /stats must show the feed's rate. The rate is a mean over complete
	// seconds since the first tick, so the daemon's ramp-up second dilutes
	// it; poll until the gate is met or we run out of time.
	var rate float64
	gateMet := false
	rateDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(rateDeadline) {
		sresp, err := http.Get("http://" + httpAddr + "/stats")
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]any
		if err := json.UnmarshalRead(sresp.Body, &body); err != nil {
			sresp.Body.Close()
			t.Fatal(err)
		}
		sresp.Body.Close()
		rate, _ = body["ticks_per_sec"].(float64)
		if rate >= 19500 {
			gateMet = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !gateMet {
		t.Fatalf("ticks_per_sec = %v, want >= 19500 (spec gate)", rate)
	}
	t.Logf("ticks_per_sec = %.0f", rate)

	// Dashboard: the SPA is served and the inquiry API resolves the alert.
	// The state is "triggered" because the trigger assertion above already
	// proved the alert fired exactly once.
	ui, err := http.Get("http://" + httpAddr + "/")
	if err != nil {
		t.Fatal(err)
	}
	uiBody, err := io.ReadAll(ui.Body)
	ui.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if ui.StatusCode != 200 || !strings.Contains(string(uiBody), "app.js") {
		t.Fatalf("ui index: status=%d", ui.StatusCode)
	}

	inq, err := http.Get("http://" + httpAddr + "/api/alerts/" + alertID)
	if err != nil {
		t.Fatal(err)
	}
	var detail struct {
		Symbol string `json:"symbol"`
		State  string `json:"state"`
	}
	if err := json.UnmarshalRead(inq.Body, &detail); err != nil {
		inq.Body.Close()
		t.Fatal(err)
	}
	inq.Body.Close()
	if detail.Symbol != "BTCUSDT" || detail.State != "triggered" {
		t.Fatalf("inquiry detail = %+v", detail)
	}

	// One SSE frame check: hello + at least one snapshot, bounded by a 5s
	// request deadline so a broken stream cannot hang the test.
	sseCtx, sseCancel := context.WithTimeout(ctx, 5*time.Second)
	defer sseCancel()
	sseReq, err := http.NewRequestWithContext(sseCtx, http.MethodGet,
		"http://"+httpAddr+"/api/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	sse, err := http.DefaultClient.Do(sseReq)
	if err != nil {
		t.Fatal(err)
	}
	defer sse.Body.Close()
	sc := bufio.NewScanner(sse.Body)
	sawHello, sawSnap, sawBatch := false, false, false
	for sc.Scan() && !(sawHello && sawSnap && sawBatch) {
		line := sc.Text()
		if strings.Contains(line, `"type":"hello"`) {
			sawHello = true
		}
		if strings.Contains(line, `"type":"snapshot"`) {
			sawSnap = true
		}
		if strings.Contains(line, `"type":"triggers"`) {
			sawBatch = true
		}
	}
	if !sawHello || !sawSnap {
		t.Fatalf("sse: hello=%v snapshot=%v (err=%v)", sawHello, sawSnap, sc.Err())
	}
	t.Logf("sse triggers batch seen: %v", sawBatch)
}
