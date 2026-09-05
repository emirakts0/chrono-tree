package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/emir/chrono-tree/internal/pub/pubtest"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestRunNATSUnavailableIsFatal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Port 1 never answers; run must return promptly with an error.
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, freeAddr(t), freeAddr(t), "nats://127.0.0.1:1", filepath.Join(t.TempDir(), "chrono.bbolt"))
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("run should fail without NATS")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not fail fast without NATS")
	}
}

func TestRunServesHTTP(t *testing.T) {
	natsURL := pubtest.Start(t)
	grpcAddr, httpAddr := freeAddr(t), freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, grpcAddr, httpAddr, natsURL, filepath.Join(t.TempDir(), "chrono.bbolt")) }()

	deadline := time.Now().Add(10 * time.Second)
	ok := false
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://%s/healthz", httpAddr))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				ok = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ok {
		t.Fatal("healthz never became ready")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not shut down within 15s")
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}
