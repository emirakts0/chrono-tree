// Package pubtest starts an in-process NATS server for tests. It exists so
// every test that needs a broker (unit, service, and integration) embeds
// one instead of requiring an external nats-server binary; production code
// never imports this package or the nats-server dependency.
package pubtest

import (
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// Start launches an embedded NATS server on 127.0.0.1 with a random port
// and returns its client URL. The server shuts down at test cleanup.
func Start(t *testing.T) string {
	t.Helper()
	s, err := natsserver.NewServer(&natsserver.Options{
		Host:   "127.0.0.1",
		Port:   -1, // random free port
		NoLog:  true,
		NoSigs: true,
	})
	if err != nil {
		t.Fatalf("embedded NATS server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded NATS server did not become ready")
	}
	t.Cleanup(s.Shutdown)
	return s.ClientURL()
}
