package perf

import (
	"os/exec"
	"testing"
	"time"
)

// TestPerfSmoke runs the full harness (both layers) at tiny parameters.
// It is the CI-proof that seeding, feeding, profiling, collecting, and
// the validity gates all still work together. Lives beside smoke.sh so
// the relative path stays honest.
func TestPerfSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("smoke run is minutes-scale")
	}
	cmd := exec.Command("bash", "smoke.sh") // cwd is this package dir; smoke.sh cds to the demo module dir
	done := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("smoke failed: %v", err)
		}
	case <-time.After(6 * time.Minute): // 4 legs (parked + trickle x 2 layers): 2 service boots dominate
		_ = cmd.Process.Kill()
		t.Fatal("smoke exceeded 6 minutes")
	}
}
