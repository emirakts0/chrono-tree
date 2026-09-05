package server

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestSysSamplerValues(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "alerts.bbolt")
	if err := os.WriteFile(db, make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	sp := newSysSampler(db)
	s := sp.sample()
	if s.DBBytes != 4096 {
		t.Fatalf("DBBytes = %d, want 4096", s.DBBytes)
	}
	if s.DiskFreeBytes == 0 || s.DiskTotalBytes == 0 {
		t.Fatalf("disk fields = free %d total %d, want both > 0", s.DiskFreeBytes, s.DiskTotalBytes)
	}
	if s.Cores < 1 {
		t.Fatalf("Cores = %d, want >= 1", s.Cores)
	}
	if s.RSSBytes == 0 {
		t.Fatal("RSSBytes = 0, want > 0 for a running process")
	}
	if s.HeapBytes == 0 {
		t.Fatal("HeapBytes = 0, want > 0")
	}
	if s.HostCPUPercent < 0 || s.HostCPUPercent > 100 {
		t.Fatalf("HostCPUPercent = %v, want 0..100", s.HostCPUPercent)
	}
	if s.ProcCPUPercent < 0 {
		t.Fatalf("ProcCPUPercent = %v, want >= 0", s.ProcCPUPercent)
	}
	if s.HostMemTotal < s.HostMemUsed {
		t.Fatalf("HostMemTotal %d < HostMemUsed %d", s.HostMemTotal, s.HostMemUsed)
	}
	// Second sample: also finite (the marshal invariant holds across the
	// CPU delta window's first refresh).
	s2 := sp.sample()
	if math.IsNaN(s2.HostCPUPercent) || math.IsInf(s2.HostCPUPercent, 0) {
		t.Fatalf("second sample HostCPUPercent = %v, want finite", s2.HostCPUPercent)
	}
}

func TestSysSamplerNoDBPath(t *testing.T) {
	s := newSysSampler("").sample()
	if s.DBBytes != 0 || s.DiskFreeBytes != 0 || s.DiskTotalBytes != 0 {
		t.Fatalf("db/disk fields = %+v, want zero without a path", s)
	}
	// Host/process fields still sampled.
	if s.Cores < 1 || s.RSSBytes == 0 {
		t.Fatalf("host fields should survive a missing db path: %+v", s)
	}
}

func TestSysSamplerMissingDBFile(t *testing.T) {
	db := filepath.Join(t.TempDir(), "nope.bbolt")
	if s := newSysSampler(db).sample(); s.DBBytes != -1 {
		t.Fatalf("DBBytes for missing file = %d, want -1", s.DBBytes)
	}
}

// TestFiniteSanitizer pins the runTick marshal invariant: encoding/json
// refuses NaN/Inf, and a non-finite gauge would skip the whole SSE
// broadcast.
func TestFiniteSanitizer(t *testing.T) {
	nan := math.NaN()
	for _, c := range []struct {
		in, want float64
	}{
		{nan, 0},
		{math.Inf(1), 0},
		{math.Inf(-1), 0},
		{0, 0},
		{12.5, 12.5},
		{-3.25, -3.25},
	} {
		if got := finite(c.in); got != c.want { // want is never NaN, so != is safe
			t.Fatalf("finite(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}
