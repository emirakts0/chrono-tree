package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/emir/chrono-tree/demo/internal/bench"
)

func write(t *testing.T, dir, name string, v any) {
	t.Helper()
	b, _ := json.Marshal(v)
	if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCollectServiceRun(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "seed.json", map[string]any{
		"scenario": "baseline", "layout": "parked", "alerts": 1000,
		"symbols": 500, "cluster": 0, "rate": 20000,
	})
	write(t, dir, "feed.json", map[string]any{
		"ticks_sent": 100000, "ticks_accepted": 100000, "ticks_dropped": 0,
		"saturated": false, "drain_ms": -1, "load_wall_ms": 5000,
		"final_stats": map[string]any{
			"triggers_fired": 0,
			"engine":         map[string]any{"dropped_triggers": 0},
			"sys": map[string]any{
				"rss_bytes": 2097152, "db_bytes": 1048576, "proc_cpu_percent": 150.0,
			},
		},
	})
	// 1 Hz stats log: 120 samples, rss climbing then flat.
	lines := "sys:values\n"
	for i := 0; i < 120; i++ {
		rss := 1048576 + i*16384 // ramp for 60, flat after
		if i >= 60 {
			rss = 1048576 + 60*16384
		}
		l, _ := json.Marshal(map[string]any{
			"sys": map[string]any{"rss_bytes": rss, "proc_cpu_percent": 150.0},
		})
		lines += string(l) + "\n"
	}
	if err := os.WriteFile(filepath.Join(dir, "stats.jsonl"), []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	// Profile gate fixtures: non-empty profile files.
	os.WriteFile(filepath.Join(dir, "cpu.pprof"), []byte("profile"), 0o644)
	os.WriteFile(filepath.Join(dir, "heap.pprof"), []byte("profile"), 0o644)

	sum := collect(dir)
	if !sum.Valid {
		t.Fatalf("clean run invalid: %v", sum.InvalidReasons)
	}
	if sum.RSSPeak < sum.RSSPlateau || sum.RSSPlateau < sum.RSSSeed {
		t.Fatalf("rss order wrong: seed=%d plateau=%d peak=%d", sum.RSSSeed, sum.RSSPlateau, sum.RSSPeak)
	}
	if sum.CPUPctOneCore != 150 {
		t.Fatalf("cpu%% = %v, want 150", sum.CPUPctOneCore)
	}
	if sum.DBBytes != 1048576 {
		t.Fatalf("db bytes = %d", sum.DBBytes)
	}
	if sum.AchievedRate != 20000 { // 100000 ticks / 5000ms = 20000 tps
		t.Fatalf("achieved = %v, want 20000", sum.AchievedRate)
	}
	if sum.Params.Layer != "service" {
		t.Fatalf("layer = %s", sum.Params.Layer)
	}
	// summary.json written and re-readable
	b, err := os.ReadFile(filepath.Join(dir, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var again bench.Summary
	if err := json.Unmarshal(b, &again); err != nil {
		t.Fatal(err)
	}
	if !again.Valid {
		t.Fatal("written summary not valid")
	}
}

// Regression: the drain/profile gate reasons were appended BEFORE
// bench.Validate, whose fresh InvalidReasons slice clobbered them — a run
// with no profile files passed as valid. Clean feed + valid stats + NO
// profiles must be INVALID with both profile reasons.
func TestCollectMissingProfilesInvalid(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "seed.json", map[string]any{
		"scenario": "baseline", "layout": "parked", "alerts": 1000,
		"symbols": 500, "cluster": 0, "rate": 20000,
	})
	write(t, dir, "feed.json", map[string]any{
		"ticks_sent": 100000, "ticks_accepted": 100000, "ticks_dropped": 0,
		"saturated": false, "drain_ms": -1, "load_wall_ms": 5000,
		"final_stats": map[string]any{
			"triggers_fired": 0,
			"engine":         map[string]any{"dropped_triggers": 0},
			"sys": map[string]any{
				"rss_bytes": 2097152, "db_bytes": 1048576, "proc_cpu_percent": 150.0,
			},
		},
	})
	var lines string
	for i := 0; i < 120; i++ {
		l, _ := json.Marshal(map[string]any{
			"sys": map[string]any{"rss_bytes": 2097152, "proc_cpu_percent": 150.0},
		})
		lines += string(l) + "\n"
	}
	if err := os.WriteFile(filepath.Join(dir, "stats.jsonl"), []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	// NO cpu.pprof / heap.pprof.

	sum := collect(dir)
	if sum.Valid {
		t.Fatal("run without profiles counted valid")
	}
	joined := strings.Join(sum.InvalidReasons, "; ")
	for _, want := range []string{"missing or empty cpu.pprof", "missing or empty heap.pprof"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("reasons %q missing %q", joined, want)
		}
	}
	// And the gates themselves still hold (the run is invalid ONLY for
	// the missing profiles).
	if strings.Contains(joined, "tick drops") || strings.Contains(joined, "no RSS") {
		t.Fatalf("clean run gained spurious reasons: %v", sum.InvalidReasons)
	}
}

// Load-window gating: with ts-stamped samples and load_start/load_end
// markers, the CPU mean and RSS plateau must come from the load window
// only, while peak RSS spans the whole log (boot replay can legitimately
// spike RSS above the load plateau).
func TestCollectLoadWindowGating(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "seed.json", map[string]any{
		"scenario": "baseline", "layout": "parked", "alerts": 1000,
		"symbols": 500, "cluster": 0, "rate": 20000,
	})
	write(t, dir, "feed.json", map[string]any{
		"ticks_sent": 100000, "ticks_accepted": 100000, "ticks_dropped": 0,
		"saturated": false, "drain_ms": -1, "load_wall_ms": 5000,
		"final_stats": map[string]any{
			"triggers_fired": 0,
			"engine":         map[string]any{"dropped_triggers": 0},
			"sys": map[string]any{
				"rss_bytes": 2097152, "db_bytes": 1048576, "proc_cpu_percent": 150.0,
			},
		},
	})
	mk := func(ts int64, rss uint64, cpu float64) string {
		l, _ := json.Marshal(map[string]any{
			"sys": map[string]any{"rss_bytes": rss, "proc_cpu_percent": cpu},
			"ts":  ts,
		})
		return string(l) + "\n"
	}
	var lines string
	// Boot replay: hot CPU, RSS spikes to 8 MiB (the whole-log peak).
	for ts := int64(100); ts < 110; ts++ {
		lines += mk(ts, 8<<20, 800.0)
	}
	// Load window [110, 120]: 150% CPU, flat 2 MiB.
	for ts := int64(110); ts <= 120; ts++ {
		lines += mk(ts, 2<<20, 150.0)
	}
	// Post-load idle: cold CPU, RSS settles at 1 MiB.
	for ts := int64(121); ts <= 140; ts++ {
		lines += mk(ts, 1<<20, 5.0)
	}
	if err := os.WriteFile(filepath.Join(dir, "stats.jsonl"), []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "load_start"), []byte("110\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "load_end"), []byte("120\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "cpu.pprof"), []byte("profile"), 0o644)
	os.WriteFile(filepath.Join(dir, "heap.pprof"), []byte("profile"), 0o644)

	sum := collect(dir)
	if !sum.Valid {
		t.Fatalf("windowed run invalid: %v", sum.InvalidReasons)
	}
	if sum.CPUPctOneCore != 150 {
		t.Fatalf("cpu%% = %v, want 150 (load window only)", sum.CPUPctOneCore)
	}
	// 150% over the 5s load wall = 7.5 CPU-seconds.
	if sum.CPUSeconds != 7.5 {
		t.Fatalf("cpu seconds = %v, want 7.5", sum.CPUSeconds)
	}
	if sum.RSSPlateau != 2<<20 {
		t.Fatalf("plateau = %d, want %d (load window)", sum.RSSPlateau, 2<<20)
	}
	if sum.RSSPeak < 8<<20 {
		t.Fatalf("peak = %d, want whole-log boot-replay peak >= %d", sum.RSSPeak, 8<<20)
	}
}

func TestCollectFlagsInvalidRun(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "seed.json", map[string]any{
		"scenario": "baseline", "layout": "parked", "alerts": 1000,
		"symbols": 500, "cluster": 0, "rate": 20000,
	})
	write(t, dir, "feed.json", map[string]any{
		"ticks_sent": 1000, "ticks_accepted": 990, "ticks_dropped": 10,
		"final_stats": map[string]any{"triggers_fired": 0, "sys": map[string]any{"rss_bytes": 1}},
	})
	os.WriteFile(filepath.Join(dir, "stats.jsonl"), []byte("\n"), 0o644)
	sum := collect(dir)
	if sum.Valid {
		t.Fatal("dropping run counted valid")
	}
}
