// Command perfcollect merges a service-layer run directory (seed.json +
// feed.json + 1 Hz stats.jsonl) into a gated bench.Summary. Engine runs
// need no collector — benchengine writes summary.json itself.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/emir/chrono-tree/internal/bench"
)

func main() {
	dir := flag.String("dir", ".", "run directory")
	flag.Parse()
	sum := collect(*dir)
	b, _ := json.MarshalIndent(sum, "", "  ")
	if err := os.WriteFile(filepath.Join(*dir, "summary.json"), b, 0o644); err != nil {
		log.Fatal(err)
	}
	log.Printf("collected %s/%s: valid=%v", sum.Params.Layer, sum.Params.Scenario, sum.Valid)
	if !sum.Valid {
		log.Printf("INVALID: %v", sum.InvalidReasons)
	}
}

func readJSON(dir, name string, v any) bool {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return false
	}
	return json.Unmarshal(b, v) == nil
}

type seedMeta struct {
	Scenario string `json:"scenario"`
	Layout   string `json:"layout"`
	Alerts   int    `json:"alerts"`
	Symbols  int    `json:"symbols"`
	Cluster  int    `json:"cluster"`
	Rate     int    `json:"rate"`
}

type feedMeta struct {
	Sent         uint64 `json:"ticks_sent"`
	Accepted     uint64 `json:"ticks_accepted"`
	Dropped      uint64 `json:"ticks_dropped"`
	Saturated    bool   `json:"saturated"`
	DrainMS      int64  `json:"drain_ms"`
	DrainTimeout bool   `json:"drain_timeout"`
	LoadWallMS   int64  `json:"load_wall_ms"`
	Final        struct {
		TriggersFired uint64 `json:"triggers_fired"`
		Published     uint64 `json:"triggers_published"`
		PubDropped    uint64 `json:"triggers_publish_dropped"`
		Engine        struct {
			DroppedTriggers uint64 `json:"dropped_triggers"`
		} `json:"engine"`
		Sys struct {
			RSSBytes   uint64  `json:"rss_bytes"`
			DBBytes    int64   `json:"db_bytes"`
			ProcCPUPct float64 `json:"proc_cpu_percent"`
			HeapBytes  uint64  `json:"heap_bytes"`
		} `json:"sys"`
	} `json:"final_stats"`
}

type statLine struct {
	Sys struct {
		RSSBytes   uint64  `json:"rss_bytes"`
		ProcCPUPct float64 `json:"proc_cpu_percent"`
	} `json:"sys"`
	TS int64 `json:"ts"` // epoch seconds, stamped by run.sh's sampler; 0 = untimestamped
}

// readEpoch reads a marker file run.sh writes around the load phase
// (load_start / load_end, epoch seconds); ok=false when absent/unparsable.
func readEpoch(dir, name string) (int64, bool) {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil || v <= 0 {
		return 0, false
	}
	return v, true
}

func collect(dir string) bench.Summary {
	var sum bench.Summary
	var seed seedMeta
	var feed feedMeta
	readJSON(dir, "seed.json", &seed)
	readJSON(dir, "feed.json", &feed)

	sum = bench.Summary{
		Params: bench.Params{
			Layer: "service", Scenario: seed.Scenario, Layout: seed.Layout,
			Alerts: seed.Alerts, Symbols: seed.Symbols, Cluster: seed.Cluster, Rate: seed.Rate,
		},
		Sent: feed.Sent, Accepted: feed.Accepted, Dropped: feed.Dropped,
		Saturated: feed.Saturated, DrainMillis: feed.DrainMS,
		Fired:          feed.Final.TriggersFired,
		Published:      feed.Final.Published,
		PublishDropped: feed.Final.PubDropped,
		RingDropped:    feed.Final.Engine.DroppedTriggers,
		DBBytes:        feed.Final.Sys.DBBytes,
		HeapInuse:      feed.Final.Sys.HeapBytes,
		WallMillis:     feed.LoadWallMS,
	}
	if feed.LoadWallMS > 0 {
		sum.AchievedRate = float64(feed.Accepted) / (float64(feed.LoadWallMS) / 1000)
		sum.NSPerTick = float64(feed.LoadWallMS) * 1e6 / float64(feed.Accepted)
	}
	sum.RSSSeed = feed.Final.Sys.RSSBytes // replaced by first stats sample below

	// 1 Hz stats log. Load-window gating: run.sh stamps each sample with
	// an epoch-seconds ts and writes load_start/load_end markers around
	// the benchfeed invocation. Peak RSS spans the whole log (boot replay
	// can legitimately grow the heap), but the CPU mean and the RSS
	// plateau come from the load window only — averaging over replay+idle
	// understates load CPU. Without markers/timestamps (jq-less hosts,
	// old logs) every sample counts, the previous behavior.
	f, err := os.Open(filepath.Join(dir, "stats.jsonl"))
	if err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		type sample struct {
			rss uint64
			cpu float64
			ts  int64
		}
		var all []sample
		first := true
		for sc.Scan() {
			var l statLine
			if json.Unmarshal(sc.Bytes(), &l) != nil || l.Sys.RSSBytes == 0 {
				continue
			}
			if first {
				sum.RSSSeed = l.Sys.RSSBytes // post-replay steady point
				first = false
			}
			all = append(all, sample{rss: l.Sys.RSSBytes, cpu: l.Sys.ProcCPUPct, ts: l.TS})
		}
		_ = f.Close()
		lo, hasLo := readEpoch(dir, "load_start")
		hi, hasHi := readEpoch(dir, "load_end")
		inWindow := func(s sample) bool { return hasLo && hasHi && s.ts >= lo && s.ts <= hi }
		var win []sample
		for _, s := range all {
			if inWindow(s) {
				win = append(win, s)
			}
		}
		if len(win) == 0 {
			win = all // no windowed samples: fall back to the whole log
		}
		for _, s := range all {
			if s.rss > sum.RSSPeak {
				sum.RSSPeak = s.rss
			}
		}
		if n := len(win); n > 0 {
			tail := win[n-min(60, n):]
			var acc uint64
			for _, s := range tail {
				acc += s.rss
			}
			sum.RSSPlateau = acc / uint64(len(tail))
			var cpuAcc float64
			for _, s := range win {
				cpuAcc += s.cpu
			}
			sum.CPUPctOneCore = cpuAcc / float64(len(win))
			sum.CPUSeconds = sum.CPUPctOneCore / 100 * float64(feed.LoadWallMS) / 1000
		}
	}
	// Gate the run first, then layer the collector's own spec gates on
	// top — Validate overwrites InvalidReasons, so appending before it
	// would silently drop these reasons (and mark a profile-less run
	// valid).
	bench.Validate(&sum)
	if feed.DrainTimeout {
		sum.InvalidReasons = append(sum.InvalidReasons, "drain exceeded 120s")
	}
	// Spec gate: profiles present and non-empty — a silent fetch failure
	// must not pass as a clean run.
	for _, p := range []string{"cpu.pprof", "heap.pprof"} {
		if fi, err := os.Stat(filepath.Join(dir, p)); err != nil || fi.Size() == 0 {
			sum.InvalidReasons = append(sum.InvalidReasons, "missing or empty "+p)
		}
	}
	sum.Valid = len(sum.InvalidReasons) == 0
	b, _ := json.MarshalIndent(sum, "", "  ")
	_ = os.WriteFile(filepath.Join(dir, "summary.json"), b, 0o644)
	return sum
}
