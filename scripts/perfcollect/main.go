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

	// 1 Hz stats log: peak = max rss; plateau = mean of the last 60
	// samples; cpu% = mean proc_cpu_percent across the window.
	f, err := os.Open(filepath.Join(dir, "stats.jsonl"))
	if err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		var rss []uint64
		var cpu []float64
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
			rss = append(rss, l.Sys.RSSBytes)
			cpu = append(cpu, l.Sys.ProcCPUPct)
		}
		_ = f.Close()
		for _, r := range rss {
			if r > sum.RSSPeak {
				sum.RSSPeak = r
			}
		}
		if n := len(rss); n > 0 {
			tail := rss[n-min(60, n):]
			var acc uint64
			for _, r := range tail {
				acc += r
			}
			sum.RSSPlateau = acc / uint64(len(tail))
		}
		if len(cpu) > 0 {
			var acc float64
			for _, c := range cpu {
				acc += c
			}
			sum.CPUPctOneCore = acc / float64(len(cpu))
			sum.CPUSeconds = sum.CPUPctOneCore / 100 * float64(feed.LoadWallMS) / 1000
		}
	}
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
	bench.Validate(&sum)
	b, _ := json.MarshalIndent(sum, "", "  ")
	_ = os.WriteFile(filepath.Join(dir, "summary.json"), b, 0o644)
	return sum
}
