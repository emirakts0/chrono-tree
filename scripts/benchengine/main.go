// Command benchengine drives the engine in-process for performance
// scenarios: seeds a layout, paces a tick stream at the target rate,
// times burst drains, and writes pprof profiles plus a gated summary.
// Pure engine — no gRPC, no store.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/emir/chrono-tree/engine"
	"github.com/emir/chrono-tree/internal/bench"
)

func main() {
	layer := "engine"
	scenario := flag.String("scenario", "baseline", "scenario name (rides in the summary)")
	layout := flag.String("layout", "parked", "parked | trickle | gap")
	alerts := flag.Int("alerts", 1_000_000, "total alerts")
	symbols := flag.Int("symbols", 500, "symbol universe size")
	cluster := flag.Int("cluster", 0, "gap layout: cluster size k")
	band := flag.Float64("band", 0.6, "trickle layout: ladder span as a fraction of Ref")
	rate := flag.Float64("rate", 20000, "target ticks/sec")
	duration := flag.Duration("duration", 4*time.Minute, "steady-load phase")
	seed := flag.Uint64("seed", 1, "market RNG seed")
	out := flag.String("out", ".", "results directory")
	flag.Parse()
	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}
	if *layout == "gap" && (*cluster <= 0 || *cluster >= *alerts) {
		log.Fatalf("gap layout needs 0 < cluster(%d) < alerts(%d)", *cluster, *alerts)
	}

	syms := bench.Symbols(*symbols)
	var specs []bench.Spec
	var gap bench.Quote
	switch *layout {
	case "parked":
		specs = bench.Parked(*alerts, syms)
	case "trickle":
		specs = bench.Trickle(*alerts, syms, *band)
	case "gap":
		specs, gap = bench.GapCluster(*alerts, *cluster, syms)
	default:
		log.Fatalf("unknown layout %q", *layout)
	}

	eng := engine.New(engine.DefaultConfig())
	defer eng.Close()

	pid := os.Getpid()
	t0 := time.Now()
	for i := range specs {
		if err := eng.Upsert(bench.EngineSpec(specs[i])); err != nil {
			log.Fatalf("upsert %d: %v", i, err)
		}
		if (i+1)%200_000 == 0 {
			log.Printf("seeded %d/%d alerts (%.1fs)", i+1, len(specs), time.Since(t0).Seconds())
		}
	}
	eng.Sync()
	rssSeed := bench.ReadRSS(pid)
	cpuSeed := bench.ReadCPUSeconds(pid)
	log.Printf("seed done: %d alerts in %.1fs, RSS %d MiB",
		len(specs), time.Since(t0).Seconds(), rssSeed>>20)

	// Trigger consumer: the ONLY Triggers() drainer, mirroring the pump.
	var fired atomic.Uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]engine.Trigger, 1024)
		for {
			if n := eng.Triggers().PopBatch(buf); n > 0 {
				fired.Add(uint64(n))
				continue
			}
			select {
			case <-stop:
				return
			case <-time.After(500 * time.Microsecond):
			}
		}
	}()

	cpuF, err := os.Create(*out + "/cpu.pprof")
	if err != nil {
		log.Fatal(err)
	}
	if err := pprof.StartCPUProfile(cpuF); err != nil {
		log.Fatal(err)
	}

	// Steady load: emitter-paced round-robin walk.
	m := bench.NewMarket(syms, *seed, *layout == "gap")
	em := &bench.Emitter{Target: *rate}
	const slice = 10 * time.Millisecond
	var sent uint64
	wallStart := time.Now()
	slices := int(*duration / slice)
	slow := 0 // slices that overran 2×: saturation evidence
	// Slices are anchored to absolute deadlines: a per-slice relative
	// sleep would accumulate overshoot (timer slack, GC pauses) across
	// the phase and understate the achieved rate even when the engine
	// keeps up. Anchoring lets a late slice be followed by a shorter
	// wait instead of drifting.
	next := wallStart
	for s := 0; s < slices; s++ {
		next = next.Add(slice)
		want := em.Take(slice)
		sliceStart := time.Now()
		now := time.Now()
		for n := want; n > 0; n-- {
			q := m.Step()
			tk := bench.EngineTick(syms, q, now.UnixNano())
			eng.Match(&tk)
			sent++
		}
		if d := time.Since(sliceStart); d > 2*slice {
			slow++
		}
		if wait := time.Until(next); wait > 0 {
			time.Sleep(wait)
		}
	}
	loadWall := time.Since(wallStart)

	// Burst: one gap tick, then time the drain to ring-empty.
	var drainMs int64 = -1
	if *layout == "gap" {
		before := fired.Load()
		gapStart := time.Now()
		tk := bench.EngineTick(syms, gap, time.Now().UnixNano())
		eng.Match(&tk)
		target := uint64(*cluster)
		deadline := time.Now().Add(120 * time.Second)
		for fired.Load()-before < target && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		drainMs = time.Since(gapStart).Milliseconds()
		if got := fired.Load() - before; got < target {
			log.Printf("drain incomplete after 120s: %d of %d", got, target)
		}
	}

	pprof.StopCPUProfile()
	_ = cpuF.Close()
	close(stop)
	<-done

	heapF, err := os.Create(*out + "/heap.pprof")
	if err != nil {
		log.Fatal(err)
	}
	if err := pprof.WriteHeapProfile(heapF); err != nil {
		log.Fatal(err)
	}
	_ = heapF.Close()
	gF, err := os.Create(*out + "/goroutine.txt")
	if err != nil {
		log.Fatal(err)
	}
	if err := pprof.Lookup("goroutine").WriteTo(gF, 1); err != nil {
		log.Fatal(err)
	}
	_ = gF.Close()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	st := eng.Stats()
	sum := bench.Summary{
		Params: bench.Params{
			Layer: layer, Scenario: *scenario, Layout: *layout,
			Alerts: *alerts, Symbols: *symbols, Cluster: *cluster, Rate: int(*rate),
		},
		Sent: sent, Accepted: sent,
		AchievedRate: float64(sent) / loadWall.Seconds(),
		NSPerTick:    float64(loadWall.Nanoseconds()) / float64(sent),
		Saturated:    slow*10 > slices, // >10% of slices overran: engine cannot keep pace
		RSSSeed:      rssSeed,
		RSSPlateau:   bench.ReadRSS(pid),
		RSSPeak:      bench.ReadPeakRSS(pid),
		HeapInuse:    ms.HeapInuse,
		GCCount:      ms.NumGC,
		CPUSeconds:   bench.ReadCPUSeconds(pid) - cpuSeed,
		Fired:        fired.Load(),
		RingDropped:  st.DroppedTriggers,
		DrainMillis:  drainMs,
		WallMillis:   time.Since(wallStart).Milliseconds(),
	}
	sum.CPUPctOneCore = 100 * sum.CPUSeconds / loadWall.Seconds()
	bench.Validate(&sum)
	b, _ := json.MarshalIndent(sum, "", "  ")
	if err := os.WriteFile(*out+"/summary.json", b, 0o644); err != nil {
		log.Fatal(err)
	}
	log.Printf("scenario %s/%s done: valid=%v achieved=%.0f tps rss_peak=%d MiB fired=%d drain=%dms",
		layer, *scenario, sum.Valid, sum.AchievedRate, sum.RSSPeak>>20, sum.Fired, sum.DrainMillis)
	if !sum.Valid {
		fmt.Fprintf(os.Stderr, "INVALID: %v\n", sum.InvalidReasons)
	}
}
