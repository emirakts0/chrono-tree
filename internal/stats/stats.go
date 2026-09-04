// Package stats holds the service-level counters backing /stats and
// /metrics: plain atomics for totals and a fixed 61-slot ring for
// per-second rates. All methods are safe for concurrent use.
package stats

import (
	"sync"
	"sync/atomic"
	"time"
)

// Rate counts events per second over a 60-second sliding window. Adds take
// a mutex — at the feed's ~20k ticks/s this is nanoseconds of work and it
// keeps rollover bookkeeping trivially correct. The ring holds 61 slots
// (index = unix second % 61): with 60 the in-progress second's bucket would
// alias the oldest second in the read window, leaking the current count
// into PerSecond.
type Rate struct {
	mu      sync.Mutex
	started int64 // unix second of first Add, 0 = never
	curSec  int64
	buckets [61]uint64 // index = unix second % 61
}

func (r *Rate) Add(n uint64, now time.Time) {
	sec := now.Unix()
	r.mu.Lock()
	r.advance(sec)
	if r.started == 0 {
		r.started = sec
	}
	r.buckets[sec%61] += n
	r.mu.Unlock()
}

// advance zeroes every bucket between curSec and sec (inclusive of sec),
// so stale counts from >60s ago are never read. Caller holds mu.
func (r *Rate) advance(sec int64) {
	switch {
	case sec == r.curSec:
		return
	case sec-r.curSec >= 61:
		for i := range r.buckets {
			r.buckets[i] = 0
		}
	default:
		for s := r.curSec + 1; s <= sec; s++ {
			r.buckets[s%61] = 0
		}
	}
	r.curSec = sec
}

// PerSecond returns the mean rate over the complete seconds in
// [started, now), clamped to the last 60. The current second is excluded
// so the value is stable across scrapes.
func (r *Rate) PerSecond(now time.Time) float64 {
	sec := now.Unix()
	r.mu.Lock()
	r.advance(sec)
	start := r.started
	if start == 0 || sec <= start {
		r.mu.Unlock()
		return 0
	}
	lo := sec - 60
	if lo < start {
		lo = start
	}
	var total uint64
	for s := lo; s < sec; s++ {
		total += r.buckets[s%61]
	}
	r.mu.Unlock()
	return float64(total) / float64(sec-lo)
}

// Stats is the service's counter set. Totals are lock-free atomics; rates
// use the Rate ring.
type Stats struct {
	started time.Time

	Ticks             atomic.Uint64
	TicksDropped      atomic.Uint64
	TriggersFired     atomic.Uint64
	TriggersDelivered atomic.Uint64
	WatcherDrops      atomic.Uint64

	TickRate Rate
	FireRate Rate
}

func New(now time.Time) *Stats { return &Stats{started: now} }

// Snapshot is the /stats JSON shape (json/v2 marshals the tags).
type Snapshot struct {
	UptimeSec         float64 `json:"uptime_sec"`
	Ticks             uint64  `json:"ticks"`
	TicksPerSec       float64 `json:"ticks_per_sec"`
	TicksDropped      uint64  `json:"ticks_dropped"`
	TriggersFired     uint64  `json:"triggers_fired"`
	TriggersPerSec    float64 `json:"triggers_per_sec"`
	TriggersDelivered uint64  `json:"triggers_delivered"`
	WatcherDrops      uint64  `json:"watcher_drops"`
}

func (s *Stats) Snapshot(now time.Time) Snapshot {
	return Snapshot{
		UptimeSec:         now.Sub(s.started).Seconds(),
		Ticks:             s.Ticks.Load(),
		TicksPerSec:       s.TickRate.PerSecond(now),
		TicksDropped:      s.TicksDropped.Load(),
		TriggersFired:     s.TriggersFired.Load(),
		TriggersPerSec:    s.FireRate.PerSecond(now),
		TriggersDelivered: s.TriggersDelivered.Load(),
		WatcherDrops:      s.WatcherDrops.Load(),
	}
}
