package bench

import "fmt"

// Params identifies one scenario run; it rides inside the summary so the
// gates (and the report) are self-describing.
type Params struct {
	Layer    string `json:"layer"`
	Scenario string `json:"scenario"`
	Layout   string `json:"layout"`
	Alerts   int    `json:"alerts"`
	Symbols  int    `json:"symbols"`
	Cluster  int    `json:"cluster"`
	Rate     int    `json:"rate"`
}

// Summary is one run's measurements. Drivers (engine) or the collector
// (service) fill it; Validate gates it; the campaign report tabulates it.
type Summary struct {
	Params         Params   `json:"params"`
	Sent           uint64   `json:"ticks_sent"`
	Accepted       uint64   `json:"ticks_accepted"`
	Dropped        uint64   `json:"ticks_dropped"`
	AchievedRate   float64  `json:"achieved_rate"`
	NSPerTick      float64  `json:"ns_per_tick"`
	Saturated      bool     `json:"saturated"`
	RSSSeed        uint64   `json:"rss_seed_bytes"`
	RSSPlateau     uint64   `json:"rss_plateau_bytes"`
	RSSPeak        uint64   `json:"rss_peak_bytes"`
	HeapInuse      uint64   `json:"heap_inuse_bytes"`
	GCCount        uint32   `json:"gc_count"`
	CPUSeconds     float64  `json:"cpu_seconds"`
	CPUPctOneCore  float64  `json:"cpu_pct_one_core"`
	DBBytes        int64    `json:"db_bytes"`
	Fired          uint64   `json:"triggers_fired"`
	Published      uint64   `json:"triggers_published"`
	PublishDropped uint64   `json:"triggers_publish_dropped"`
	RingDropped    uint64   `json:"ring_dropped"`
	DrainMillis    int64    `json:"drain_ms"`
	WallMillis     int64    `json:"wall_ms"`
	Valid          bool     `json:"valid"`
	InvalidReasons []string `json:"invalid_reasons,omitempty"`
}

// Validate applies the run-validity gates in place. Conservation replaces
// naive equality for bursts: fired + ring_dropped == cluster — a ring
// overflow is a finding, not an invalidation, while lost alerts mean the
// layout lied (seeding bug). Saturation likewise: a service that cannot
// keep up at target rate is exactly the measurement, so the 95% rate gate
// only applies when the feed was not back-pressured.
func Validate(s *Summary) {
	var why []string
	if s.Dropped != 0 {
		why = append(why, "tick drops > 0")
	}
	if s.Accepted != s.Sent {
		why = append(why, "accepted != sent")
	}
	switch s.Params.Layout {
	case "parked":
		if s.Fired != 0 {
			why = append(why, "zero-fire layout fired")
		}
	case "gap":
		if got := s.Fired + s.RingDropped; got != uint64(s.Params.Cluster) {
			why = append(why, fmt.Sprintf("burst conservation broken: fired+ring=%d, cluster=%d", got, s.Params.Cluster))
		}
	case "trickle":
		if s.Fired == 0 || s.Fired >= uint64(s.Params.Alerts) {
			why = append(why, fmt.Sprintf("trickle fired %d of %d: outside (0, all)", s.Fired, s.Params.Alerts))
		}
	default:
		why = append(why, "unknown layout "+s.Params.Layout)
	}
	if !s.Saturated && s.AchievedRate < 0.95*float64(s.Params.Rate) {
		why = append(why, fmt.Sprintf("achieved %.0f tps < 95%% of target %d (and not marked saturated)", s.AchievedRate, s.Params.Rate))
	}
	if s.RSSPeak == 0 {
		why = append(why, "no RSS samples")
	}
	if s.CPUSeconds <= 0 {
		why = append(why, "no CPU samples")
	}
	s.InvalidReasons = why
	s.Valid = len(why) == 0
}
