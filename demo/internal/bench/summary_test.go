package bench

import "testing"

// base returns a summary that passes every gate for the parked layout.
func base() Summary {
	return Summary{
		Params:       Params{Layer: "engine", Scenario: "baseline", Layout: "parked", Alerts: 1000, Symbols: 500, Rate: 20000},
		Sent:         100000, Accepted: 100000,
		AchievedRate: 19950, NSPerTick: 50,
		RSSSeed: 1 << 20, RSSPlateau: 2 << 20, RSSPeak: 3 << 20,
		HeapInuse: 1 << 20, GCCount: 5, CPUSeconds: 10, CPUPctOneCore: 250,
		DrainMillis: -1, WallMillis: 5000,
	}
}

func TestValidateParkedPasses(t *testing.T) {
	s := base()
	Validate(&s)
	if !s.Valid {
		t.Fatalf("clean parked run invalid: %v", s.InvalidReasons)
	}
}

func TestValidateDropFails(t *testing.T) {
	s := base()
	s.Dropped = 3
	Validate(&s)
	if s.Valid || len(s.InvalidReasons) != 1 {
		t.Fatalf("drop not gated: valid=%v reasons=%v", s.Valid, s.InvalidReasons)
	}
}

func TestValidateParkedFireFails(t *testing.T) {
	s := base()
	s.Fired = 7
	Validate(&s)
	if s.Valid {
		t.Fatal("parked layout fired but run counted valid")
	}
}

func TestValidateGapConservation(t *testing.T) {
	s := base()
	s.Params.Layout, s.Params.Cluster = "gap", 1000
	s.Fired, s.RingDropped = 990, 10 // 10 fell off the ring: finding, still valid
	Validate(&s)
	if !s.Valid {
		t.Fatalf("conserved burst marked invalid: %v", s.InvalidReasons)
	}
	s.Fired = 500 // 490 + 10 lost: seeding bug
	Validate(&s)
	if s.Valid {
		t.Fatal("unconserved burst counted valid")
	}
}

func TestValidateTrickleWindow(t *testing.T) {
	s := base()
	s.Params.Layout = "trickle"
	s.Fired = 1
	Validate(&s)
	if !s.Valid {
		t.Fatalf("sparse trickle invalid: %v", s.InvalidReasons)
	}
	s.Fired = uint64(s.Params.Alerts)
	Validate(&s)
	if s.Valid {
		t.Fatal("trickle burned the whole ladder and still valid")
	}
}

func TestValidateRateGateAndSaturation(t *testing.T) {
	s := base()
	s.AchievedRate = 10000 // 50% of target
	Validate(&s)
	if s.Valid {
		t.Fatal("half-rate run counted valid")
	}
	s.Saturated = true // the finding IS that the service saturated
	Validate(&s)
	if !s.Valid {
		t.Fatalf("saturated run invalid: %v", s.InvalidReasons)
	}
}

func TestValidateRequiresMeasurements(t *testing.T) {
	s := base()
	s.RSSPeak, s.CPUSeconds = 0, 0
	Validate(&s)
	if s.Valid || len(s.InvalidReasons) != 2 {
		t.Fatalf("missing measurements not gated: %v", s.InvalidReasons)
	}
}
