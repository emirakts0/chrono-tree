package alertstore

import (
	"bytes"
	"testing"

	"github.com/emir/chrono-tree/engine"
)

func sampleAlert() Alert {
	return Alert{
		ID: engine.AlertID{0x01, 0x02, 0x03}, Symbol: "BTCUSDT", Decimals: 8,
		Venue: "ATLAS", Tier: "TOP",
		PriceType: engine.PriceAsk, Direction: engine.DirLTE,
		TargetPrice: 650001200000000, ValidFrom: 100, Expires: 200,
		State: StateTriggered, AutoDeactivate: true,
		CreatedAt:  1234567890,
		FiredPrice: 650000000000000, FiredAt: 1234567999,
	}
}

func TestAlertCodecRoundTrip(t *testing.T) {
	a := sampleAlert()
	got, err := decodeAlert(appendAlert(make([]byte, 0, 128), &a))
	if err != nil {
		t.Fatalf("decodeAlert: %v", err)
	}
	if got != a {
		t.Fatalf("round trip mismatch:\n got  %+v\n want %+v", got, a)
	}
}

func TestAlertCodecShortRecord(t *testing.T) {
	for n := range recHeadLen {
		if _, err := decodeAlert(bytes.Repeat([]byte{0}, n)); err == nil {
			t.Fatalf("decodeAlert(%d bytes) should fail", n)
		}
	}
	// Truncated string section: head only.
	if _, err := decodeAlert(bytes.Repeat([]byte{0}, recHeadLen)); err == nil {
		t.Fatal("decodeAlert with no string section should fail")
	}
}

func TestIdxKeyOrdering(t *testing.T) {
	older := idxKey(statePrefix(StateActive), 1000, engine.AlertID{1})
	newer := idxKey(statePrefix(StateActive), 2000, engine.AlertID{2})
	if bytes.Compare(older, newer) <= 0 {
		t.Fatal("newer createdAt must sort FIRST (inverted timestamp)")
	}
	ts, id, ok := idxTail(newer, len(statePrefix(StateActive)))
	if !ok || ts != 2000 || id != (engine.AlertID{2}) {
		t.Fatalf("idxTail = (%d, %v, %v), want (2000, 02…, true)", ts, id, ok)
	}
	if _, _, ok := idxTail(append(newer, 0), len(statePrefix(StateActive))); ok {
		t.Fatal("idxTail must reject wrong-length keys")
	}
}
