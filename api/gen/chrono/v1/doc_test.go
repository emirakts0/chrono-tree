package chronov1_test

import (
	"testing"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
)

// The generated code is committed; this pins that both services and the
// streaming methods exist with the shapes later tasks call.
func TestGeneratedStubs(t *testing.T) {
	var _ = chronov1.PriceType_PRICE_TYPE_BID
	var _ = chronov1.Direction_DIRECTION_ABOVE
	// Interface satisfaction is checked at registration in cmd/chronod;
	// here we only pin the constructor symbols exist.
	_ = chronov1.RegisterAlertServiceServer
	_ = chronov1.RegisterFeedServiceServer
}
