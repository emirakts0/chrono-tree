package service

import (
	"fmt"

	"uuid"

	"github.com/emir/chrono-tree/engine"
)

// newAlertID mints a UUIDv7 — time-ordered, matching the engine's
// AlertID tie-break expectations.
func newAlertID() engine.AlertID {
	u := uuid.NewV7()
	return engine.AlertID(u)
}

func alertIDString(id engine.AlertID) string {
	u := uuid.UUID(id)
	return u.String()
}

func parseAlertID(s string) (engine.AlertID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return engine.AlertID{}, fmt.Errorf("alert id %q: %w", s, err)
	}
	return engine.AlertID(u), nil
}
