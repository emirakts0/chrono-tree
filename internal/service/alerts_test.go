package service

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chronov1 "github.com/emir/chrono-tree/api/gen/chrono/v1"
)

func validUpsert() *chronov1.UpsertAlertRequest {
	return &chronov1.UpsertAlertRequest{
		Symbol:      "BTCUSDT",
		PriceType:   chronov1.PriceType_PRICE_TYPE_ASK,
		Direction:   chronov1.Direction_DIRECTION_ABOVE,
		TargetPrice: "65000.00",
		Venue:       "ATLAS",
		Tier:        "TOP",
	}
}

func TestUpsertGeneratesUUID(t *testing.T) {
	e := newEnv(t)
	resp, err := e.alerts().UpsertAlert(context.Background(), validUpsert())
	if err != nil {
		t.Fatalf("UpsertAlert: %v", err)
	}
	id := resp.GetAlertId()
	if len(id) != 36 || !strings.Contains(id, "-") {
		t.Fatalf("alert id %q not UUID string form", id)
	}
	// 0x70 nibble: UUIDv7 version marker.
	if id[14] != '7' {
		t.Fatalf("alert id %q not v7", id)
	}
	if got := e.core.AlertCount(); got != 1 {
		t.Fatalf("alert count = %d, want 1", got)
	}
}

func TestUpsertVisibleImmediately(t *testing.T) {
	e := newEnv(t)
	if _, err := e.alerts().UpsertAlert(context.Background(), validUpsert()); err != nil {
		t.Fatalf("UpsertAlert: %v", err)
	}
	if got := e.core.Engine().Stats().Live; got != 1 {
		t.Fatalf("engine live = %d, want 1 (Sync before return)", got)
	}
}

func TestUpsertValidation(t *testing.T) {
	e := newEnv(t)
	cases := []struct {
		name string
		mut  func(*chronov1.UpsertAlertRequest)
		want codes.Code
	}{
		{"unknown symbol", func(r *chronov1.UpsertAlertRequest) { r.Symbol = "NOSUCH" }, codes.InvalidArgument},
		{"no price type", func(r *chronov1.UpsertAlertRequest) { r.PriceType = chronov1.PriceType_PRICE_TYPE_UNSPECIFIED }, codes.InvalidArgument},
		{"no direction", func(r *chronov1.UpsertAlertRequest) { r.Direction = chronov1.Direction_DIRECTION_UNSPECIFIED }, codes.InvalidArgument},
		{"precision loss", func(r *chronov1.UpsertAlertRequest) { r.TargetPrice = "65000.005" }, codes.InvalidArgument},
		{"bad number", func(r *chronov1.UpsertAlertRequest) { r.TargetPrice = "abc" }, codes.InvalidArgument},
		{"overflow", func(r *chronov1.UpsertAlertRequest) { r.TargetPrice = "99999999999999999.00" }, codes.InvalidArgument},
		{"unknown venue", func(r *chronov1.UpsertAlertRequest) { r.Venue = "BINANCE" }, codes.InvalidArgument},
		{"unknown tier", func(r *chronov1.UpsertAlertRequest) { r.Tier = "DEEP" }, codes.InvalidArgument},
		{"expires before valid", func(r *chronov1.UpsertAlertRequest) { r.ValidFromUnixNanos = 200; r.ExpiresUnixNanos = 100 }, codes.InvalidArgument},
		{"empty symbol", func(r *chronov1.UpsertAlertRequest) { r.Symbol = "" }, codes.InvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validUpsert()
			tc.mut(req)
			_, err := e.alerts().UpsertAlert(context.Background(), req)
			if status.Code(err) != tc.want {
				t.Fatalf("code = %v, want %v (err %v)", status.Code(err), tc.want, err)
			}
			if got := e.core.AlertCount(); got != 0 {
				t.Fatalf("rejected upsert left %d alerts behind", got)
			}
		})
	}
}

func TestUpsertReplaceCancelsOld(t *testing.T) {
	e := newEnv(t)
	first, err := e.alerts().UpsertAlert(context.Background(), validUpsert())
	if err != nil {
		t.Fatal(err)
	}
	req := validUpsert()
	req.TargetPrice = "64000.00"
	req.AlertId = mustBytes(t, first.GetAlertId())
	if _, err := e.alerts().UpsertAlert(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if got := e.core.AlertCount(); got != 1 {
		t.Fatalf("count after replace = %d, want 1", got)
	}
	if got := e.core.Engine().Stats().Live; got != 1 {
		t.Fatalf("engine live after replace = %d, want 1", got)
	}
}

func TestCancel(t *testing.T) {
	e := newEnv(t)
	resp, err := e.alerts().UpsertAlert(context.Background(), validUpsert())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.alerts().CancelAlert(context.Background(), &chronov1.CancelAlertRequest{AlertId: resp.GetAlertId()}); err != nil {
		t.Fatalf("CancelAlert: %v", err)
	}
	if got := e.core.Engine().Stats().Live; got != 0 {
		t.Fatalf("engine live after cancel = %d, want 0", got)
	}
	if _, err := e.alerts().CancelAlert(context.Background(), &chronov1.CancelAlertRequest{AlertId: resp.GetAlertId()}); status.Code(err) != codes.NotFound {
		t.Fatalf("double cancel code = %v, want NotFound", status.Code(err))
	}
	if _, err := e.alerts().CancelAlert(context.Background(), &chronov1.CancelAlertRequest{AlertId: "not-a-uuid"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad uuid code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestAlertsByState(t *testing.T) {
	e := newEnv(t)
	if _, err := e.alerts().UpsertAlert(context.Background(), validUpsert()); err != nil {
		t.Fatal(err)
	}
	if got := e.core.AlertsByState()["active"]; got != 1 {
		t.Fatalf("active = %d, want 1", got)
	}
}

func mustBytes(t *testing.T, s string) []byte {
	t.Helper()
	id, err := parseAlertID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id[:]
}
