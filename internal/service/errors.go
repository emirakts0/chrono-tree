package service

import (
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/emir/chrono-tree/engine"
)

// invalidf builds an InvalidArgument status — the catch-all for boundary
// validation failures.
func invalidf(format string, args ...any) error {
	return status.Errorf(codes.InvalidArgument, format, args...)
}

// mapEngineErr maps control-plane engine errors to gRPC statuses. One
// place, so handlers stay one-liners.
func mapEngineErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, engine.ErrClosed):
		return status.Error(codes.Unavailable, "engine shutting down")
	case errors.Is(err, engine.ErrNotFound):
		return status.Error(codes.NotFound, "alert not found")
	case errors.Is(err, engine.ErrInvalidTransition):
		return status.Error(codes.FailedPrecondition, "alert already in a terminal state")
	case errors.Is(err, engine.ErrAlertLimit), errors.Is(err, engine.ErrSymbolLimit):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, engine.ErrDims):
		return status.Error(codes.InvalidArgument, "invalid dimension combination")
	default:
		slog.Error("unmapped engine error", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}
