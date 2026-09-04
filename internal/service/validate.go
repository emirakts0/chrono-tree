package service

import (
	"context"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// NewValidateInterceptor returns a unary interceptor enforcing the proto's
// buf.validate rules (symbol pattern, price format, UUID shapes) before
// service-level catalog validation runs.
func NewValidateInterceptor() grpc.UnaryServerInterceptor {
	v, err := protovalidate.New()
	if err != nil {
		panic("protovalidate: " + err.Error())
	}
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if msg, ok := req.(proto.Message); ok {
			if err := v.Validate(msg); err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "validation: %v", err)
			}
		}
		return handler(ctx, req)
	}
}
