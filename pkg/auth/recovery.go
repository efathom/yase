package auth

import (
	"context"
	"log/slog"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoveryUnaryInterceptor converts a panic in a unary handler into an
// Internal error.
//
// grpc-go does not recover panics raised inside handlers: an unrecovered panic
// unwinds the serving goroutine and takes the entire process with it, so one
// malformed response from a downstream service can drop a whole cluster node.
// The panic value and stack are logged; the caller only ever sees a generic
// message.
func RecoveryUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("grpc: recovered panic in handler",
					"method", info.FullMethod,
					"panic", rec,
					"stack", string(debug.Stack()))
				resp = nil
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

// RecoveryStreamInterceptor is the streaming counterpart to
// RecoveryUnaryInterceptor.
func RecoveryStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("grpc: recovered panic in stream handler",
					"method", info.FullMethod,
					"panic", rec,
					"stack", string(debug.Stack()))
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(srv, ss)
	}
}
