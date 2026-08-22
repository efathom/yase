package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// M-09: grpc-go does not recover panics in handlers, so an unhandled panic
// terminates the whole node process rather than failing one RPC. The HTTP
// gateway has gateway.Recover; the gRPC surface needs the same.
func TestRecoveryUnaryInterceptorConvertsPanicToInternalError(t *testing.T) {
	interceptor := RecoveryUnaryInterceptor()

	handler := func(_ context.Context, _ any) (any, error) {
		panic("index out of range [7] with length 2")
	}

	var resp any
	var err error
	require.NotPanics(t, func() {
		resp, err = interceptor(context.Background(), nil,
			&grpc.UnaryServerInfo{FullMethod: "/index.IndexService/Search"}, handler)
	}, "a handler panic must not escape the interceptor")

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.NotContains(t, status.Convert(err).Message(), "index out of range",
		"the panic value must stay in the logs, not go back to the caller")
}

// A handler that returns normally must be unaffected.
func TestRecoveryUnaryInterceptorPassesThroughSuccess(t *testing.T) {
	interceptor := RecoveryUnaryInterceptor()

	handler := func(_ context.Context, _ any) (any, error) {
		return "ok", nil
	}

	resp, err := interceptor(context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: "/index.IndexService/Search"}, handler)

	require.NoError(t, err)
	assert.Equal(t, "ok", resp)
}

type fakeStream struct{ grpc.ServerStream }

func (fakeStream) Context() context.Context { return context.Background() }

func TestRecoveryStreamInterceptorConvertsPanicToInternalError(t *testing.T) {
	interceptor := RecoveryStreamInterceptor()

	handler := func(_ any, _ grpc.ServerStream) error {
		panic("boom")
	}

	var err error
	require.NotPanics(t, func() {
		err = interceptor(nil, fakeStream{},
			&grpc.StreamServerInfo{FullMethod: "/index.IndexService/Stream"}, handler)
	})

	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

// Recovery must be installed even when auth is disabled — a panic crashes the
// process regardless of whether the caller was authenticated.
func TestGRPCServerOptionsAlwaysIncludeRecovery(t *testing.T) {
	assert.NotEmpty(t, GRPCServerOptions(nil, nil),
		"server options must carry the recovery interceptor with no auth and no TLS")
}
