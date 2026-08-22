package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// failingAuthenticator rejects everything with a detailed internal reason.
type failingAuthenticator struct{}

func (failingAuthenticator) Authenticate(_ context.Context, _ string) (*AuthContext, error) {
	return nil, errDetailed
}

var errDetailed = &detailedError{}

type detailedError struct{}

func (*detailedError) Error() string {
	return `invalid JWT issuer: got "attacker", want "https://idp.internal.example.com"`
}

// M-12: the interceptor formatted the underlying error into the status
// message, handing an unauthenticated caller the expected issuer and audience.
// The HTTP middleware already logs the detail and returns a fixed string.
func TestUnaryInterceptorDoesNotLeakAuthFailureDetail(t *testing.T) {
	interceptor := UnaryServerInterceptor(failingAuthenticator{})

	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer forged-token"))

	_, err := interceptor(ctx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/index.IndexService/Search"},
		func(context.Context, any) (any, error) { return "ok", nil })

	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))

	msg := status.Convert(err).Message()
	assert.NotContains(t, msg, "idp.internal.example.com",
		"the expected issuer must not be echoed to an unauthenticated caller")
	assert.NotContains(t, msg, "issuer", "no internal reason should reach the caller")
}

func TestStreamInterceptorDoesNotLeakAuthFailureDetail(t *testing.T) {
	interceptor := StreamServerInterceptor(failingAuthenticator{})

	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer forged-token"))

	err := interceptor(nil, &ctxStream{ctx: ctx},
		&grpc.StreamServerInfo{FullMethod: "/index.IndexService/Stream"},
		func(any, grpc.ServerStream) error { return nil })

	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.NotContains(t, status.Convert(err).Message(), "idp.internal.example.com")
}

type ctxStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *ctxStream) Context() context.Context { return s.ctx }

// L-18: subtle.ConstantTimeCompare returns 0 immediately when lengths differ,
// so comparing raw keys still leaks the configured key length. Comparing
// fixed-width digests removes that signal.
func TestAPIKeyAuthenticatorAcceptsValidKey(t *testing.T) {
	a := NewAPIKeyAuthenticator([]APIKeyEntry{
		{Key: "secret-key-one", TenantID: "tenant-a", Roles: []string{"reader"}},
		{Key: "a-much-longer-secret-key-two", TenantID: "tenant-b", Roles: []string{"admin"}},
	})

	ac, err := a.Authenticate(context.Background(), "a-much-longer-secret-key-two")
	require.NoError(t, err)
	assert.Equal(t, "tenant-b", ac.TenantID)
	assert.True(t, ac.HasScope("admin"))
}

func TestAPIKeyAuthenticatorRejectsUnknownKey(t *testing.T) {
	a := NewAPIKeyAuthenticator([]APIKeyEntry{
		{Key: "secret-key-one", TenantID: "tenant-a", Roles: []string{"reader"}},
	})

	_, err := a.Authenticate(context.Background(), "wrong")
	assert.Error(t, err)
}

// A key that is a prefix of a valid one must not authenticate.
func TestAPIKeyAuthenticatorRejectsPrefixOfValidKey(t *testing.T) {
	a := NewAPIKeyAuthenticator([]APIKeyEntry{
		{Key: "secret-key-one", TenantID: "tenant-a", Roles: []string{"reader"}},
	})

	_, err := a.Authenticate(context.Background(), "secret-key")
	assert.Error(t, err)
}

func TestAPIKeyRemoveKeyRevokesAccess(t *testing.T) {
	a := NewAPIKeyAuthenticator([]APIKeyEntry{
		{Key: "secret-key-one", TenantID: "tenant-a", Roles: []string{"reader"}},
	})
	require.NotNil(t, a)

	_, err := a.Authenticate(context.Background(), "secret-key-one")
	require.NoError(t, err)

	a.RemoveKey("secret-key-one")

	_, err = a.Authenticate(context.Background(), "secret-key-one")
	assert.Error(t, err, "a removed key must no longer authenticate")
}

func TestAPIKeyAddKeyGrantsAccess(t *testing.T) {
	a := NewAPIKeyAuthenticator(nil)

	a.AddKey(APIKeyEntry{Key: "brand-new-key", TenantID: "tenant-c", Roles: []string{"writer"}})

	ac, err := a.Authenticate(context.Background(), "brand-new-key")
	require.NoError(t, err)
	assert.Equal(t, "tenant-c", ac.TenantID)
}
