package auth

import (
	"context"
	"testing"

	"github.com/efathom/yase/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// H-07: every gRPC server installs the auth interceptor when auth is enabled,
// but no client ever attached credentials — so turning auth on broke all
// internal gRPC traffic with "Unauthenticated: missing credentials".
func TestTokenCredentialsAttachAuthorizationHeader(t *testing.T) {
	creds := NewTokenCredentials("service-token")

	md, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)

	assert.Equal(t, "Bearer service-token", md["authorization"],
		"the token must be sent in the same header the server interceptor reads")
}

// The metadata this produces must be readable by the server-side extractor —
// otherwise the two halves disagree about the header name.
func TestTokenCredentialsRoundTripThroughServerExtractor(t *testing.T) {
	creds := NewTokenCredentials("service-token")
	md, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)

	ctx := incomingContextFromMap(md)
	assert.Equal(t, "service-token", extractGRPCToken(ctx),
		"the server must recover exactly the token the client sent")
}

// Credentials travel in cleartext over an insecure connection, so the
// transport-security requirement must reflect whether TLS is configured.
func TestTokenCredentialsRequireTransportSecurityFollowsTLS(t *testing.T) {
	assert.False(t, NewTokenCredentials("t").RequireTransportSecurity(),
		"plaintext clusters must still be able to dial")

	secure := NewTokenCredentials("t")
	secure.RequireTLS = true
	assert.True(t, secure.RequireTransportSecurity())
}

// An empty token must produce no metadata rather than an empty Bearer header.
func TestTokenCredentialsWithEmptyTokenSendNothing(t *testing.T) {
	md, err := NewTokenCredentials("").GetRequestMetadata(context.Background())
	require.NoError(t, err)
	assert.Empty(t, md)
}

// ClientDialOptions must include the per-RPC credentials when a token is set.
func TestClientDialOptionsIncludeCredentialsWhenTokenConfigured(t *testing.T) {
	withToken, err := ClientDialOptions(config.AuthConfig{
		Enabled:     true,
		Method:      "api_key",
		ClientToken: "service-token",
	}, config.TLSConfig{})
	require.NoError(t, err)

	withoutToken, err := ClientDialOptions(config.AuthConfig{}, config.TLSConfig{})
	require.NoError(t, err)

	assert.Greater(t, len(withToken), len(withoutToken),
		"configuring a client token must add a dial option")
}
