package auth

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// TokenCredentials attaches a service token to every outbound gRPC call.
//
// It is the client-side counterpart to UnaryServerInterceptor: with auth
// enabled, servers demand credentials that no client would otherwise send,
// which fails internal traffic (crawler→ingestion, gateway→shard,
// parser→PDF service) with "Unauthenticated: missing credentials".
type TokenCredentials struct {
	Token string
	// RequireTLS refuses to send the token over an insecure connection.
	// Set it whenever TLS is configured, so a misconfigured dial cannot leak
	// the credential in cleartext.
	RequireTLS bool
}

// NewTokenCredentials creates per-RPC credentials carrying token.
func NewTokenCredentials(token string) *TokenCredentials {
	return &TokenCredentials{Token: token}
}

// GetRequestMetadata returns the authorization header for each RPC. The header
// name and "Bearer " prefix match what extractGRPCToken reads server-side.
func (c *TokenCredentials) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	if c.Token == "" {
		return nil, nil
	}
	return map[string]string{"authorization": "Bearer " + c.Token}, nil
}

// RequireTransportSecurity reports whether the credential may only be sent
// over a secure transport.
func (c *TokenCredentials) RequireTransportSecurity() bool {
	return c.RequireTLS
}

// incomingContextFromMap builds a server-side context from client metadata.
// Used to verify that what the client sends is what the server reads.
func incomingContextFromMap(md map[string]string) context.Context {
	pairs := make([]string, 0, len(md)*2)
	for k, v := range md {
		pairs = append(pairs, k, v)
	}
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(pairs...))
}
