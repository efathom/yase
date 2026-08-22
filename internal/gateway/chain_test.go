package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/efathom/yase/pkg/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// staticAuthenticator maps a bearer token straight onto a tenant.
type staticAuthenticator struct{}

func (staticAuthenticator) Authenticate(_ context.Context, token string) (*auth.AuthContext, error) {
	if token == "" {
		return nil, auth.ErrUnauthorized
	}
	return &auth.AuthContext{
		TenantID: token, // token doubles as the tenant name
		UserID:   "u1",
		Roles:    []string{"reader"},
		Scopes:   []string{"search"},
	}, nil
}

// M-08: the rate limiter reads the tenant from the auth context, so it must run
// after the auth middleware. Wrapped in the wrong order it buckets every
// request under "default" and one tenant can exhaust everyone's budget.
func TestRateLimitBucketsArePerTenant(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// One request per second, burst of one: the second request from the same
	// tenant is rejected, but a different tenant still has its own budget.
	rl := auth.NewRateLimiter(auth.RateLimitConfig{
		Enabled:          true,
		DefaultSearchQPS: 1,
	})

	root := Chain(okHandler, ChainOptions{
		Authenticator: staticAuthenticator{},
		RateLimiter:   rl,
		RateScope:     "search",
		BodyLimit:     1 << 20,
	})

	do := func(tenant string) int {
		req := httptest.NewRequest("POST", "/search", nil)
		req.Header.Set("Authorization", "Bearer "+tenant)
		w := httptest.NewRecorder()
		root.ServeHTTP(w, req)
		return w.Code
	}

	// Drain tenant-a's burst.
	require.Equal(t, http.StatusOK, do("tenant-a"))
	require.Equal(t, http.StatusTooManyRequests, do("tenant-a"),
		"tenant-a's own budget should be exhausted")

	assert.Equal(t, http.StatusOK, do("tenant-b"),
		"tenant-b must have an independent budget from tenant-a")
}

// The auth middleware must still reject anonymous callers through the chain.
func TestChainRejectsMissingCredentials(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	root := Chain(okHandler, ChainOptions{
		Authenticator: staticAuthenticator{},
		BodyLimit:     1 << 20,
	})

	req := httptest.NewRequest("POST", "/search", nil)
	w := httptest.NewRecorder()
	root.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// Health probes must stay reachable without credentials.
func TestChainAllowsHealthProbes(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	root := Chain(okHandler, ChainOptions{
		Authenticator: staticAuthenticator{},
		BodyLimit:     1 << 20,
	})

	for _, path := range []string{"/health", "/ready"} {
		req := httptest.NewRequest("GET", path, nil)
		w := httptest.NewRecorder()
		root.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code, "%s must not require credentials", path)
	}
}

// A panic in a handler must become a 500, not take the process down.
func TestChainRecoversPanics(t *testing.T) {
	boom := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		panic(fmt.Errorf("boom"))
	})

	root := Chain(boom, ChainOptions{BodyLimit: 1 << 20})

	req := httptest.NewRequest("POST", "/search", nil)
	w := httptest.NewRecorder()
	root.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
