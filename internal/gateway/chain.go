package gateway

import (
	"net/http"

	"github.com/efathom/yase/pkg/auth"
)

// ChainOptions configures the gateway HTTP middleware stack. A nil or zero
// field disables that layer.
type ChainOptions struct {
	// Authenticator enforces credentials. Nil means auth is disabled.
	Authenticator auth.Authenticator
	// RateLimiter applies per-tenant limits. Nil means unlimited.
	RateLimiter *auth.RateLimiter
	// RateScope selects which configured limit applies ("search" or "ingest").
	RateScope string
	// LoadShedder rejects requests above a concurrency ceiling. Nil disables it.
	LoadShedder *auth.LoadShedder
	// BodyLimit caps the request body in bytes. Zero disables the cap.
	BodyLimit int64
}

// Chain wraps the gateway mux in the standard middleware stack, returning the
// handler to serve.
//
// Request order is load shed → auth → rate limit → body limit → recover → mux.
// The rate limiter reads the tenant from the auth context, so it must run
// after the authenticator: reversing the two silently buckets every request
// under one key and collapses per-tenant limits into a single global budget.
// Because each wrap puts a layer *outside* the previous one, the code below
// applies them in reverse of the request order.
func Chain(mux http.Handler, opts ChainOptions) http.Handler {
	root := Recover(mux)

	if opts.BodyLimit > 0 {
		root = BodyLimit(opts.BodyLimit)(root)
	}

	// Wrapped before the authenticator so it runs after it.
	if opts.RateLimiter != nil {
		scope := opts.RateScope
		if scope == "" {
			scope = "search"
		}
		root = opts.RateLimiter.HTTPMiddleware(scope)(root)
	}

	if opts.Authenticator != nil {
		root = auth.HTTPMiddleware(opts.Authenticator)(root)
	}

	if opts.LoadShedder != nil {
		root = opts.LoadShedder.HTTPMiddleware(root)
	}

	return root
}
