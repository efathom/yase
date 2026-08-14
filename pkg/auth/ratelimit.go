package auth

import (
	"net/http"
	"sync"

	"golang.org/x/time/rate"
)

// RateLimitConfig configures per-tenant rate limiting.
type RateLimitConfig struct {
	Enabled          bool                   `json:"enabled" yaml:"enabled"`
	DefaultSearchQPS float64                `json:"default_search_qps" yaml:"default_search_qps"`
	DefaultIngestRPS float64                `json:"default_ingest_rps" yaml:"default_ingest_rps"`
	PerTenant        map[string]TenantLimit `json:"per_tenant" yaml:"per_tenant"`
}

// TenantLimit defines rate limits for a specific tenant.
type TenantLimit struct {
	SearchQPS float64 `json:"search_qps" yaml:"search_qps"`
	IngestRPS float64 `json:"ingest_rps" yaml:"ingest_rps"`
}

// RateLimiter enforces per-tenant request rate limits.
type RateLimiter struct {
	config      RateLimitConfig
	mu          sync.Mutex
	limiters    map[string]*rate.Limiter
	maxLimiters int
}

// maxRateLimiterEntries bounds the number of distinct tenant/scope limiters
// held in memory to prevent unbounded growth.
const maxRateLimiterEntries = 10000

// NewRateLimiter creates a per-tenant rate limiter.
func NewRateLimiter(cfg RateLimitConfig) *RateLimiter {
	return &RateLimiter{
		config:      cfg,
		limiters:    make(map[string]*rate.Limiter),
		maxLimiters: maxRateLimiterEntries,
	}
}

func (rl *RateLimiter) getLimiter(tenantID, scope string) *rate.Limiter {
	key := tenantID + ":" + scope

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if limiter, ok := rl.limiters[key]; ok {
		return limiter
	}

	var qps float64
	if tenant, ok := rl.config.PerTenant[tenantID]; ok {
		switch scope {
		case "search":
			qps = tenant.SearchQPS
		case "ingest":
			qps = tenant.IngestRPS
		}
	}
	if qps <= 0 {
		switch scope {
		case "search":
			qps = rl.config.DefaultSearchQPS
		case "ingest":
			qps = rl.config.DefaultIngestRPS
		}
	}
	if qps <= 0 {
		qps = 100 // fallback default
	}

	// Bound the map: evict an arbitrary entry when at capacity.
	if len(rl.limiters) >= rl.maxLimiters {
		for k := range rl.limiters {
			delete(rl.limiters, k)
			break
		}
	}

	limiter := rate.NewLimiter(rate.Limit(qps), int(qps))
	rl.limiters[key] = limiter
	return limiter
}

// HTTPMiddleware returns an HTTP middleware that enforces rate limits.
// Must be used after auth middleware (needs AuthContext).
func (rl *RateLimiter) HTTPMiddleware(scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !rl.config.Enabled {
				next.ServeHTTP(w, r)
				return
			}

			ac := FromContext(r.Context())
			tenantID := "default"
			if ac != nil {
				tenantID = ac.TenantID
			}

			limiter := rl.getLimiter(tenantID, scope)
			if !limiter.Allow() {
				w.Header().Set("Retry-After", "1")
				http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
