package auth

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
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

	// redis, when set, enforces limits cluster-wide (shared by every gateway
	// replica) with a per-second fixed window. On Redis errors the limiter
	// falls back to the local token bucket rather than failing open.
	redis          redis.UniversalClient
	redisDownUntil atomic.Int64 // unix nanos; skip Redis until then after a failure
}

// redisBackoff is how long the limiter stays on the local bucket after a
// Redis error, so an outage does not add a reconnect delay to every request.
const redisBackoff = 5 * time.Second

// WithRedis makes the limiter enforce limits across all processes sharing c.
func (rl *RateLimiter) WithRedis(c redis.UniversalClient) *RateLimiter {
	rl.redis = c
	return rl
}

// limitFor returns the configured rate for tenant/scope.
func (rl *RateLimiter) limitFor(tenantID, scope string) float64 {
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
		qps = 100
	}
	return qps
}

// Allow reports whether one more request for tenant/scope is within limits.
func (rl *RateLimiter) Allow(ctx context.Context, tenantID, scope string) bool {
	if rl.redis != nil && time.Now().UnixNano() >= rl.redisDownUntil.Load() {
		limit := int64(math.Ceil(rl.limitFor(tenantID, scope)))
		key := fmt.Sprintf("yase:rl:%s:%s:%d", tenantID, scope, time.Now().Unix())
		pipe := rl.redis.TxPipeline()
		incr := pipe.Incr(ctx, key)
		pipe.Expire(ctx, key, 2*time.Second)
		if _, err := pipe.Exec(ctx); err == nil {
			return incr.Val() <= limit
		}
		// Redis unavailable: enforce locally instead of allowing everything,
		// and stop trying Redis for a while.
		rl.redisDownUntil.Store(time.Now().Add(redisBackoff).UnixNano())
	}
	return rl.getLimiter(tenantID, scope).Allow()
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

	qps := rl.limitFor(tenantID, scope)

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

			if !rl.Allow(r.Context(), tenantID, scope) {
				w.Header().Set("Retry-After", "1")
				http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
