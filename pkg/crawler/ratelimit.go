package crawler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/efathom/yase/pkg/metrics"
	"github.com/redis/go-redis/v9"
)

// slidingWindowLua implements an atomic sliding-window rate limiter using
// Redis sorted sets. Timestamps are stored as microseconds for precision.
// The member includes a random suffix so concurrent requests in the same
// microsecond are counted distinctly (a bare timestamp would collapse them).
const slidingWindowLua = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local nonce = tostring(ARGV[4])

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)

local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, now, tostring(now) .. ':' .. nonce)
    redis.call('EXPIRE', key, math.ceil(window / 1000000) + 1)
    return 1
else
    return 0
end
`

// GlobalRateLimiter enforces per-domain request limits using a Redis-backed
// sliding window. All crawler nodes share the same Redis state for global
// coordination.
type GlobalRateLimiter struct {
	client *redis.Client
	script *redis.Script
}

// NewGlobalRateLimiter creates a rate limiter backed by the given Redis client.
func NewGlobalRateLimiter(client *redis.Client) *GlobalRateLimiter {
	return &GlobalRateLimiter{
		client: client,
		script: redis.NewScript(slidingWindowLua),
	}
}

// AllowRequest checks if a request to the given domain is allowed under the
// rate limit. Returns true if allowed, false if denied.
// On denial, injects randomized jitter (0-500ms) to desynchronize the
// thundering herd. Fail-closed: returns false on Redis errors to prevent
// accidental target DoS.
func (rl *GlobalRateLimiter) AllowRequest(ctx context.Context, domain string, limit int, window time.Duration) bool {
	nowMicro := time.Now().UnixMicro()
	windowMicro := window.Microseconds()
	key := fmt.Sprintf("ratelimit:%s", domain)

	res, err := rl.script.Run(ctx, rl.client, []string{key}, nowMicro, windowMicro, limit, rand.Int63()).Result()
	if err != nil {
		metrics.CrawlerRateLimitErrors.Inc()
		return false // Fail-closed on Redis error
	}

	allowed, ok := res.(int64)
	if !ok {
		return false
	}
	if allowed != 1 {
		// Randomized jitter to smooth the thundering herd
		jitter := time.Duration(rand.Intn(500)) * time.Millisecond
		time.Sleep(jitter)
	}
	return allowed == 1
}
