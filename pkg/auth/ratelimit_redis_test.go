package auth

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisRateLimiterIsSharedAcrossInstances(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	cfg := RateLimitConfig{Enabled: true, DefaultSearchQPS: 5}
	a := NewRateLimiter(cfg).WithRedis(client)
	b := NewRateLimiter(cfg).WithRedis(client)

	allowed := 0
	for i := 0; i < 10; i++ {
		lim := a
		if i%2 == 1 {
			lim = b
		}
		if lim.Allow(context.Background(), "t1", "search") {
			allowed++
		}
	}
	// Two gateways share one 5/s budget (a second boundary may admit one
	// more window, hence the range).
	assert.GreaterOrEqual(t, allowed, 5)
	assert.LessOrEqual(t, allowed, 10-4)

	// Other tenants are independent.
	assert.True(t, a.Allow(context.Background(), "t2", "search"))
}

func TestRedisRateLimiterFallsBackLocallyWhenRedisDown(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), DialTimeout: 20 * time.Millisecond, MaxRetries: -1})
	defer client.Close()
	mr.Close()

	lim := NewRateLimiter(RateLimitConfig{Enabled: true, DefaultSearchQPS: 3}).WithRedis(client)
	allowed := 0
	for i := 0; i < 20; i++ {
		if lim.Allow(context.Background(), "t1", "search") {
			allowed++
		}
	}
	require.Positive(t, allowed)
	assert.LessOrEqual(t, allowed, 3, "local token bucket still limits")
}
