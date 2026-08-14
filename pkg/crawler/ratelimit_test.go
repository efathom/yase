package crawler

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupMiniredis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return mr, client
}

func TestRateLimiterAllows(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	rl := NewGlobalRateLimiter(client)
	ctx := context.Background()

	// Allow 3 requests per second
	for i := 0; i < 3; i++ {
		if !rl.AllowRequest(ctx, "example.com", 3, time.Second) {
			t.Errorf("request %d should be allowed", i+1)
		}
	}
}

func TestRateLimiterDenies(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	rl := NewGlobalRateLimiter(client)
	ctx := context.Background()

	// Fill the limit
	for i := 0; i < 3; i++ {
		rl.AllowRequest(ctx, "example.com", 3, time.Second)
	}

	// 4th request should be denied
	if rl.AllowRequest(ctx, "example.com", 3, time.Second) {
		t.Error("4th request should be denied")
	}
}

func TestRateLimiterPerDomain(t *testing.T) {
	mr, client := setupMiniredis(t)
	defer mr.Close()
	defer client.Close()

	rl := NewGlobalRateLimiter(client)
	ctx := context.Background()

	// Fill domain A
	rl.AllowRequest(ctx, "a.com", 1, time.Second)

	// Domain B should still be allowed
	if !rl.AllowRequest(ctx, "b.com", 1, time.Second) {
		t.Error("different domain should be allowed independently")
	}
}

func TestRateLimiterFailClosed(t *testing.T) {
	// Use a client pointing to a non-existent Redis
	client := redis.NewClient(&redis.Options{Addr: "localhost:59999"})
	defer client.Close()

	rl := NewGlobalRateLimiter(client)
	ctx := context.Background()

	// Should return false (fail-closed) when Redis is unavailable
	if rl.AllowRequest(ctx, "example.com", 10, time.Second) {
		t.Error("expected fail-closed on Redis error")
	}
}
