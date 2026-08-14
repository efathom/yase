package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efathom/yase/internal/idempotency"
	"github.com/efathom/yase/pkg/crawler"
	"github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/grpc/metadata"
)

func startRedisStack(t *testing.T) *redis.Client {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	ctx := context.Background()
	container, err := tcredis.Run(ctx, "redis/redis-stack-server:latest")
	if err != nil {
		t.Fatalf("start redis container: %v", err)
	}
	t.Cleanup(func() { container.Terminate(ctx) })

	connStr, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("redis connection string: %v", err)
	}

	opts, err := redis.ParseURL(connStr)
	if err != nil {
		t.Fatalf("parse redis URL: %v", err)
	}

	client := redis.NewClient(opts)
	t.Cleanup(func() { client.Close() })

	// Wait for Redis to be ready
	for i := 0; i < 30; i++ {
		if err := client.Ping(ctx).Err(); err == nil {
			return client
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("redis did not become ready")
	return nil
}

func TestBloomFilterConcurrent(t *testing.T) {
	client := startRedisStack(t)
	ctx := context.Background()

	bloom := crawler.NewBloomFilter(client, "test:bloom")

	// Add a URL — should be new
	isNew, err := bloom.CheckAndAdd(ctx, "https://example.com/page1")
	if err != nil {
		t.Fatalf("first add: %v", err)
	}
	if !isNew {
		t.Error("first add should be new")
	}

	// Add same URL — should be duplicate
	isNew, err = bloom.CheckAndAdd(ctx, "https://example.com/page1")
	if err != nil {
		t.Fatalf("second add: %v", err)
	}
	if isNew {
		t.Error("second add should be duplicate")
	}

	// Concurrent adds of unique URLs
	var wg sync.WaitGroup
	var newCount atomic.Int32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			url := "https://example.com/concurrent/" + string(rune('a'+i%26)) + string(rune('0'+i/26))
			isNew, err := bloom.CheckAndAdd(ctx, url)
			if err != nil {
				t.Errorf("concurrent add %d: %v", i, err)
				return
			}
			if isNew {
				newCount.Add(1)
			}
		}(i)
	}
	wg.Wait()

	if newCount.Load() == 0 {
		t.Error("expected at least some new URLs")
	}
}

func TestIdempotencyLockAcquireRelease(t *testing.T) {
	client := startRedisStack(t)

	mgr := idempotency.NewManager(client)

	md := metadata.Pairs("x-idempotency-key", "integration-test-001")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	// First acquire
	_, dup, err := mgr.CheckAndLock(ctx)
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	if dup {
		t.Error("first call should not be duplicate")
	}

	// Second acquire — same key
	_, dup, err = mgr.CheckAndLock(ctx)
	if err != nil {
		t.Fatalf("second lock: %v", err)
	}
	if !dup {
		t.Error("second call should be duplicate")
	}

	// Different key should succeed
	md2 := metadata.Pairs("x-idempotency-key", "integration-test-002")
	ctx2 := metadata.NewIncomingContext(context.Background(), md2)
	_, dup, err = mgr.CheckAndLock(ctx2)
	if err != nil {
		t.Fatalf("different key: %v", err)
	}
	if dup {
		t.Error("different key should not be duplicate")
	}
}

func TestRateLimiterSlidingWindow(t *testing.T) {
	client := startRedisStack(t)
	ctx := context.Background()

	rl := crawler.NewGlobalRateLimiter(client)
	domain := "ratelimit-test.example.com"
	limit := 3
	window := 2 * time.Second

	// Should allow exactly 3 requests
	for i := 0; i < limit; i++ {
		if !rl.AllowRequest(ctx, domain, limit, window) {
			t.Errorf("request %d should be allowed", i+1)
		}
	}

	// 4th request should be denied
	if rl.AllowRequest(ctx, domain, limit, window) {
		t.Error("request beyond limit should be denied")
	}

	// Wait for window to expire, then should allow again
	time.Sleep(window + 500*time.Millisecond)
	if !rl.AllowRequest(ctx, domain, limit, window) {
		t.Error("request after window expiry should be allowed")
	}
}
