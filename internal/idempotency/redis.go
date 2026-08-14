package idempotency

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Manager provides idempotency guarantees for gRPC ingestion requests
// using Redis SETNX locks keyed by the x-idempotency-key header.
type Manager struct {
	rdb *redis.Client
}

// NewManager creates an idempotency manager backed by the given Redis client.
func NewManager(rdb *redis.Client) *Manager {
	return &Manager{rdb: rdb}
}

// CheckAndLock extracts the x-idempotency-key from gRPC incoming metadata
// and attempts to acquire a SETNX lock with a 24-hour TTL.
// It returns the lock key (empty when no key was provided), whether the
// request is a duplicate (lock already held), and any error.
func (m *Manager) CheckAndLock(ctx context.Context) (key string, dup bool, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md.Get("x-idempotency-key")) == 0 {
		// No idempotency key — treat as new (non-idempotent request)
		return "", false, nil
	}

	key = "idemp:lock:" + md.Get("x-idempotency-key")[0]

	acquired, err := m.rdb.SetNX(ctx, key, "PROCESSING", 24*time.Hour).Result()
	if err != nil {
		return "", false, status.Errorf(codes.Internal, "redis error: %v", err)
	}

	return key, !acquired, nil // dup=true if lock was already held
}

// Complete marks an idempotency key as successfully processed, preserving the
// remaining TTL so late retries still report a duplicate with the result.
func (m *Manager) Complete(ctx context.Context, key, result string) {
	if key == "" {
		return
	}
	m.rdb.Set(ctx, key, "DONE:"+result, redis.KeepTTL)
}

// GetResult returns the cached completion result for a key, or "" if the key
// has not completed. Used to replay the original result on duplicate requests.
func (m *Manager) GetResult(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", nil
	}
	val, err := m.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(val, "DONE:") {
		return strings.TrimPrefix(val, "DONE:"), nil
	}
	return "", nil
}

// Release removes an idempotency lock so the client can safely retry after a
// partial/failed submission.
func (m *Manager) Release(ctx context.Context, key string) {
	if key == "" {
		return
	}
	m.rdb.Del(ctx, key)
}
