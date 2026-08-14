package idempotency

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/metadata"
)

func TestCheckAndLock_NewRequest(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	mgr := NewManager(client)

	md := metadata.Pairs("x-idempotency-key", "req-001")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	key, dup, err := mgr.CheckAndLock(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dup {
		t.Error("first request should not be a duplicate")
	}
	if key == "" {
		t.Error("expected a non-empty lock key")
	}
}

func TestCheckAndLock_DuplicateRequest(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	mgr := NewManager(client)

	md := metadata.Pairs("x-idempotency-key", "req-002")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, dup1, err := mgr.CheckAndLock(ctx)
	if err != nil {
		t.Fatalf("first call error: %v", err)
	}
	if dup1 {
		t.Error("first call should not be duplicate")
	}

	_, dup2, err := mgr.CheckAndLock(ctx)
	if err != nil {
		t.Fatalf("second call error: %v", err)
	}
	if !dup2 {
		t.Error("second call should be a duplicate")
	}
}

func TestCheckAndLock_NoKey(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	mgr := NewManager(client)

	// No metadata at all
	key, dup, err := mgr.CheckAndLock(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dup {
		t.Error("missing key should be treated as non-duplicate")
	}
	if key != "" {
		t.Error("missing key should return an empty lock key")
	}

	// Metadata present but no idempotency key
	md := metadata.Pairs("other-header", "value")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	key, dup, err = mgr.CheckAndLock(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dup {
		t.Error("missing idempotency key should be treated as non-duplicate")
	}
	if key != "" {
		t.Error("missing idempotency key should return an empty lock key")
	}
}

func TestCheckAndLock_DifferentKeys(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	mgr := NewManager(client)

	ctx1 := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("x-idempotency-key", "key-a"))
	ctx2 := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("x-idempotency-key", "key-b"))

	_, dup1, _ := mgr.CheckAndLock(ctx1)
	_, dup2, _ := mgr.CheckAndLock(ctx2)

	if dup1 || dup2 {
		t.Error("different keys should not be duplicates of each other")
	}
}

func TestCheckAndLock_CompleteAndRelease(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	mgr := NewManager(client)

	md := metadata.Pairs("x-idempotency-key", "req-003")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	key, dup, err := mgr.CheckAndLock(ctx)
	if err != nil || dup {
		t.Fatalf("unexpected err=%v dup=%v", err, dup)
	}

	// Complete: retry should still be a duplicate.
	mgr.Complete(ctx, key, "42")
	if _, dup, _ := mgr.CheckAndLock(ctx); !dup {
		t.Error("expected duplicate after Complete")
	}

	// Release: retry should be allowed again.
	mgr.Release(ctx, key)
	if _, dup, _ := mgr.CheckAndLock(ctx); dup {
		t.Error("expected non-duplicate after Release")
	}
}
