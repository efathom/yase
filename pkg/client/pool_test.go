package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startDummyGRPCServer(t *testing.T) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	go srv.Serve(lis)
	return lis.Addr().String(), func() {
		srv.GracefulStop()
		lis.Close()
	}
}

func TestNewConnectionPool(t *testing.T) {
	addr, stop := startDummyGRPCServer(t)
	defer stop()

	pool, err := NewConnectionPool(addr, 3, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	if len(pool.conns) != 3 {
		t.Errorf("expected 3 connections, got %d", len(pool.conns))
	}
}

func TestRoundRobinDistribution(t *testing.T) {
	addr, stop := startDummyGRPCServer(t)
	defer stop()

	pool, err := NewConnectionPool(addr, 3, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	// Track which connections are returned
	counts := make(map[*grpc.ClientConn]int)
	for i := 0; i < 9; i++ {
		conn := pool.Get()
		counts[conn]++
	}

	// Each connection should be returned exactly 3 times
	for conn, count := range counts {
		if count != 3 {
			t.Errorf("connection %v returned %d times, expected 3", conn, count)
		}
	}
	if len(counts) != 3 {
		t.Errorf("expected 3 distinct connections, got %d", len(counts))
	}
}

func TestExecuteSuccess(t *testing.T) {
	addr, stop := startDummyGRPCServer(t)
	defer stop()

	pool, err := NewConnectionPool(addr, 2, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	called := false
	err = pool.Execute(context.Background(), func(ctx context.Context, conn *grpc.ClientConn) error {
		called = true
		if conn == nil {
			return fmt.Errorf("nil connection")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("execute error: %v", err)
	}
	if !called {
		t.Error("function was not called")
	}
}

func TestCircuitBreakerTrips(t *testing.T) {
	addr, stop := startDummyGRPCServer(t)
	defer stop()

	pool, err := NewConnectionPool(addr, 1, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	errFail := errors.New("simulated failure")

	// Send enough failures to trip the breaker (need >= 10 requests, >= 50% failure)
	for i := 0; i < 15; i++ {
		pool.Execute(context.Background(), func(ctx context.Context, conn *grpc.ClientConn) error {
			return errFail
		})
	}

	// Next call should be rejected by circuit breaker
	err = pool.Execute(context.Background(), func(ctx context.Context, conn *grpc.ClientConn) error {
		return nil // Would succeed, but breaker should be open
	})
	if err == nil {
		t.Error("expected circuit breaker to reject request")
	}
}

func TestPoolDefaultSize(t *testing.T) {
	addr, stop := startDummyGRPCServer(t)
	defer stop()

	pool, err := NewConnectionPool(addr, 0, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	if len(pool.conns) != 4 {
		t.Errorf("expected default 4 connections, got %d", len(pool.conns))
	}
}

func TestPoolClose(t *testing.T) {
	addr, stop := startDummyGRPCServer(t)
	defer stop()

	pool, err := NewConnectionPool(addr, 2, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}

	err = pool.Close()
	if err != nil {
		t.Errorf("close error: %v", err)
	}
}
