package client

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sony/gobreaker/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ConnectionPool manages a round-robin pool of gRPC connections with
// circuit breaker protection and exponential backoff.
type ConnectionPool struct {
	conns   []*grpc.ClientConn
	counter atomic.Uint64
	cb      *gobreaker.CircuitBreaker[any]
}

// NewConnectionPool creates N gRPC connections to the target address
// and wraps them in a circuit breaker.
func NewConnectionPool(target string, poolSize int, opts ...grpc.DialOption) (*ConnectionPool, error) {
	if poolSize <= 0 {
		poolSize = 4
	}

	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}

	conns := make([]*grpc.ClientConn, poolSize)
	for i := 0; i < poolSize; i++ {
		cc, err := grpc.NewClient(target, opts...)
		if err != nil {
			// Close any already-opened connections
			for j := 0; j < i; j++ {
				conns[j].Close()
			}
			return nil, fmt.Errorf("dial connection %d: %w", i, err)
		}
		conns[i] = cc
	}

	cb := gobreaker.NewCircuitBreaker[any](gobreaker.Settings{
		Name:        "grpc-pool-" + target,
		MaxRequests: 3,
		Interval:    10 * time.Second,
		Timeout:     30 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			if counts.Requests < 10 {
				return false
			}
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return failureRatio >= 0.5
		},
	})

	return &ConnectionPool{
		conns: conns,
		cb:    cb,
	}, nil
}

// Get returns the next connection using round-robin selection.
func (p *ConnectionPool) Get() *grpc.ClientConn {
	idx := p.counter.Add(1) - 1
	return p.conns[idx%uint64(len(p.conns))]
}

// Execute runs fn against a round-robin connection, wrapped
// in the circuit breaker with a 5-second timeout.
func (p *ConnectionPool) Execute(ctx context.Context, fn func(ctx context.Context, conn *grpc.ClientConn) error) error {
	_, err := p.cb.Execute(func() (any, error) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		conn := p.Get()
		return nil, fn(timeoutCtx, conn)
	})
	return err
}

// Close shuts down all connections in the pool.
func (p *ConnectionPool) Close() error {
	var firstErr error
	for _, cc := range p.conns {
		if err := cc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
