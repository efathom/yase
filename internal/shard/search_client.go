package shard

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/efathom/yase/internal/query"
	"github.com/efathom/yase/pkg/index"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// RemoteShardSearcher implements query.ShardSearcher by calling remote
// cluster nodes via gRPC ShardSearchService. Each shard may have several
// replicas; requests rotate across them and fail over to the next replica
// when one is unavailable.
type RemoteShardSearcher struct {
	mu       sync.RWMutex
	conns    map[string]*grpc.ClientConn // nodeAddr → conn
	replicas map[uint32][]string         // shardID → replica addresses
	next     atomic.Uint64               // round-robin cursor
	dialOpts []grpc.DialOption
}

// NewRemoteShardSearcher creates a searcher that routes to remote shard nodes.
// Optional grpc.DialOption(s) configure transport credentials and per-RPC
// credentials. All options are retained — keeping only the first would
// silently drop the auth credentials alongside the transport option.
func NewRemoteShardSearcher(opts ...grpc.DialOption) *RemoteShardSearcher {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &RemoteShardSearcher{
		conns:    make(map[string]*grpc.ClientConn),
		replicas: make(map[uint32][]string),
		dialOpts: opts,
	}
}

// UpdateTopology replaces the shard → replica-address mapping. Connections to
// addresses no longer present are closed.
func (r *RemoteShardSearcher) UpdateTopology(shardReplicas map[uint32][]string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	active := make(map[string]bool)
	for _, addrs := range shardReplicas {
		for _, a := range addrs {
			active[a] = true
		}
	}
	for addr, conn := range r.conns {
		if !active[addr] {
			conn.Close()
			delete(r.conns, addr)
		}
	}

	r.replicas = make(map[uint32][]string, len(shardReplicas))
	for k, v := range shardReplicas {
		r.replicas[k] = append([]string(nil), v...)
	}
}

// retryable reports whether another replica might succeed.
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Internal, codes.Unknown:
		return true
	}
	return false
}

// SearchShard queries one replica of the shard, failing over to the others.
func (r *RemoteShardSearcher) SearchShard(ctx context.Context, shardID uint32, queryText string, queryVec []float32, filters map[string]string, topK int) ([]index.ScoredResult, error) {
	r.mu.RLock()
	addrs := r.replicas[shardID]
	r.mu.RUnlock()
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no replica known for shard %d", shardID)
	}

	start := int(r.next.Add(1) % uint64(len(addrs)))
	var lastErr error
	for i := 0; i < len(addrs); i++ {
		if ctx.Err() != nil {
			break
		}
		addr := addrs[(start+i)%len(addrs)]
		client, err := r.getClient(addr)
		if err != nil {
			lastErr = fmt.Errorf("connect to %s: %w", addr, err)
			continue
		}
		resp, err := client.SearchShard(ctx, &ingestionv1.ShardSearchRequest{
			ShardId:     shardID,
			Query:       queryText,
			QueryVector: queryVec,
			Filters:     filters,
			TopK:        int32(topK),
		})
		if err != nil {
			lastErr = fmt.Errorf("replica %s: %w", addr, err)
			if !retryable(err) {
				return nil, lastErr
			}
			continue
		}
		results := make([]index.ScoredResult, len(resp.Results))
		for i, x := range resp.Results {
			results[i] = index.ScoredResult{
				ID:            x.Id,
				FusedScore:    x.FusedScore,
				BM25Score:     x.Bm25Score,
				SemanticScore: x.SemanticScore,
			}
		}
		return results, nil
	}
	if lastErr == nil {
		lastErr = ctx.Err()
	}
	return nil, lastErr
}

func (r *RemoteShardSearcher) getClient(addr string) (ingestionv1.ShardSearchServiceClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if conn, ok := r.conns[addr]; ok {
		return ingestionv1.NewShardSearchServiceClient(conn), nil
	}

	conn, err := grpc.NewClient(addr, r.dialOpts...)
	if err != nil {
		return nil, err
	}
	r.conns[addr] = conn
	return ingestionv1.NewShardSearchServiceClient(conn), nil
}

// Close shuts down all gRPC connections.
func (r *RemoteShardSearcher) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, conn := range r.conns {
		conn.Close()
	}
}

// Verify interface compliance.
var _ query.ShardSearcher = (*RemoteShardSearcher)(nil)
