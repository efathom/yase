package shard

import (
	"context"
	"fmt"
	"sync"

	"github.com/efathom/yase/internal/query"
	"github.com/efathom/yase/pkg/index"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RemoteShardSearcher implements query.ShardSearcher by calling remote
// cluster nodes via gRPC ShardSearchService.
type RemoteShardSearcher struct {
	mu      sync.RWMutex
	conns   map[string]*grpc.ClientConn // nodeAddr → conn
	nodes   map[uint32]string           // shardID → nodeAddr
	dialOpt grpc.DialOption
}

// NewRemoteShardSearcher creates a searcher that routes to remote shard nodes.
// Optional grpc.DialOption(s) configure transport credentials (e.g. TLS).
func NewRemoteShardSearcher(opts ...grpc.DialOption) *RemoteShardSearcher {
	var dialOpt grpc.DialOption
	if len(opts) == 0 {
		dialOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		dialOpt = opts[0]
	}
	return &RemoteShardSearcher{
		conns:   make(map[string]*grpc.ClientConn),
		nodes:   make(map[uint32]string),
		dialOpt: dialOpt,
	}
}

// UpdateTopology updates the shard-to-node mapping. Called when Raft state changes.
// Connections to nodes no longer present in the topology are closed.
func (r *RemoteShardSearcher) UpdateTopology(shardToNode map[uint32]string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Close and drop connections to addresses that left the topology.
	active := make(map[string]bool, len(shardToNode))
	for _, addr := range shardToNode {
		active[addr] = true
	}
	for addr, conn := range r.conns {
		if !active[addr] {
			conn.Close()
			delete(r.conns, addr)
		}
	}

	r.nodes = make(map[uint32]string, len(shardToNode))
	for k, v := range shardToNode {
		r.nodes[k] = v
	}
}

// SearchShard calls the remote node hosting the given shard via gRPC.
func (r *RemoteShardSearcher) SearchShard(ctx context.Context, shardID uint32, queryText string, queryVec []float32, filters map[string]string, topK int) ([]index.ScoredResult, error) {
	r.mu.RLock()
	addr, ok := r.nodes[shardID]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no node known for shard %d", shardID)
	}

	client, err := r.getClient(addr)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", addr, err)
	}

	resp, err := client.SearchShard(ctx, &ingestionv1.ShardSearchRequest{
		ShardId:     shardID,
		Query:       queryText,
		QueryVector: queryVec,
		Filters:     filters,
		TopK:        int32(topK),
	})
	if err != nil {
		return nil, err
	}

	results := make([]index.ScoredResult, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = index.ScoredResult{
			ID:            r.Id,
			FusedScore:    r.FusedScore,
			BM25Score:     r.Bm25Score,
			SemanticScore: r.SemanticScore,
		}
	}
	return results, nil
}

func (r *RemoteShardSearcher) getClient(addr string) (ingestionv1.ShardSearchServiceClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if conn, ok := r.conns[addr]; ok {
		return ingestionv1.NewShardSearchServiceClient(conn), nil
	}

	conn, err := grpc.NewClient(addr, r.dialOpt)
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
