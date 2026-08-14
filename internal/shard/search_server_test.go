package shard

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/efathom/yase/internal/query"
	"github.com/efathom/yase/pkg/index"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestSearchServerLocalShard(t *testing.T) {
	engine := createTestEngine(t, 50, 16)

	server := NewSearchServer()
	server.RegisterEngine(0, engine)

	ctx := context.Background()
	resp, err := server.SearchShard(ctx, &ingestionv1.ShardSearchRequest{
		ShardId:     0,
		Query:       "test document",
		QueryVector: randomVec(16),
		TopK:        5,
	})
	if err != nil {
		t.Fatalf("SearchShard: %v", err)
	}
	if len(resp.Results) == 0 {
		t.Error("expected results from local shard")
	}
	t.Logf("Local shard search: %d results", len(resp.Results))
}

func TestSearchServerMissingShard(t *testing.T) {
	server := NewSearchServer()
	_, err := server.SearchShard(context.Background(), &ingestionv1.ShardSearchRequest{
		ShardId: 99,
		Query:   "test",
		TopK:    5,
	})
	if err == nil {
		t.Error("expected error for missing shard")
	}
}

func TestSearchServerGRPCRoundTrip(t *testing.T) {
	dim := 16
	engine := createTestEngine(t, 30, dim)

	// Start gRPC server
	server := NewSearchServer()
	server.RegisterEngine(0, engine)

	grpcServer := grpc.NewServer()
	ingestionv1.RegisterShardSearchServiceServer(grpcServer, server)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go grpcServer.Serve(lis)
	defer grpcServer.GracefulStop()

	// Connect client
	addr := lis.Addr().String()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := ingestionv1.NewShardSearchServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.SearchShard(ctx, &ingestionv1.ShardSearchRequest{
		ShardId:     0,
		Query:       "test document",
		QueryVector: randomVec(dim),
		TopK:        5,
	})
	if err != nil {
		t.Fatalf("gRPC SearchShard: %v", err)
	}
	if len(resp.Results) == 0 {
		t.Error("expected results via gRPC")
	}
	t.Logf("gRPC round-trip: %d results from shard %d", len(resp.Results), resp.ShardId)
}

func TestRemoteShardSearcherE2E(t *testing.T) {
	dim := 16

	// Start 2 shard servers on different ports
	engines := make(map[uint32]*index.HybridEngine)
	addrs := make(map[uint32]string)
	var grpcServers []*grpc.Server

	for _, shardID := range []uint32{0, 1} {
		engine := createTestEngine(t, 20, dim)
		engines[shardID] = engine

		srv := NewSearchServer()
		srv.RegisterEngine(shardID, engine)

		grpcSrv := grpc.NewServer()
		ingestionv1.RegisterShardSearchServiceServer(grpcSrv, srv)
		grpcServers = append(grpcServers, grpcSrv)

		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen shard %d: %v", shardID, err)
		}
		addrs[shardID] = lis.Addr().String()
		go grpcSrv.Serve(lis)
	}
	defer func() {
		for _, s := range grpcServers {
			s.GracefulStop()
		}
	}()

	// Remote searcher
	searcher := NewRemoteShardSearcher()
	searcher.UpdateTopology(addrs)
	defer searcher.Close()

	// Scatter-gather coordinator with static alias
	resolver := &testAliasResolver{
		shardIDs: []uint32{0, 1},
	}

	coordinator := &query.Coordinator{
		Resolver: resolver,
		Searcher: searcher,
		Timeout:  5 * time.Second,
	}

	resp, err := coordinator.Search(context.Background(), &query.SearchRequest{
		Alias:       "test",
		Query:       "test document",
		QueryVector: randomVec(dim),
		TopK:        5,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	t.Logf("Distributed search: %d results from %d shards (%d errors)",
		len(resp.Results), resp.ShardCount, len(resp.Errors))

	if resp.ShardCount != 2 {
		t.Errorf("expected 2 shards, got %d", resp.ShardCount)
	}
	if len(resp.Results) == 0 {
		t.Error("expected search results")
	}

	for i, r := range resp.Results {
		t.Logf("  #%d: ID=%d fused=%.6f bm25=%.4f sem=%.4f",
			i+1, r.ID, r.FusedScore, r.BM25Score, r.SemanticScore)
	}
}

// ── Helpers ──

type testAliasResolver struct {
	shardIDs []uint32
}

func (r *testAliasResolver) ResolveAlias(name string) ([]uint32, map[string]string, error) {
	return r.shardIDs, nil, nil
}

func createTestEngine(t *testing.T, numDocs, dim int) *index.HybridEngine {
	t.Helper()
	dir := t.TempDir()
	engine, err := index.NewHybridEngine(dir+"/bluge", 16*1024*1024, dim, 5)
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < numDocs; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		err := engine.Ingest(context.Background(), index.Document{
			ID:       uint32(i + 1),
			Text:     fmt.Sprintf("test document number %d about search and indexing", i+1),
			Vector:   vec,
			Metadata: map[string]string{"shard": "test"},
		})
		if err != nil {
			t.Fatalf("ingest doc %d: %v", i, err)
		}
	}
	return engine
}

func randomVec(dim int) []float32 {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	return v
}
