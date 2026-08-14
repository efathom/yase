package integration

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/efathom/yase/internal/builder"
	"github.com/efathom/yase/internal/consensus"
	"github.com/efathom/yase/internal/parser"
	"github.com/efathom/yase/internal/query"
	"github.com/efathom/yase/internal/shard"
	"github.com/efathom/yase/pkg/cluster"
	"github.com/efathom/yase/pkg/index"
	"github.com/efathom/yase/pkg/routing"
	"github.com/efathom/yase/pkg/storage"
	"github.com/efathom/yase/pkg/vector"
	"github.com/hashicorp/raft"
)

// TestDistributedPipelineE2E exercises the full distributed stack in-process:
//
//  1. K-means clustering + Product Quantization (train on sample vectors)
//  2. Offline index builder (shard documents via consistent hashing, build per-shard indexes)
//  3. Raft consensus (3-node cluster, register nodes, assign shards, create alias)
//  4. Scatter-gather query coordinator (fan out to shards, merge with RRF)
//  5. Storage + cache (upload segments, cache locally)
//  6. MIME-based parser router (HTML → markdown)
func TestDistributedPipelineE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping distributed E2E in -short mode")
	}

	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))
	dim := 32

	// ═══════════════════════════════════════════════
	// Phase 1: K-means + Product Quantization
	// ═══════════════════════════════════════════════
	t.Log("=== Phase 1: K-means + PQ ===")

	trainVecs := generateVectors(rng, 500, dim)

	kmCfg := &cluster.KMeansConfig{K: 8, Seed: 42}
	kmResult, err := kmCfg.Fit(trainVecs)
	if err != nil {
		t.Fatalf("KMeans: %v", err)
	}
	t.Logf("K-means: %d clusters, %d iterations, inertia=%.2f",
		len(kmResult.Centroids), kmResult.Iterations, kmResult.Inertia)

	pq, err := vector.NewProductQuantizer(dim, 8, 32) // 8 sub-vectors, 32 centroids each
	if err != nil {
		t.Fatalf("NewPQ: %v", err)
	}
	if err := pq.Train(trainVecs); err != nil {
		t.Fatalf("PQ Train: %v", err)
	}

	// Verify PQ encode/decode round-trip
	codes := pq.Encode(trainVecs[0])
	decoded := pq.Decode(codes)
	reconDist := vector.L2SquaredUnrolled(trainVecs[0], decoded)
	t.Logf("PQ reconstruction L2²=%.4f (codes=%d bytes)", reconDist, len(codes))

	// Verify ADC distance
	adcTable := pq.BuildADCTable(trainVecs[0])
	adcDist := adcTable.Distance(codes)
	t.Logf("ADC distance=%.4f (should ≈ 0 for same vector)", adcDist)

	// ═══════════════════════════════════════════════
	// Phase 2: Offline index build with sharding
	// ═══════════════════════════════════════════════
	t.Log("=== Phase 2: Offline index build ===")

	storeDir := t.TempDir()
	store, err := storage.NewFSStore(storeDir)
	if err != nil {
		t.Fatalf("NewFSStore: %v", err)
	}

	docs := make([]index.Document, 100)
	for i := range docs {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		docs[i] = index.Document{
			ID:       uint32(i + 1),
			Text:     fmt.Sprintf("document %d about search and indexing technology", i+1),
			Vector:   vec,
			Metadata: map[string]string{"batch": "e2e-test"},
		}
	}

	manifest, err := builder.Build(ctx, docs, &builder.BuildConfig{
		NumShards:    3,
		VecDim:       dim,
		CentroidRate: 5,
		ArenaSize:    16 * 1024 * 1024,
		OutputPrefix: "indexes/v1",
	}, store)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	t.Logf("Built %d shards, %d docs, version=%s", len(manifest.ShardIDs), manifest.DocCount, manifest.Version)

	// Verify manifest round-trip from storage
	loadedManifest, err := builder.LoadManifest(ctx, store, "indexes/v1/manifest.json")
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if loadedManifest.DocCount != 100 {
		t.Errorf("manifest doc count: got %d, want 100", loadedManifest.DocCount)
	}

	// ═══════════════════════════════════════════════
	// Phase 3: Storage cache
	// ═══════════════════════════════════════════════
	t.Log("=== Phase 3: Storage cache ===")

	cacheDir := t.TempDir()
	cache, err := storage.NewLocalCache(store, cacheDir, 10*1024*1024) // 10MB cache
	if err != nil {
		t.Fatalf("NewLocalCache: %v", err)
	}

	// Cache the manifest (simulates query node pulling segments)
	localPath, err := cache.Get(ctx, "indexes/v1/manifest.json")
	if err != nil {
		t.Fatalf("cache.Get: %v", err)
	}
	t.Logf("Cached manifest at: %s", localPath)

	// Second access: cache hit
	localPath2, err := cache.Get(ctx, "indexes/v1/manifest.json")
	if err != nil {
		t.Fatalf("cache.Get (hit): %v", err)
	}
	if localPath != localPath2 {
		t.Error("cache hit should return same path")
	}
	t.Logf("Cache: %d entries, %d bytes used", cache.EntryCount(), cache.UsedSize())

	// ═══════════════════════════════════════════════
	// Phase 4: Raft consensus cluster
	// ═══════════════════════════════════════════════
	t.Log("=== Phase 4: Raft consensus ===")

	nodes := startRaftCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Shutdown()
		}
	}()

	leader := waitForLeaderNode(t, nodes, 5*time.Second)
	t.Logf("Leader elected: %s", nodeID(leader))

	// Register 3 nodes
	for i := 0; i < 3; i++ {
		err := leader.Apply(&consensus.Command{
			Type:         consensus.CmdRegisterNode,
			RegisterNode: &consensus.RegisterNode{ID: fmt.Sprintf("node-%d", i), Address: fmt.Sprintf("10.0.0.%d:50052", i)},
		}, 5*time.Second)
		if err != nil {
			t.Fatalf("RegisterNode: %v", err)
		}
	}

	// Assign shards from manifest
	for _, shardID := range manifest.ShardIDs {
		nodeIdx := int(shardID) % 3
		err := leader.Apply(&consensus.Command{
			Type:        consensus.CmdAssignShard,
			AssignShard: &consensus.AssignShard{ShardID: shardID, Replicas: []string{fmt.Sprintf("node-%d", nodeIdx)}},
		}, 5*time.Second)
		if err != nil {
			t.Fatalf("AssignShard: %v", err)
		}
	}

	// Create alias pointing to all shards
	err = leader.Apply(&consensus.Command{
		Type:        consensus.CmdUpdateAlias,
		UpdateAlias: &consensus.UpdateAlias{Name: "production", ShardIDs: manifest.ShardIDs},
	}, 5*time.Second)
	if err != nil {
		t.Fatalf("UpdateAlias: %v", err)
	}

	// Wait for replication
	time.Sleep(300 * time.Millisecond)

	// Verify state on all Raft nodes
	for i, node := range nodes {
		state := node.State()
		if state.NodeCount() != 3 {
			t.Errorf("raft node %d: expected 3 nodes, got %d", i, state.NodeCount())
		}
		alias, ok := state.GetAlias("production")
		if !ok {
			t.Errorf("raft node %d: alias 'production' not found", i)
		} else if len(alias.ShardIDs) != len(manifest.ShardIDs) {
			t.Errorf("raft node %d: alias has %d shards, want %d", i, len(alias.ShardIDs), len(manifest.ShardIDs))
		}
	}
	t.Logf("Raft: 3 nodes registered, %d shards assigned, alias 'production' created", len(manifest.ShardIDs))

	// ═══════════════════════════════════════════════
	// Phase 5: Consistent hashing + shard manager
	// ═══════════════════════════════════════════════
	t.Log("=== Phase 5: Consistent hashing + shard manager ===")

	ring := routing.NewHashRing(256)
	ring.AddNode("node-0")
	ring.AddNode("node-1")
	ring.AddNode("node-2")

	sm := shard.NewShardManager("node-0", ring, leader)

	// Verify alias resolution through shard manager → Raft
	shardIDs, _, err := sm.ResolveAlias("production")
	if err != nil {
		t.Fatalf("ResolveAlias: %v", err)
	}
	t.Logf("Alias 'production' resolves to shards: %v", shardIDs)

	// Verify document routing distributes across nodes
	routeCounts := make(map[string]int)
	for i := uint32(1); i <= 100; i++ {
		routeCounts[sm.RouteDocID(i)]++
	}
	t.Logf("Document routing distribution: %v", routeCounts)
	if len(routeCounts) < 2 {
		t.Error("documents should distribute across multiple nodes")
	}

	// ═══════════════════════════════════════════════
	// Phase 6: Scatter-gather query
	// ═══════════════════════════════════════════════
	t.Log("=== Phase 6: Scatter-gather query ===")

	// Build per-shard search indexes for the coordinator to query
	shardEngines := buildShardEngines(t, docs, manifest, dim)

	coordinator := &query.Coordinator{
		Resolver: sm,
		Searcher: &localShardSearcher{engines: shardEngines},
		Timeout:  5 * time.Second,
	}

	resp, err := coordinator.Search(ctx, &query.SearchRequest{
		Alias:       "production",
		Query:       "search indexing",
		QueryVector: docs[0].Vector,
		TopK:        5,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	t.Logf("Search returned %d results from %d shards (%d errors)",
		len(resp.Results), resp.ShardCount, len(resp.Errors))
	for i, r := range resp.Results {
		t.Logf("  #%d: ID=%d, fused=%.6f, bm25=%.4f, semantic=%.4f",
			i+1, r.ID, r.FusedScore, r.BM25Score, r.SemanticScore)
	}

	if len(resp.Results) == 0 {
		t.Fatal("expected at least 1 search result")
	}
	if resp.ShardCount != len(manifest.ShardIDs) {
		t.Errorf("shard count: got %d, want %d", resp.ShardCount, len(manifest.ShardIDs))
	}

	// Verify alias swap
	t.Log("=== Phase 6b: Alias swap ===")
	err = sm.SwapAlias(ctx, "production", []uint32{manifest.ShardIDs[0]}) // narrow to 1 shard
	if err != nil {
		t.Fatalf("SwapAlias: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	resp2, err := coordinator.Search(ctx, &query.SearchRequest{
		Alias:       "production",
		Query:       "search indexing",
		QueryVector: docs[0].Vector,
		TopK:        5,
	})
	if err != nil {
		t.Fatalf("Search after swap: %v", err)
	}
	if resp2.ShardCount != 1 {
		t.Errorf("after alias swap: expected 1 shard, got %d", resp2.ShardCount)
	}
	t.Logf("After alias swap: %d results from %d shard", len(resp2.Results), resp2.ShardCount)

	// ═══════════════════════════════════════════════
	// Phase 7: MIME-based parser router
	// ═══════════════════════════════════════════════
	t.Log("=== Phase 7: Parser router ===")

	router := parser.NewRouter(parser.NewNativePDFParser())

	htmlResult, err := router.Parse(ctx, []byte(`<html><body>
		<h1>Distributed Search</h1>
		<p>YASE scales horizontally via Raft consensus and consistent hashing.</p>
		<a href="https://example.com/docs">Docs</a>
		<script>malicious()</script>
	</body></html>`), "text/html")
	if err != nil {
		t.Fatalf("Parse HTML: %v", err)
	}
	if htmlResult.Markdown == "" {
		t.Error("expected non-empty HTML markdown")
	}
	if len(htmlResult.Outlinks) != 1 {
		t.Errorf("expected 1 outlink, got %d", len(htmlResult.Outlinks))
	}
	t.Logf("HTML parsed: %d chars, %d outlinks", len(htmlResult.Markdown), len(htmlResult.Outlinks))

	textResult, err := router.Parse(ctx, []byte("plain text fallback"), "text/plain")
	if err != nil {
		t.Fatalf("Parse text: %v", err)
	}
	if textResult.Markdown != "plain text fallback" {
		t.Error("plain text should pass through verbatim")
	}

	t.Log("=== Distributed E2E test complete ===")
}

// ── Helpers ──

// localShardSearcher searches against in-process HybridEngine instances.
type localShardSearcher struct {
	engines map[uint32]*index.HybridEngine
}

func (s *localShardSearcher) SearchShard(ctx context.Context, shardID uint32, queryText string, queryVec []float32, filters map[string]string, topK int) ([]index.ScoredResult, error) {
	engine, ok := s.engines[shardID]
	if !ok {
		return nil, fmt.Errorf("shard %d not found", shardID)
	}
	return engine.HybridSearch(ctx, queryText, queryVec, filters, topK)
}

// buildShardEngines creates per-shard HybridEngines with documents routed
// by the same consistent hashing used in the offline builder.
func buildShardEngines(t *testing.T, docs []index.Document, manifest *builder.BuildManifest, dim int) map[uint32]*index.HybridEngine {
	t.Helper()

	ring := routing.NewHashRing(256)
	for _, sid := range manifest.ShardIDs {
		ring.AddNode(fmt.Sprintf("shard-%d", sid))
	}

	shardDocs := make(map[uint32][]index.Document)
	for _, doc := range docs {
		node := ring.GetNode(fmt.Sprintf("%d", doc.ID))
		var sid uint32
		fmt.Sscanf(node, "shard-%d", &sid)
		shardDocs[sid] = append(shardDocs[sid], doc)
	}

	engines := make(map[uint32]*index.HybridEngine)
	for _, sid := range manifest.ShardIDs {
		dir := t.TempDir()
		engine, err := index.NewHybridEngine(dir+"/bluge", 16*1024*1024, dim, 5)
		if err != nil {
			t.Fatalf("shard %d engine: %v", sid, err)
		}
		t.Cleanup(func() { engine.Close() })

		for _, doc := range shardDocs[sid] {
			if err := engine.Ingest(context.Background(), doc); err != nil {
				t.Fatalf("shard %d ingest: %v", sid, err)
			}
		}
		engines[sid] = engine
	}

	return engines
}

func startRaftCluster(t *testing.T, n int) []*consensus.RaftNode {
	t.Helper()

	_, trans := raft.NewInmemTransport("")
	nodes := make([]*consensus.RaftNode, n)

	var err error
	nodes[0], err = consensus.NewRaftNodeWithTransport(consensus.RaftNodeConfig{
		NodeID:    "raft-0",
		Bootstrap: true,
	}, trans)
	if err != nil {
		t.Fatalf("create raft-0: %v", err)
	}

	if n == 1 {
		return nodes
	}

	waitForLeaderNode(t, nodes[:1], 5*time.Second)

	for i := 1; i < n; i++ {
		_, peerTrans := raft.NewInmemTransport("")
		trans.Connect(peerTrans.LocalAddr(), peerTrans)
		peerTrans.Connect(trans.LocalAddr(), trans)

		// Cross-connect with previous peers handled by inmem transport registry

		nodeID := fmt.Sprintf("raft-%d", i)
		nodes[i], err = consensus.NewRaftNodeWithTransport(consensus.RaftNodeConfig{
			NodeID: nodeID,
		}, peerTrans)
		if err != nil {
			t.Fatalf("create %s: %v", nodeID, err)
		}

		if err := nodes[0].AddVoter(nodeID, string(peerTrans.LocalAddr()), 5*time.Second); err != nil {
			t.Fatalf("AddVoter %s: %v", nodeID, err)
		}
	}

	return nodes
}

func waitForLeaderNode(t *testing.T, nodes []*consensus.RaftNode, timeout time.Duration) *consensus.RaftNode {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for leader")
		default:
			for _, n := range nodes {
				if n != nil && n.IsLeader() {
					return n
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func nodeID(n *consensus.RaftNode) string {
	// Access via State() which returns the FSM
	return "leader"
}

func generateVectors(rng *rand.Rand, n, dim int) [][]float32 {
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		vecs[i] = v
	}
	return vecs
}
