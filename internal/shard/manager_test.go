package shard

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/efathom/yase/internal/consensus"
	"github.com/efathom/yase/pkg/routing"
	"github.com/hashicorp/raft"
)

func TestShardManagerRouteKey(t *testing.T) {
	ring := routing.NewHashRing(256)
	ring.AddNode("node-a")
	ring.AddNode("node-b")

	sm := NewShardManager("node-a", ring, nil)

	// Same key always routes to same node
	n1 := sm.RouteKey("doc-123")
	n2 := sm.RouteKey("doc-123")
	if n1 != n2 {
		t.Errorf("inconsistent routing: %q vs %q", n1, n2)
	}
	if n1 != "node-a" && n1 != "node-b" {
		t.Errorf("unexpected node: %q", n1)
	}
}

func TestShardManagerRouteDocID(t *testing.T) {
	ring := routing.NewHashRing(256)
	ring.AddNode("a")
	ring.AddNode("b")
	ring.AddNode("c")

	sm := NewShardManager("a", ring, nil)

	// Different doc IDs should distribute across nodes
	counts := make(map[string]int)
	for i := uint32(0); i < 1000; i++ {
		counts[sm.RouteDocID(i)]++
	}
	if len(counts) < 2 {
		t.Error("documents should distribute across multiple nodes")
	}
	t.Logf("Distribution: %v", counts)
}

func TestShardManagerLocalShards(t *testing.T) {
	sm := NewShardManager("node-1", routing.NewHashRing(256), nil)

	sm.mu.Lock()
	sm.localShards[1] = &ShardInfo{ID: 1, State: ShardReady}
	sm.localShards[2] = &ShardInfo{ID: 2, State: ShardBuilding}
	sm.mu.Unlock()

	s, ok := sm.GetLocalShard(1)
	if !ok || s.State != ShardReady {
		t.Error("shard 1 should be ready")
	}

	ids := sm.LocalShardIDs()
	if len(ids) != 2 {
		t.Errorf("expected 2 local shards, got %d", len(ids))
	}

	sm.RemoveShard(1)
	_, ok = sm.GetLocalShard(1)
	if ok {
		t.Error("shard 1 should be removed")
	}
}

func TestShardManagerResolveAlias(t *testing.T) {
	node := startSingleNodeRaft(t)
	defer node.Shutdown()

	ring := routing.NewHashRing(256)
	sm := NewShardManager("node-0", ring, node)

	// Create alias via Raft
	err := node.Apply(&consensus.Command{
		Type: consensus.CmdUpdateAlias,
		UpdateAlias: &consensus.UpdateAlias{
			Name:     "prod",
			ShardIDs: []uint32{1, 2, 3},
			Filters:  map[string]string{"tenant": "acme"},
		},
	}, 5*time.Second)
	if err != nil {
		t.Fatalf("apply alias: %v", err)
	}

	shards, filters, err := sm.ResolveAlias("prod")
	if err != nil {
		t.Fatalf("ResolveAlias: %v", err)
	}
	if len(shards) != 3 {
		t.Errorf("expected 3 shards, got %d", len(shards))
	}
	if filters["tenant"] != "acme" {
		t.Errorf("filter: got %q, want acme", filters["tenant"])
	}

	// Non-existent alias
	_, _, err = sm.ResolveAlias("nonexistent")
	if err == nil {
		t.Error("expected error for missing alias")
	}
}

func TestShardManagerSwapAlias(t *testing.T) {
	node := startSingleNodeRaft(t)
	defer node.Shutdown()

	ring := routing.NewHashRing(256)
	sm := NewShardManager("node-0", ring, node)

	// Create initial alias
	ctx := context.Background()
	err := sm.SwapAlias(ctx, "prod", []uint32{1, 2})
	if err != nil {
		t.Fatalf("SwapAlias: %v", err)
	}

	shards, _, _ := sm.ResolveAlias("prod")
	if len(shards) != 2 {
		t.Fatalf("expected 2 shards, got %d", len(shards))
	}

	// Swap to new shards
	err = sm.SwapAlias(ctx, "prod", []uint32{3, 4, 5})
	if err != nil {
		t.Fatalf("SwapAlias v2: %v", err)
	}

	shards, _, _ = sm.ResolveAlias("prod")
	if len(shards) != 3 {
		t.Errorf("after swap: expected 3 shards, got %d", len(shards))
	}
	if shards[0] != 3 {
		t.Errorf("first shard: got %d, want 3", shards[0])
	}
}

func TestShardManagerRegisterShard(t *testing.T) {
	node := startSingleNodeRaft(t)
	defer node.Shutdown()

	ring := routing.NewHashRing(256)
	sm := NewShardManager("node-0", ring, node)

	ctx := context.Background()
	err := sm.RegisterShard(ctx, 42, []string{"node-0", "node-1"})
	if err != nil {
		t.Fatalf("RegisterShard: %v", err)
	}

	// Local tracking
	s, ok := sm.GetLocalShard(42)
	if !ok {
		t.Fatal("shard 42 not found locally")
	}
	if s.State != ShardReady {
		t.Errorf("state: got %d, want ShardReady", s.State)
	}

	// Raft state
	shard, ok := node.State().GetShard(42)
	if !ok {
		t.Fatal("shard 42 not in Raft state")
	}
	if len(shard.Replicas) != 2 {
		t.Errorf("replicas: got %d, want 2", len(shard.Replicas))
	}
}

func TestShardManagerShardsForQuery(t *testing.T) {
	node := startSingleNodeRaft(t)
	defer node.Shutdown()

	ring := routing.NewHashRing(256)
	sm := NewShardManager("node-0", ring, node)

	// Set up alias
	sm.SwapAlias(context.Background(), "search", []uint32{10, 20, 30})

	shards, err := sm.ShardsForQuery("search")
	if err != nil {
		t.Fatalf("ShardsForQuery: %v", err)
	}
	if len(shards) != 3 {
		t.Errorf("expected 3 shards, got %d", len(shards))
	}
}

// ── Helpers ──

func startSingleNodeRaft(t *testing.T) *consensus.RaftNode {
	t.Helper()

	_, trans := raft.NewInmemTransport("")
	node, err := consensus.NewRaftNodeWithTransport(consensus.RaftNodeConfig{
		NodeID:    "node-0",
		Bootstrap: true,
	}, trans)
	if err != nil {
		t.Fatalf("create raft node: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for leader")
		default:
			if node.IsLeader() {
				return node
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func init() {
	// Suppress unused import error
	_ = fmt.Sprintf
}
