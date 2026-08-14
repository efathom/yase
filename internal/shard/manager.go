package shard

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/efathom/yase/internal/consensus"
	"github.com/efathom/yase/pkg/routing"
)

// ShardState represents the lifecycle state of a local shard.
type ShardState uint8

const (
	ShardReady     ShardState = iota
	ShardBuilding             // offline build in progress
	ShardReadOnly             // serving reads but no writes
	ShardMigrating            // moving to another node
)

// ShardInfo holds metadata for a shard (local or remote).
type ShardInfo struct {
	ID       uint32
	Replicas []string // nodeIDs: first is primary
	State    ShardState
}

// ShardManager coordinates shard lifecycle across the cluster.
// It owns the hash ring for document routing and reads cluster
// topology from the Raft FSM.
type ShardManager struct {
	mu          sync.RWMutex
	localShards map[uint32]*ShardInfo // shards hosted on this node
	ring        *routing.HashRing
	raft        *consensus.RaftNode
	nodeID      string
}

// NewShardManager creates a shard manager for the given node.
func NewShardManager(nodeID string, ring *routing.HashRing, raftNode *consensus.RaftNode) *ShardManager {
	return &ShardManager{
		localShards: make(map[uint32]*ShardInfo),
		ring:        ring,
		raft:        raftNode,
		nodeID:      nodeID,
	}
}

// RouteKey determines which node a key (e.g. document ID) maps to.
func (sm *ShardManager) RouteKey(key string) string {
	return sm.ring.GetNode(key)
}

// RouteDocID determines which node owns a document by its numeric ID.
func (sm *ShardManager) RouteDocID(docID uint32) string {
	return sm.ring.GetNode(strconv.FormatUint(uint64(docID), 10))
}

// ResolveAlias atomically resolves an alias name to its target shard IDs
// and embedded filters by reading from the Raft FSM.
func (sm *ShardManager) ResolveAlias(name string) ([]uint32, map[string]string, error) {
	state := sm.raft.State()
	alias, ok := state.GetAlias(name)
	if !ok {
		return nil, nil, fmt.Errorf("alias %q not found", name)
	}
	return alias.ShardIDs, alias.Filters, nil
}

// SwapAlias atomically redirects an alias to new shards via Raft.
func (sm *ShardManager) SwapAlias(ctx context.Context, name string, newShardIDs []uint32) error {
	cmd := &consensus.Command{
		Type: consensus.CmdUpdateAlias,
		UpdateAlias: &consensus.UpdateAlias{
			Name:     name,
			ShardIDs: newShardIDs,
		},
	}
	return sm.raft.Apply(cmd, 5*time.Second)
}

// RegisterShard records a shard as locally hosted on this node and
// submits the assignment to Raft for cluster-wide visibility.
func (sm *ShardManager) RegisterShard(ctx context.Context, shardID uint32, replicas []string) error {
	sm.mu.Lock()
	sm.localShards[shardID] = &ShardInfo{
		ID:       shardID,
		Replicas: replicas,
		State:    ShardReady,
	}
	sm.mu.Unlock()

	cmd := &consensus.Command{
		Type: consensus.CmdAssignShard,
		AssignShard: &consensus.AssignShard{
			ShardID:  shardID,
			Replicas: replicas,
		},
	}
	return sm.raft.Apply(cmd, 5*time.Second)
}

// GetLocalShard returns a locally hosted shard by ID.
func (sm *ShardManager) GetLocalShard(id uint32) (*ShardInfo, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	s, ok := sm.localShards[id]
	if !ok {
		return nil, false
	}
	cp := *s
	return &cp, true
}

// LocalShardIDs returns IDs of all shards hosted on this node.
func (sm *ShardManager) LocalShardIDs() []uint32 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	ids := make([]uint32, 0, len(sm.localShards))
	for id := range sm.localShards {
		ids = append(ids, id)
	}
	return ids
}

// ShardsForQuery returns the shard IDs that must be queried for a given
// alias. Returns all alias target shards.
func (sm *ShardManager) ShardsForQuery(alias string) ([]uint32, error) {
	shardIDs, _, err := sm.ResolveAlias(alias)
	return shardIDs, err
}

// RemoveShard removes a shard from local tracking.
func (sm *ShardManager) RemoveShard(id uint32) {
	sm.mu.Lock()
	delete(sm.localShards, id)
	sm.mu.Unlock()
}
