package consensus

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/efathom/yase/pkg/collection"
	"github.com/hashicorp/raft"
)

// NodeState represents the lifecycle state of a cluster node.
type NodeState uint8

const (
	NodeActive   NodeState = iota
	NodeDraining           // graceful shutdown in progress
	NodeDead               // missed heartbeats / explicitly removed
)

// ShardState represents the lifecycle state of an index shard.
type ShardState uint8

const (
	ShardReady     ShardState = iota
	ShardBuilding             // offline build in progress
	ShardMigrating            // moving between nodes
)

// NodeInfo tracks a cluster node.
type NodeInfo struct {
	ID       string    `json:"id"`
	Address  string    `json:"address"`
	HTTPAddr string    `json:"http_addr,omitempty"`
	Shards   []uint32  `json:"shards"`
	State    NodeState `json:"state"`
	JoinedAt time.Time `json:"joined_at"`
}

// ShardInfo tracks an index shard and its replicas.
type ShardInfo struct {
	ID           uint32     `json:"id"`
	CollectionID string     `json:"collection_id,omitempty"` // owning collection
	Replicas     []string   `json:"replicas"`                // first is primary
	State        ShardState `json:"state"`
	Version      uint64     `json:"version"`
}

// CollectionInfo tracks a collection in the distributed cluster state.
type CollectionInfo struct {
	ID        string                      `json:"id"`
	TenantID  string                      `json:"tenant_id"`
	Name      string                      `json:"name"`
	ShardIDs  []uint32                    `json:"shard_ids"`
	Config    collection.CollectionConfig `json:"config"`
	Status    collection.CollectionStatus `json:"status"`
	CreatedAt time.Time                   `json:"created_at"`
	UpdatedAt time.Time                   `json:"updated_at"`
}

// AliasInfo maps a logical name to a set of shards with optional filters.
type AliasInfo struct {
	Name      string            `json:"name"`
	ShardIDs  []uint32          `json:"shard_ids"`
	Filters   map[string]string `json:"filters,omitempty"`
	UpdatedAt time.Time         `json:"updated_at"`
}

// ClusterState is the Raft FSM state replicated across all nodes.
type ClusterState struct {
	mu          sync.RWMutex
	Nodes       map[string]*NodeInfo       `json:"nodes"`
	Shards      map[uint32]*ShardInfo      `json:"shards"`
	Aliases     map[string]*AliasInfo      `json:"aliases"`
	Collections map[string]*CollectionInfo `json:"collections"`
	Version     uint64                     `json:"version"`
}

// NewClusterState creates an empty cluster state.
func NewClusterState() *ClusterState {
	return &ClusterState{
		Nodes:       make(map[string]*NodeInfo),
		Shards:      make(map[uint32]*ShardInfo),
		Aliases:     make(map[string]*AliasInfo),
		Collections: make(map[string]*CollectionInfo),
	}
}

// Apply is called by Raft when a log entry is committed by quorum.
// It deserializes the command and mutates the FSM state.
func (cs *ClusterState) Apply(log *raft.Log) interface{} {
	cmd, err := UnmarshalCommand(log.Data)
	if err != nil {
		return fmt.Errorf("unmarshal command: %w", err)
	}

	// Use the command timestamp when set (deterministic), otherwise the time
	// this log entry was appended.
	now := cmd.Timestamp
	if now.IsZero() {
		now = log.AppendedAt
	}
	if now.IsZero() {
		now = time.Now()
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	switch cmd.Type {
	case CmdRegisterNode:
		n := cmd.RegisterNode
		cs.Nodes[n.ID] = &NodeInfo{
			ID:       n.ID,
			Address:  n.Address,
			HTTPAddr: n.HTTPAddr,
			State:    NodeActive,
			JoinedAt: now,
		}

	case CmdRemoveNode:
		n := cmd.RemoveNode
		if node, ok := cs.Nodes[n.ID]; ok {
			node.State = NodeDead
		}

	case CmdAssignShard:
		s := cmd.AssignShard
		shard, ok := cs.Shards[s.ShardID]
		if !ok {
			shard = &ShardInfo{ID: s.ShardID, State: ShardReady}
			cs.Shards[s.ShardID] = shard
		}
		shard.Replicas = s.Replicas
		shard.Version++

		// Update node → shard mappings
		for _, nodeID := range s.Replicas {
			if node, ok := cs.Nodes[nodeID]; ok {
				if !containsUint32(node.Shards, s.ShardID) {
					node.Shards = append(node.Shards, s.ShardID)
				}
			}
		}

	case CmdUpdateAlias:
		a := cmd.UpdateAlias
		cs.Aliases[a.Name] = &AliasInfo{
			Name:      a.Name,
			ShardIDs:  a.ShardIDs,
			Filters:   a.Filters,
			UpdatedAt: now,
		}

	case CmdRemoveAlias:
		delete(cs.Aliases, cmd.RemoveAlias.Name)

	case CmdCreateCollection:
		c := cmd.CreateCollection
		cs.Collections[c.ID] = &CollectionInfo{
			ID:        c.ID,
			TenantID:  c.TenantID,
			Name:      c.Name,
			Config:    c.Config,
			Status:    collection.StatusReady,
			CreatedAt: now,
			UpdatedAt: now,
		}

	case CmdUpdateCollection:
		u := cmd.UpdateCollection
		col, ok := cs.Collections[u.ID]
		if ok {
			if u.Name != "" {
				col.Name = u.Name
			}
			if u.Config != nil {
				col.Config = *u.Config
			}
			if u.ShardIDs != nil {
				col.ShardIDs = u.ShardIDs
			}
			if u.Status != "" {
				col.Status = u.Status
			}
			col.UpdatedAt = now
		}

	case CmdDeleteCollection:
		delete(cs.Collections, cmd.DeleteCollection.ID)

	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}

	cs.Version++
	return nil
}

// Snapshot returns a consistent point-in-time snapshot for Raft log compaction.
func (cs *ClusterState) Snapshot() (raft.FSMSnapshot, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	data, err := json.Marshal(cs)
	if err != nil {
		return nil, fmt.Errorf("snapshot marshal: %w", err)
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore replaces the FSM state from a snapshot.
func (cs *ClusterState) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("snapshot read: %w", err)
	}

	var restored ClusterState
	if err := json.Unmarshal(data, &restored); err != nil {
		return fmt.Errorf("snapshot unmarshal: %w", err)
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.Nodes = restored.Nodes
	cs.Shards = restored.Shards
	cs.Aliases = restored.Aliases
	cs.Collections = restored.Collections

	// Nil-guard all maps against old/corrupt snapshots.
	if cs.Nodes == nil {
		cs.Nodes = make(map[string]*NodeInfo)
	}
	if cs.Shards == nil {
		cs.Shards = make(map[uint32]*ShardInfo)
	}
	if cs.Aliases == nil {
		cs.Aliases = make(map[string]*AliasInfo)
	}
	if cs.Collections == nil {
		cs.Collections = make(map[string]*CollectionInfo)
	}
	cs.Version = restored.Version
	return nil
}

// GetNode returns a copy of the node info (thread-safe read).
func (cs *ClusterState) GetNode(id string) (*NodeInfo, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	n, ok := cs.Nodes[id]
	if !ok {
		return nil, false
	}
	cp := *n
	return &cp, true
}

// GetShard returns a copy of the shard info (thread-safe read).
func (cs *ClusterState) GetShard(id uint32) (*ShardInfo, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	s, ok := cs.Shards[id]
	if !ok {
		return nil, false
	}
	cp := *s
	return &cp, true
}

// GetAlias returns a copy of the alias info (thread-safe read).
func (cs *ClusterState) GetAlias(name string) (*AliasInfo, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	a, ok := cs.Aliases[name]
	if !ok {
		return nil, false
	}
	cp := *a
	return &cp, true
}

// GetCollection returns a copy of the collection info (thread-safe read).
func (cs *ClusterState) GetCollection(id string) (*CollectionInfo, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	c, ok := cs.Collections[id]
	if !ok {
		return nil, false
	}
	cp := *c
	return &cp, true
}

// ListCollections returns copies of all collection infos (thread-safe read).
func (cs *ClusterState) ListCollections() []*CollectionInfo {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	result := make([]*CollectionInfo, 0, len(cs.Collections))
	for _, c := range cs.Collections {
		cp := *c
		result = append(result, &cp)
	}
	return result
}

// NodeCount returns the number of registered nodes.
func (cs *ClusterState) NodeCount() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return len(cs.Nodes)
}

// ShardCount returns the number of registered shards.
func (cs *ClusterState) ShardCount() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return len(cs.Shards)
}

// fsmSnapshot holds serialized FSM data for Raft.
type fsmSnapshot struct {
	data []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

func containsUint32(slice []uint32, val uint32) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}
