package consensus

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// RaftNodeConfig holds configuration for a Raft node.
type RaftNodeConfig struct {
	NodeID    string // unique node identifier
	BindAddr  string // host:port for Raft transport
	DataDir   string // directory for Raft logs and snapshots
	Bootstrap bool   // true for the first node in a new cluster
}

// RaftNode wraps a hashicorp/raft instance with the YASE cluster FSM.
type RaftNode struct {
	raft  *raft.Raft
	fsm   *ClusterState
	cfg   RaftNodeConfig
	trans raft.Transport
}

// NewRaftNode creates and starts a Raft node. If Bootstrap is true, the node
// self-elects as leader (use only for the first node in a new cluster).
func NewRaftNode(cfg RaftNodeConfig) (*RaftNode, error) {
	fsm := NewClusterState()

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.SnapshotThreshold = 1024
	raftCfg.SnapshotInterval = 30 * time.Second

	// Transport
	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("tcp transport: %w", err)
	}

	// Log store and stable store (BoltDB-backed)
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir data dir: %w", err)
	}

	boltPath := filepath.Join(cfg.DataDir, "raft.db")
	boltStore, err := raftboltdb.NewBoltStore(boltPath)
	if err != nil {
		return nil, fmt.Errorf("bolt store: %w", err)
	}
	logStore := boltStore
	stableStore := boltStore

	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("snapshot store: %w", err)
	}

	r, err := raft.NewRaft(raftCfg, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}

	if cfg.Bootstrap {
		config := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(cfg.NodeID),
					Address: raft.ServerAddress(cfg.BindAddr),
				},
			},
		}
		r.BootstrapCluster(config)
	}

	return &RaftNode{
		raft:  r,
		fsm:   fsm,
		cfg:   cfg,
		trans: transport,
	}, nil
}

// NewRaftNodeWithTransport creates a Raft node with a custom transport
// (useful for testing with in-memory transport).
func NewRaftNodeWithTransport(cfg RaftNodeConfig, trans raft.Transport) (*RaftNode, error) {
	fsm := NewClusterState()

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.SnapshotThreshold = 128
	raftCfg.SnapshotInterval = 5 * time.Second
	// Fast timeouts for testing
	raftCfg.HeartbeatTimeout = 200 * time.Millisecond
	raftCfg.ElectionTimeout = 200 * time.Millisecond
	raftCfg.LeaderLeaseTimeout = 100 * time.Millisecond
	raftCfg.CommitTimeout = 50 * time.Millisecond

	dataDir := filepath.Join(os.TempDir(), "yase-raft-test", cfg.NodeID)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	r, err := raft.NewRaft(raftCfg, fsm, logStore, stableStore, snapshotStore, trans)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}

	if cfg.Bootstrap {
		config := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(cfg.NodeID),
					Address: trans.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(config)
	}

	return &RaftNode{
		raft:  r,
		fsm:   fsm,
		cfg:   cfg,
		trans: trans,
	}, nil
}

// Apply submits a command to the Raft cluster. Only the leader can apply;
// followers return ErrNotLeader. The command timestamp is set here so all
// replicas apply the mutation with the same wall-clock time.
func (rn *RaftNode) Apply(cmd *Command, timeout time.Duration) error {
	if cmd.Timestamp.IsZero() {
		cmd.Timestamp = time.Now().UTC()
	}
	data, err := cmd.Marshal()
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}
	f := rn.raft.Apply(data, timeout)
	if err := f.Error(); err != nil {
		return err
	}
	if resp := f.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}
	return nil
}

// AddVoter adds a new node to the Raft cluster as a voting member.
func (rn *RaftNode) AddVoter(id, address string, timeout time.Duration) error {
	f := rn.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, timeout)
	return f.Error()
}

// RemoveServer removes a node from the Raft cluster.
func (rn *RaftNode) RemoveServer(id string, timeout time.Duration) error {
	f := rn.raft.RemoveServer(raft.ServerID(id), 0, timeout)
	return f.Error()
}

// State returns the FSM cluster state for reads.
func (rn *RaftNode) State() *ClusterState {
	return rn.fsm
}

// IsLeader returns true if this node is the current Raft leader.
func (rn *RaftNode) IsLeader() bool {
	return rn.raft.State() == raft.Leader
}

// LeaderAddress returns the address of the current leader.
func (rn *RaftNode) LeaderAddress() string {
	addr, _ := rn.raft.LeaderWithID()
	return string(addr)
}

// LeaderHTTPAddr returns the cluster management HTTP address of the current
// leader, resolved from the FSM. Returns "" if unknown.
func (rn *RaftNode) LeaderHTTPAddr() string {
	_, id := rn.raft.LeaderWithID()
	if node, ok := rn.fsm.GetNode(string(id)); ok {
		return node.HTTPAddr
	}
	return ""
}

// Shutdown gracefully stops the Raft node.
func (rn *RaftNode) Shutdown() error {
	f := rn.raft.Shutdown()
	return f.Error()
}
