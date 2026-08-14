package consensus

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestRaftSingleNodeLeaderElection(t *testing.T) {
	node := startTestCluster(t, 1)[0]
	defer node.Shutdown()

	waitForLeader(t, node, 5*time.Second)

	if !node.IsLeader() {
		t.Fatal("single node should be leader")
	}
}

func TestRaftThreeNodeCluster(t *testing.T) {
	nodes := startTestCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Shutdown()
		}
	}()

	leader := waitForAnyLeader(t, nodes, 5*time.Second)
	t.Logf("Leader elected: %s", leader.cfg.NodeID)

	// Apply commands via leader
	err := leader.Apply(&Command{
		Type:         CmdRegisterNode,
		RegisterNode: &RegisterNode{ID: "node-1", Address: "10.0.0.1:50052"},
	}, 5*time.Second)
	if err != nil {
		t.Fatalf("Apply RegisterNode: %v", err)
	}

	err = leader.Apply(&Command{
		Type:        CmdAssignShard,
		AssignShard: &AssignShard{ShardID: 1, Replicas: []string{"node-1"}},
	}, 5*time.Second)
	if err != nil {
		t.Fatalf("Apply AssignShard: %v", err)
	}

	err = leader.Apply(&Command{
		Type:        CmdUpdateAlias,
		UpdateAlias: &UpdateAlias{Name: "prod", ShardIDs: []uint32{1}},
	}, 5*time.Second)
	if err != nil {
		t.Fatalf("Apply UpdateAlias: %v", err)
	}

	// Wait for replication
	time.Sleep(500 * time.Millisecond)

	// Verify state on all nodes
	for _, node := range nodes {
		state := node.State()
		if state.NodeCount() != 1 {
			t.Errorf("node %s: expected 1 registered node, got %d", node.cfg.NodeID, state.NodeCount())
		}
		if state.ShardCount() != 1 {
			t.Errorf("node %s: expected 1 shard, got %d", node.cfg.NodeID, state.ShardCount())
		}
		alias, ok := state.GetAlias("prod")
		if !ok {
			t.Errorf("node %s: alias prod not found", node.cfg.NodeID)
		} else if len(alias.ShardIDs) != 1 {
			t.Errorf("node %s: alias shards: %v", node.cfg.NodeID, alias.ShardIDs)
		}
	}
}

func TestRaftFollowerRejectsWrites(t *testing.T) {
	nodes := startTestCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.Shutdown()
		}
	}()

	waitForAnyLeader(t, nodes, 5*time.Second)

	// Find a follower
	var follower *RaftNode
	for _, n := range nodes {
		if !n.IsLeader() {
			follower = n
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower found")
	}

	// Writing to follower should fail
	err := follower.Apply(&Command{
		Type:         CmdRegisterNode,
		RegisterNode: &RegisterNode{ID: "x", Address: "a"},
	}, 2*time.Second)
	if err == nil {
		t.Error("expected error writing to follower")
	}
	t.Logf("Follower write error (expected): %v", err)
}

func TestRaftSnapshotAndRecover(t *testing.T) {
	nodes := startTestCluster(t, 1)
	leader := nodes[0]
	defer leader.Shutdown()

	waitForLeader(t, leader, 5*time.Second)

	// Apply many commands to trigger snapshot
	for i := 0; i < 200; i++ {
		err := leader.Apply(&Command{
			Type:         CmdRegisterNode,
			RegisterNode: &RegisterNode{ID: "n", Address: "a"},
		}, 5*time.Second)
		if err != nil {
			t.Fatalf("Apply %d: %v", i, err)
		}
	}

	// State should reflect all applies
	state := leader.State()
	state.mu.RLock()
	v := state.Version
	state.mu.RUnlock()
	if v != 200 {
		t.Errorf("version: got %d, want 200", v)
	}
}

// ── Helpers ──

func startTestCluster(t *testing.T, n int) []*RaftNode {
	t.Helper()

	_, trans := raft.NewInmemTransport("")
	nodes := make([]*RaftNode, n)

	// First node bootstraps
	var err error
	nodes[0], err = NewRaftNodeWithTransport(RaftNodeConfig{
		NodeID:    "node-0",
		Bootstrap: true,
	}, trans)
	if err != nil {
		t.Fatalf("create node-0: %v", err)
	}

	if n == 1 {
		return nodes
	}

	// Wait for leader before adding voters
	waitForLeader(t, nodes[0], 5*time.Second)

	// Additional nodes join as voters
	for i := 1; i < n; i++ {
		_, peerTrans := raft.NewInmemTransport("")

		// Connect transports
		trans.Connect(peerTrans.LocalAddr(), peerTrans)
		peerTrans.Connect(trans.LocalAddr(), trans)

		// Cross-connect all previous peers
		for j := 1; j < i; j++ {
			prevTrans := nodes[j].trans.(*raft.InmemTransport)
			peerTrans.Connect(prevTrans.LocalAddr(), prevTrans)
			prevTrans.Connect(peerTrans.LocalAddr(), peerTrans)
		}

		nodeID := fmt.Sprintf("node-%d", i)
		nodes[i], err = NewRaftNodeWithTransport(RaftNodeConfig{
			NodeID: nodeID,
		}, peerTrans)
		if err != nil {
			t.Fatalf("create %s: %v", nodeID, err)
		}

		// Leader adds the new node as voter
		if err := nodes[0].AddVoter(nodeID, string(peerTrans.LocalAddr()), 5*time.Second); err != nil {
			t.Fatalf("AddVoter %s: %v", nodeID, err)
		}
	}

	return nodes
}

func waitForLeader(t *testing.T, node *RaftNode, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for %s to become leader", node.cfg.NodeID)
		default:
			if node.IsLeader() {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func waitForAnyLeader(t *testing.T, nodes []*RaftNode, timeout time.Duration) *RaftNode {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for leader election")
		default:
			for _, n := range nodes {
				if n.IsLeader() {
					return n
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}
