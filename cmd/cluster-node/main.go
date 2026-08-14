// cluster-node runs a distributed YASE node that:
//   - Participates in Raft consensus (cluster topology)
//   - Consumes from Kafka and indexes documents into a local HybridEngine
//   - Serves ShardSearchService gRPC for distributed queries
//   - Exposes cluster management HTTP endpoints
//
// Usage:
//
//	# Bootstrap first node (shard 0)
//	cluster-node --bootstrap --node-id node-0 --raft-addr localhost:7000 \
//	  --shard-port 50053 --cluster-http-port 9100 --shard-id 0
//
//	# Join existing cluster (shard 1)
//	cluster-node --node-id node-1 --raft-addr localhost:7001 \
//	  --shard-port 50054 --cluster-http-port 9101 --shard-id 1 \
//	  --join http://localhost:9100
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/efathom/yase/internal/consensus"
	"github.com/efathom/yase/internal/glue"
	"github.com/efathom/yase/internal/shard"
	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/config"
	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/index"
	"github.com/efathom/yase/pkg/logging"
	"github.com/efathom/yase/pkg/metrics"
	"github.com/efathom/yase/pkg/pipeline"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"google.golang.org/grpc"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	nodeID := flag.String("node-id", "", "Unique node identifier")
	raftAddr := flag.String("raft-addr", "", "Raft bind address")
	shardPort := flag.Int("shard-port", 0, "gRPC port for ShardSearchService")
	clusterHTTPPort := flag.Int("cluster-http-port", 9100, "HTTP port for cluster management API")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap new cluster (first node only)")
	joinAddr := flag.String("join", "", "Cluster HTTP address of leader to join")
	shardID := flag.Int("shard-id", 0, "Shard ID this node owns")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		slog.Error("Invalid config", "error", err)
		os.Exit(1)
	}

	logging.Setup(logging.Config{Format: cfg.Logging.Format, Level: cfg.Logging.Level, Output: cfg.Logging.Output})
	defer logging.Close()

	if *nodeID != "" {
		cfg.Cluster.NodeID = *nodeID
	}
	if *raftAddr != "" {
		cfg.Cluster.RaftAddr = *raftAddr
	}
	if *shardPort != 0 {
		cfg.Cluster.ShardPort = *shardPort
	}
	if *bootstrap {
		cfg.Cluster.Bootstrap = true
	}
	if *joinAddr != "" {
		cfg.Cluster.JoinAddr = *joinAddr
	}

	sid := uint32(*shardID)

	slog.Info("Node starting",
		"node_id", cfg.Cluster.NodeID, "raft_addr", cfg.Cluster.RaftAddr,
		"shard_port", cfg.Cluster.ShardPort, "http_port", *clusterHTTPPort,
		"shard_id", sid, "bootstrap", cfg.Cluster.Bootstrap)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := metrics.ServeMetrics(cfg.Metrics.Addr); err != nil {
			slog.Error("Metrics error", "error", err)
		}
	}()

	// ── Hybrid engine for this shard ──
	indexPath := fmt.Sprintf("%s/shard-%d", cfg.Index.Path, sid)

	engine, err := index.NewHybridEngineWithConfig(indexPath, cfg.Index.ArenaSize, index.HNswConfig(cfg.HNSW, cfg.Embedder.Dimension), cfg.Index.CentroidRate)
	if err != nil {
		slog.Error("HybridEngine", "error", err)
		os.Exit(1)
	}
	defer engine.Close()
	slog.Info("Shard engine ready", "shard_id", sid, "path", indexPath, "dim", cfg.Embedder.Dimension)

	// ── Embedder ──
	emb, err := embedder.NewFromConfig(cfg.Embedder)
	if err != nil {
		slog.Error("failed to initialize embedder", "error", err)
		os.Exit(1)
	}

	// ── Kafka consumer → indexing pipeline ──
	wasmEngine := pipeline.NewWasmEngine(ctx)
	defer wasmEngine.Close(ctx)

	pipelineHandler := glue.NewPipelineHandler(emb, engine, wasmEngine, 0.75)

	// All cluster nodes share the same Kafka consumer group. Kafka auto-assigns
	// partitions across consumers in the group — each node processes a subset
	// of partitions. This gives true sharded ingestion without wasted work.
	daemon := glue.NewIndexingDaemon(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupID, pipelineHandler.Handle)

	go func() {
		slog.Info("Kafka consumer started", "node_id", cfg.Cluster.NodeID, "group", cfg.Kafka.GroupID, "topic", cfg.Kafka.Topic)
		if err := daemon.Start(ctx); err != nil {
			slog.Error("Kafka daemon error", "error", err)
		}
	}()

	// ── Raft ──
	raftNode, err := consensus.NewRaftNode(consensus.RaftNodeConfig{
		NodeID:    cfg.Cluster.NodeID,
		BindAddr:  cfg.Cluster.RaftAddr,
		DataDir:   cfg.Cluster.RaftDir,
		Bootstrap: cfg.Cluster.Bootstrap,
	})
	if err != nil {
		slog.Error("Raft init", "error", err)
		os.Exit(1)
	}

	// ── Cluster management HTTP ──
	clusterSvc := consensus.NewClusterService(raftNode)
	clusterMux := http.NewServeMux()
	clusterSvc.RegisterRoutes(clusterMux)

	clusterHTTPAddr := fmt.Sprintf(":%d", *clusterHTTPPort)
	go func() {
		slog.Info("Cluster HTTP API", "addr", clusterHTTPAddr)
		if err := http.ListenAndServe(clusterHTTPAddr, clusterMux); err != nil {
			slog.Error("Cluster HTTP error", "error", err)
		}
	}()

	// ── Join or bootstrap ──
	httpAddr := fmt.Sprintf("%s:%d", hostFromAddr(cfg.Cluster.RaftAddr), *clusterHTTPPort)
	if cfg.Cluster.JoinAddr != "" && !cfg.Cluster.Bootstrap {
		shardAddr := fmt.Sprintf("%s:%d", hostFromAddr(cfg.Cluster.RaftAddr), cfg.Cluster.ShardPort)
		if err := joinCluster(cfg.Cluster.JoinAddr, cfg.Cluster.NodeID, cfg.Cluster.RaftAddr, shardAddr, httpAddr); err != nil {
			slog.Error("Join cluster", "error", err)
			os.Exit(1)
		}
		slog.Info("Joined cluster", "addr", cfg.Cluster.JoinAddr)
	}

	if cfg.Cluster.Bootstrap {
		waitForLeader(raftNode, 10*time.Second)
		shardAddr := fmt.Sprintf("%s:%d", hostFromAddr(cfg.Cluster.RaftAddr), cfg.Cluster.ShardPort)
		raftNode.Apply(&consensus.Command{
			Type:         consensus.CmdRegisterNode,
			RegisterNode: &consensus.RegisterNode{ID: cfg.Cluster.NodeID, Address: shardAddr, HTTPAddr: httpAddr},
		}, 5*time.Second)
	}

	// ── ShardSearchService gRPC ──
	searchServer := shard.NewSearchServer()
	searchServer.RegisterEngine(sid, engine)

	authOpts, err := auth.ServerAuthTLS(cfg.Auth, cfg.TLS)
	if err != nil {
		slog.Error("failed to configure auth/TLS", "error", err)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer(authOpts...)
	ingestionv1.RegisterShardSearchServiceServer(grpcServer, searchServer)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Cluster.ShardPort))
	if err != nil {
		slog.Error("Listen", "error", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("ShardSearchService gRPC", "port", cfg.Cluster.ShardPort, "shard_id", sid)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC error", "error", err)
		}
	}()

	slog.Info("Node ready", "node_id", cfg.Cluster.NodeID, "shard_id", sid, "leader", raftNode.IsLeader())

	// Publish Raft leader state as a metric.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if raftNode.IsLeader() {
					metrics.RaftIsLeader.Set(1)
				} else {
					metrics.RaftIsLeader.Set(0)
				}
			}
		}
	}()

	// ── Shutdown ──
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("Shutting down")
	cancel()
	gracefulStop(grpcServer)
	daemon.Close()
	raftNode.Shutdown()
	slog.Info("Cluster node shut down")
}

// gracefulStop stops the gRPC server, falling back to a hard Stop after a
// bounded grace period so a held stream cannot block shutdown indefinitely.
func gracefulStop(srv *grpc.Server) {
	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		srv.Stop()
	}
}

func joinCluster(leaderHTTPAddr, nodeID, raftAddr, shardAddr, httpAddr string) error {
	body, _ := json.Marshal(map[string]string{
		"node_id": nodeID, "raft_addr": raftAddr, "shard_addr": shardAddr, "http_addr": httpAddr,
	})
	resp, err := http.Post(leaderHTTPAddr+"/cluster/join", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("POST /cluster/join: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		var result map[string]string
		json.NewDecoder(resp.Body).Decode(&result)
		return fmt.Errorf("join failed (%d): %s", resp.StatusCode, result["error"])
	}
	return nil
}

func waitForLeader(node *consensus.RaftNode, timeout time.Duration) {
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			slog.Error("Timeout waiting for Raft leader")
			os.Exit(1)
		default:
			if node.IsLeader() {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func hostFromAddr(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return "localhost"
	}
	return host
}
