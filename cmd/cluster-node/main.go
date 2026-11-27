// cluster-node runs a distributed YASE node that:
//   - Participates in Raft consensus (cluster topology)
//   - Consumes the document log from Kafka and indexes the documents whose URL
//     hashes to its shard into a durable local HybridEngine
//   - Serves ShardSearchService gRPC for distributed queries
//   - Exposes cluster management HTTP endpoints (token-protected)
//
// Replication: run several nodes with the same --shard-id. Each consumes the
// whole topic under its own consumer group and keeps only its shard, so every
// replica builds the full shard from the log independently; the distributed
// gateway fails over between them. Use a log-compacted topic (keyed by URL) so
// a replacement node can rebuild from Kafka.
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
	"path/filepath"
	"syscall"
	"time"

	"github.com/efathom/yase/internal/app"
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
	"github.com/efathom/yase/pkg/reranker"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"google.golang.org/grpc"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	nodeID := flag.String("node-id", "", "Unique node identifier")
	raftAddr := flag.String("raft-addr", "", "Raft bind address")
	shardPort := flag.Int("shard-port", 0, "gRPC port for ShardSearchService")
	clusterHTTPPort := flag.Int("cluster-http-port", 0, "HTTP port for cluster management API (overrides cluster.http_addr port)")
	bootstrap := flag.Bool("bootstrap", false, "Bootstrap new cluster (first node only)")
	joinAddr := flag.String("join", "", "Cluster HTTP address of leader to join")
	shardID := flag.Int("shard-id", 0, "Shard ID this node serves")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

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
	if *clusterHTTPPort != 0 {
		host, _, err := net.SplitHostPort(cfg.Cluster.HTTPAddr)
		if err != nil {
			host = "127.0.0.1"
		}
		cfg.Cluster.HTTPAddr = net.JoinHostPort(host, fmt.Sprint(*clusterHTTPPort))
	}
	if err := cfg.Validate(); err != nil {
		slog.Error("Invalid config", "error", err)
		os.Exit(1)
	}

	logging.Setup(logging.Config{Format: cfg.Logging.Format, Level: cfg.Logging.Level, Output: cfg.Logging.Output})
	defer logging.Close()

	sid := uint32(*shardID)
	numShards := cfg.Cluster.NumShards
	if int(sid) >= numShards {
		slog.Error("shard-id out of range", "shard_id", sid, "num_shards", numShards)
		os.Exit(1)
	}

	slog.Info("Node starting",
		"node_id", cfg.Cluster.NodeID, "raft_addr", cfg.Cluster.RaftAddr,
		"shard_port", cfg.Cluster.ShardPort, "cluster_http", cfg.Cluster.HTTPAddr,
		"shard_id", sid, "num_shards", numShards, "bootstrap", cfg.Cluster.Bootstrap)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := metrics.ServeMetrics(cfg.Metrics.Addr); err != nil {
			slog.Error("Metrics error", "error", err)
		}
	}()

	// ── Embedder ──
	emb, err := embedder.NewFromConfig(cfg.Embedder)
	if err != nil {
		slog.Error("failed to initialize embedder", "error", err)
		os.Exit(1)
	}

	// ── Durable hybrid engine for this shard ──
	engineDir := filepath.Join(cfg.Index.Path, fmt.Sprintf("shard-%d", sid))
	engine, err := index.OpenHybridEngine(index.EngineConfig{
		Dir:          engineDir,
		ArenaSize:    cfg.Index.ArenaSize,
		HNSW:         index.HNswConfig(cfg.HNSW, cfg.Embedder.Dimension),
		CentroidRate: cfg.Index.CentroidRate,
	})
	if err != nil {
		slog.Error("HybridEngine", "error", err)
		os.Exit(1)
	}
	defer engine.Close()
	if st, err := engine.Reconcile(ctx, emb.EmbedBatch, 64); err != nil {
		slog.Error("reconcile failed", "error", err)
	} else {
		slog.Info("Shard engine ready", "shard_id", sid, "dir", engineDir, "documents", st.Documents,
			"re_embedded", st.ReEmbedded, "dropped_vectors", st.DroppedVectors)
	}
	if ranker := reranker.NewFromConfig(cfg.Reranker); ranker != nil {
		engine.Reranker = ranker
		engine.RerankerCandidates = cfg.Reranker.Candidates
	}
	maint := app.Maintenance(cfg.Index)
	go engineMaintenance(ctx, engine, maint.CheckpointInterval, maint.CompactDeadRatio)
	engine.StartTTLSweeper(ctx, maint.TTL)

	// ── Kafka consumer → indexing pipeline ──
	wasmEngine := pipeline.NewWasmEngine(ctx)
	defer wasmEngine.Close(ctx)

	pipelineHandler := glue.NewPipelineHandler(emb, engine, wasmEngine, 0.75)
	pipelineHandler.Accept = func(r *ingestionv1.CrawlRecord) bool {
		return shard.ForKey(r.Url, numShards) == sid
	}

	// Each node has its own consumer group and reads the whole log, keeping
	// its shard. Replicas of a shard therefore converge independently, and a
	// Kafka rebalance never moves documents between shards.
	kcfg := cfg.Kafka
	kcfg.GroupID = fmt.Sprintf("%s-shard-%d-%s", cfg.Kafka.GroupID, sid, cfg.Cluster.NodeID)
	daemon := app.NewIndexingDaemon(kcfg, pipelineHandler.Handle, func(context.Context) error { return engine.Checkpoint() })
	daemonDone := make(chan struct{})
	go func() {
		defer close(daemonDone)
		slog.Info("Kafka consumer started", "node_id", cfg.Cluster.NodeID, "group", kcfg.GroupID, "topic", kcfg.Topic)
		if err := daemon.Start(ctx); err != nil {
			slog.Error("Kafka daemon error", "error", err)
		}
	}()

	// ── Raft ──
	raftCfg := consensus.RaftNodeConfig{
		NodeID:    cfg.Cluster.NodeID,
		BindAddr:  cfg.Cluster.RaftAddr,
		DataDir:   cfg.Cluster.RaftDir,
		Bootstrap: cfg.Cluster.Bootstrap,
	}
	if cfg.TLS.Enabled {
		tc := auth.TLSConfig{Enabled: true, CertPath: cfg.TLS.CertPath, KeyPath: cfg.TLS.KeyPath, CAPath: cfg.TLS.CAPath, ClientAuth: true}
		if raftCfg.ServerTLS, err = auth.LoadTLSConfig(tc); err == nil {
			raftCfg.ClientTLS, err = auth.LoadClientTLSConfig(tc)
		}
		if err != nil {
			slog.Error("Raft TLS", "error", err)
			os.Exit(1)
		}
	} else {
		slog.Warn("Raft traffic is unencrypted (tls.enabled=false); run only on a trusted network")
	}
	raftNode, err := consensus.NewRaftNode(raftCfg)
	if err != nil {
		slog.Error("Raft init", "error", err)
		os.Exit(1)
	}

	// ── Cluster management HTTP (loopback unless a join token is configured) ──
	clusterSvc := consensus.NewClusterService(raftNode).SetToken(cfg.Cluster.JoinToken)
	clusterMux := http.NewServeMux()
	clusterSvc.RegisterRoutes(clusterMux)
	clusterSrv := app.NewHTTPServer(cfg.Cluster.HTTPAddr, clusterMux)
	go func() {
		slog.Info("Cluster HTTP API", "addr", cfg.Cluster.HTTPAddr)
		if err := app.ListenAndServe(clusterSrv, cfg.TLS); err != nil {
			slog.Error("Cluster HTTP error", "error", err)
		}
	}()

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

	// ── Join or bootstrap, then announce this node as a replica of its shard ──
	advertiseHost := hostFromAddr(cfg.Cluster.RaftAddr)
	_, httpPort, _ := net.SplitHostPort(cfg.Cluster.HTTPAddr)
	scheme := "http"
	if cfg.TLS.Enabled {
		scheme = "https"
	}
	httpAddr := fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(advertiseHost, httpPort))
	shardAddr := net.JoinHostPort(advertiseHost, fmt.Sprint(cfg.Cluster.ShardPort))
	mgmt := &clusterClient{token: cfg.Cluster.JoinToken}

	if cfg.Cluster.JoinAddr != "" && !cfg.Cluster.Bootstrap {
		if err := mgmt.post(cfg.Cluster.JoinAddr+"/cluster/join", map[string]any{
			"node_id": cfg.Cluster.NodeID, "raft_addr": cfg.Cluster.RaftAddr, "shard_addr": shardAddr, "http_addr": httpAddr,
		}); err != nil {
			slog.Error("Join cluster", "error", err)
			os.Exit(1)
		}
		if err := mgmt.post(cfg.Cluster.JoinAddr+"/cluster/shard/replica", map[string]any{
			"shard_id": sid, "node_id": cfg.Cluster.NodeID,
		}); err != nil {
			slog.Error("Register shard replica", "error", err)
			os.Exit(1)
		}
		slog.Info("Joined cluster", "addr", cfg.Cluster.JoinAddr)
	}
	if cfg.Cluster.Bootstrap {
		waitForLeader(raftNode, 10*time.Second)
		_ = raftNode.Apply(&consensus.Command{
			Type:         consensus.CmdRegisterNode,
			RegisterNode: &consensus.RegisterNode{ID: cfg.Cluster.NodeID, Address: shardAddr, HTTPAddr: httpAddr},
		}, 5*time.Second)
		_ = raftNode.Apply(&consensus.Command{
			Type:       consensus.CmdAddReplica,
			AddReplica: &consensus.AddReplica{ShardID: sid, NodeID: cfg.Cluster.NodeID},
		}, 5*time.Second)
	}

	health := app.ServeHealth(cfg.Server.HealthPort, func(ctx context.Context) error {
		if raftNode.LeaderAddress() == "" {
			return fmt.Errorf("no raft leader")
		}
		_, err := emb.Embed(ctx, "ping")
		return err
	})

	slog.Info("Node ready", "node_id", cfg.Cluster.NodeID, "shard_id", sid, "leader", raftNode.IsLeader())

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
	gracefulStop(grpcServer)
	cancel()
	<-daemonDone // final checkpoint + offset commit
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = clusterSrv.Shutdown(shutCtx)
	if health != nil {
		_ = health.Shutdown(shutCtx)
	}
	shutCancel()
	_ = raftNode.Shutdown()
	slog.Info("Cluster node shut down")
}

// engineMaintenance periodically checkpoints and compacts the shard engine.
func engineMaintenance(ctx context.Context, engine *index.HybridEngine, every time.Duration, ratio float64) {
	if every <= 0 {
		every = time.Minute
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := engine.Checkpoint(); err != nil {
				slog.Error("checkpoint failed", "error", err)
			}
			if ran, err := engine.MaybeCompact(ratio); err != nil {
				slog.Error("compaction failed", "error", err)
			} else if ran {
				slog.Info("shard compacted")
			}
		}
	}
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

// clusterClient calls other nodes' management APIs with the cluster token.
type clusterClient struct {
	token string
}

func (c *clusterClient) post(url string, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set(consensus.TokenHeader, c.token)
	}
	hc := &http.Client{Timeout: 15 * time.Second}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var result map[string]string
		_ = json.NewDecoder(resp.Body).Decode(&result)
		return fmt.Errorf("POST %s failed (%d): %s", url, resp.StatusCode, result["error"])
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
