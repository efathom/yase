package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/efathom/yase/internal/broker"
	"github.com/efathom/yase/internal/glue"
	"github.com/efathom/yase/internal/indexsvc"
	"github.com/efathom/yase/internal/shard"
	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/config"
	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/index"
	"github.com/efathom/yase/pkg/logging"
	"github.com/efathom/yase/pkg/metrics"
	"github.com/efathom/yase/pkg/pipeline"
	"github.com/efathom/yase/pkg/tracing"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"google.golang.org/grpc"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
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

	if shutdownTracing, err := tracing.Setup(context.Background(), "indexer", tracing.Config{
		Enabled: cfg.Tracing.Enabled, Exporter: cfg.Tracing.Exporter, Endpoint: cfg.Tracing.Endpoint, SampleRate: cfg.Tracing.SampleRate,
	}); err != nil {
		slog.Warn("tracing setup failed", "error", err)
	} else {
		defer shutdownTracing(context.Background())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start Prometheus metrics endpoint
	go func() {
		if err := metrics.ServeMetrics(cfg.Metrics.Addr); err != nil {
			slog.Error("Metrics server error", "error", err)
		}
	}()

	// Initialize embedder
	emb, err := embedder.NewFromConfig(cfg.Embedder)
	if err != nil {
		slog.Error("failed to initialize embedder", "error", err)
		os.Exit(1)
	}

	// Initialize hybrid engine
	engine, err := index.NewHybridEngineWithConfig(
		cfg.Index.Path,
		cfg.Index.ArenaSize,
		index.HNswConfig(cfg.HNSW, cfg.Embedder.Dimension),
		cfg.Index.CentroidRate,
	)
	if err != nil {
		slog.Error("Failed to create hybrid engine", "error", err)
		os.Exit(1)
	}
	defer engine.Close()

	// Initialize WASM plugin engine
	wasmEngine := pipeline.NewWasmEngine(ctx)
	defer wasmEngine.Close(ctx)

	// Build pipeline handler
	pipelineHandler := glue.NewPipelineHandler(emb, engine, wasmEngine, 0.75)

	// Start Kafka consumer with at-least-once semantics
	daemon := glue.NewIndexingDaemon(
		cfg.Kafka.Brokers,
		cfg.Kafka.Topic,
		cfg.Kafka.GroupID,
		pipelineHandler.Handle,
	)
	if cfg.Kafka.DLQTopic != "" {
		daemon.SetDLQ(broker.NewDLQWriter(cfg.Kafka.Brokers, cfg.Kafka.DLQTopic))
	}

	slog.Info("Indexer daemon starting, consuming from Kafka", "topic", cfg.Kafka.Topic)

	go func() {
		if err := daemon.Start(ctx); err != nil {
			slog.Error("Indexing daemon error", "error", err)
		}
	}()

	// Start gRPC IndexService so gateway can call Search remotely
	idxServer := indexsvc.NewServer(engine)
	authOpts, err := auth.ServerAuthTLS(cfg.Auth, cfg.TLS)
	if err != nil {
		slog.Error("failed to configure auth/TLS", "error", err)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer(authOpts...)
	ingestionv1.RegisterIndexServiceServer(grpcServer, idxServer)

	// Also register ShardSearchService so dist-gateway can query this node
	shardServer := shard.NewSearchServer()
	shardServer.RegisterEngine(0, engine)
	ingestionv1.RegisterShardSearchServiceServer(grpcServer, shardServer)

	grpcAddr := fmt.Sprintf(":%d", cfg.Index.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		slog.Error("Failed to listen", "addr", grpcAddr, "error", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("IndexService gRPC listening", "addr", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC serve error", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		slog.Info("Received signal, shutting down", "signal", sig)
		cancel()
	case <-ctx.Done():
	}

	gracefulStop(grpcServer)
	daemon.Close()
	slog.Info("Indexer shut down cleanly")
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
