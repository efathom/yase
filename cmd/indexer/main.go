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

	"github.com/efathom/yase/internal/app"
	"github.com/efathom/yase/internal/glue"
	"github.com/efathom/yase/internal/indexsvc"
	"github.com/efathom/yase/internal/shard"
	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/collection"
	"github.com/efathom/yase/pkg/config"
	"github.com/efathom/yase/pkg/logging"
	"github.com/efathom/yase/pkg/metrics"
	"github.com/efathom/yase/pkg/pipeline"
	"github.com/efathom/yase/pkg/reranker"
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
		defer func() { _ = shutdownTracing(context.Background()) }()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start Prometheus metrics endpoint
	go func() {
		if err := metrics.ServeMetrics(cfg.Metrics.Addr); err != nil {
			slog.Error("Metrics server error", "error", err)
		}
	}()

	// Collections: each collection owns a durable HybridEngine. Restoring
	// reconciles every engine with its text index (re-embedding anything not
	// covered by the last checkpoint) before the service reports ready.
	store, err := collection.NewFileStore(cfg.Collections.BasePath)
	if err != nil {
		slog.Error("Failed to create collection store", "error", err)
		os.Exit(1)
	}
	mgr := collection.NewManager(store, cfg.Collections.BasePath, cfg, slog.Default())
	if err := mgr.RestoreAll(ctx); err != nil {
		slog.Error("Failed to restore collections", "error", err)
		os.Exit(1)
	}
	defer mgr.Close()

	if ranker := reranker.NewFromConfig(cfg.Reranker); ranker != nil {
		mgr.SetReranker(ranker, cfg.Reranker.Candidates)
		slog.Info("Reranker enabled", "provider", cfg.Reranker.Provider, "model", cfg.Reranker.Model)
	}
	mgr.StartMaintenance(ctx, app.Maintenance(cfg.Index))

	// Initialize WASM plugin engine
	wasmEngine := pipeline.NewWasmEngine(ctx)
	defer wasmEngine.Close(ctx)

	// Kafka consumer → collection-aware pipeline. Offsets are committed only
	// after every engine has checkpointed, so a crash re-delivers instead of
	// losing documents.
	pipelineHandler := glue.NewCollectionPipelineHandler(mgr, wasmEngine, 0.75)
	daemon := app.NewIndexingDaemon(cfg.Kafka, pipelineHandler.Handle, mgr.Checkpoint)

	slog.Info("Indexer daemon starting, consuming from Kafka", "topic", cfg.Kafka.Topic)
	daemonDone := make(chan struct{})
	go func() {
		defer close(daemonDone)
		if err := daemon.Start(ctx); err != nil {
			slog.Error("Indexing daemon error", "error", err)
		}
	}()

	// gRPC: IndexService (search, documents, delete), CollectionService (CRUD)
	// and ShardSearchService (dist-gateway) over the same engines.
	authOpts, err := auth.ServerAuthTLS(cfg.Auth, cfg.TLS)
	if err != nil {
		slog.Error("failed to configure auth/TLS", "error", err)
		os.Exit(1)
	}
	if cfg.Server.MaxRecvMsgSize > 0 {
		authOpts = append(authOpts, grpc.MaxRecvMsgSize(cfg.Server.MaxRecvMsgSize))
	}
	grpcServer := grpc.NewServer(authOpts...)
	ingestionv1.RegisterIndexServiceServer(grpcServer, indexsvc.NewCollectionServer(mgr))
	ingestionv1.RegisterCollectionServiceServer(grpcServer, indexsvc.NewCollectionService(mgr))

	defaultEngine, err := mgr.GetEngine(collection.DefaultCollectionID)
	if err != nil {
		slog.Error("default collection missing", "error", err)
		os.Exit(1)
	}
	shardServer := shard.NewSearchServer()
	shardServer.RegisterEngine(0, defaultEngine)
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

	emb, err := mgr.GetEmbedder(collection.DefaultCollectionID)
	if err != nil {
		slog.Error("default embedder missing", "error", err)
		os.Exit(1)
	}
	health := app.ServeHealth(cfg.Server.HealthPort, func(ctx context.Context) error {
		_, err := emb.Embed(ctx, "ping")
		return err
	})

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("Received signal, shutting down", "signal", sig)

	// Stop serving, drain the consumer (final checkpoint + commit), then the
	// deferred mgr.Close writes final snapshots.
	gracefulStop(grpcServer)
	cancel()
	<-daemonDone
	if health != nil {
		_ = health.Close()
	}
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
