// cmd/local combines the indexer (Kafka consumer) and gateway (HTTP search API)
// into a single process so they can share the same Bluge writer lock.
// Usage: YASE_EMBEDDER_PROVIDER=mock bin/local
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/efathom/yase/internal/broker"
	"github.com/efathom/yase/internal/gateway"
	"github.com/efathom/yase/internal/glue"
	"github.com/efathom/yase/pkg/audit"
	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/cache"
	"github.com/efathom/yase/pkg/collection"
	"github.com/efathom/yase/pkg/config"
	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/logging"
	"github.com/efathom/yase/pkg/metrics"
	"github.com/efathom/yase/pkg/pipeline"
	"github.com/efathom/yase/pkg/reranker"
	"github.com/efathom/yase/pkg/tracing"
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

	if shutdownTracing, err := tracing.Setup(context.Background(), "local", tracing.Config{
		Enabled: cfg.Tracing.Enabled, Exporter: cfg.Tracing.Exporter, Endpoint: cfg.Tracing.Endpoint, SampleRate: cfg.Tracing.SampleRate,
	}); err != nil {
		slog.Warn("tracing setup failed", "error", err)
	} else {
		defer func() { _ = shutdownTracing(context.Background()) }()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Metrics
	go func() {
		if err := metrics.ServeMetrics(cfg.Metrics.Addr); err != nil {
			slog.Error("Metrics server error", "error", err)
		}
	}()

	// Embedder
	emb, err := embedder.NewFromConfig(cfg.Embedder)
	if err != nil {
		slog.Error("failed to initialize embedder", "error", err)
		os.Exit(1)
	}

	// Reranker (optional)
	ranker := reranker.NewFromConfig(cfg.Reranker)
	if ranker != nil {
		slog.Info("Reranker enabled", "provider", cfg.Reranker.Provider, "model", cfg.Reranker.Model)
	}

	// Collections subsystem: each collection owns its own HybridEngine
	// (Bluge + HNSW-IF + Arena) for physical isolation. The _default collection
	// is auto-created and serves the legacy /search path.
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

	defaultEngine, err := mgr.GetEngine(collection.DefaultCollectionID)
	if err != nil {
		slog.Error("Failed to get default collection engine", "error", err)
		os.Exit(1)
	}
	defaultEngine.Reranker = ranker
	defaultEngine.RerankerCandidates = cfg.Reranker.Candidates

	// WASM plugin engine
	wasmEngine := pipeline.NewWasmEngine(ctx)
	defer wasmEngine.Close(ctx)

	// Kafka consumer → collection-aware index pipeline (routes by _collection_id).
	pipelineHandler := glue.NewCollectionPipelineHandler(mgr, wasmEngine, 0.75)
	daemon := glue.NewIndexingDaemon(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupID, pipelineHandler.Handle)
	if cfg.Kafka.DLQTopic != "" {
		daemon.SetDLQ(broker.NewDLQWriter(cfg.Kafka.Brokers, cfg.Kafka.DLQTopic))
	}

	slog.Info("Indexer consuming from Kafka", "topic", cfg.Kafka.Topic)
	go func() {
		if err := daemon.Start(ctx); err != nil {
			slog.Error("Indexing daemon error", "error", err)
		}
	}()

	// HTTP gateway: legacy /search uses the _default engine; collections use the manager.
	handler := gateway.NewHandler(&gateway.LocalSearcher{Engine: defaultEngine}, emb)
	handler.WithCollectionSearcher(collection.NewSearcher(mgr))
	if cfg.Cache.Enabled {
		handler.SetCache(cache.NewSearchCache(collection.DefaultCollectionID, cfg.Cache.MaxSize, cfg.Cache.TTL))
	}
	if cfg.Audit.Enabled {
		auditLog := audit.NewLogger(cfg.Audit.Output)
		handler.SetAudit(auditLog)
		defer auditLog.Close()
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	gateway.NewCollectionHandler(mgr).RegisterRoutes(mux)

	var root http.Handler = gateway.Recover(mux)
	root = gateway.BodyLimit(10 << 20)(root) // 10 MB request body cap

	if cfg.Auth.Enabled {
		authn, err := auth.NewFromConfig(cfg.Auth)
		if err != nil {
			slog.Error("Failed to build authenticator", "error", err)
			os.Exit(1)
		}
		if authn != nil {
			handler.SetAuthEnabled(true)
			root = auth.HTTPMiddleware(authn)(root)
		}
	}

	if cfg.RateLimit.Enabled {
		rl := auth.NewRateLimiter(auth.RateLimitConfig{
			Enabled:          cfg.RateLimit.Enabled,
			DefaultSearchQPS: cfg.RateLimit.DefaultSearchQPS,
			DefaultIngestRPS: cfg.RateLimit.DefaultIngestRPS,
		})
		root = rl.HTTPMiddleware("search")(root)
	}

	root = auth.NewLoadShedder(100).HTTPMiddleware(root)

	addr := fmt.Sprintf(":%d", cfg.Server.HTTPPort)
	srv := &http.Server{
		Addr:              addr,
		Handler:           root,
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		slog.Info("Search gateway listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	slog.Info("Shutdown signal received")
	cancel()
	daemon.Close()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	_ = srv.Shutdown(shutCtx)
	slog.Info("Local server shut down cleanly")
}
