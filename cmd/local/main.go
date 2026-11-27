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

	"github.com/efathom/yase/internal/app"
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
	if ranker := reranker.NewFromConfig(cfg.Reranker); ranker != nil {
		mgr.SetReranker(ranker, cfg.Reranker.Candidates)
		slog.Info("Reranker enabled", "provider", cfg.Reranker.Provider, "model", cfg.Reranker.Model)
	}
	mgr.StartMaintenance(ctx, app.Maintenance(cfg.Index))

	// WASM plugin engine
	wasmEngine := pipeline.NewWasmEngine(ctx)
	defer wasmEngine.Close(ctx)

	// Kafka consumer → collection-aware index pipeline (routes by _collection_id).
	pipelineHandler := glue.NewCollectionPipelineHandler(mgr, wasmEngine, 0.75)
	daemon := app.NewIndexingDaemon(cfg.Kafka, pipelineHandler.Handle, mgr.Checkpoint)

	slog.Info("Indexer consuming from Kafka", "topic", cfg.Kafka.Topic)
	daemonDone := make(chan struct{})
	go func() {
		defer close(daemonDone)
		if err := daemon.Start(ctx); err != nil {
			slog.Error("Indexing daemon error", "error", err)
		}
	}()

	// HTTP gateway: legacy /search uses the _default engine; collections use the manager.
	handler := gateway.NewHandler(&gateway.LocalSearcher{Engine: defaultEngine}, emb)
	handler.WithCollectionSearcher(collection.NewSearcher(mgr))
	if err := handler.SetTrustedProxies(cfg.Server.TrustedProxies); err != nil {
		slog.Error("invalid server.trusted_proxies", "error", err)
		os.Exit(1)
	}
	if cfg.Cache.Enabled {
		handler.SetCache(cache.NewSearchCache(collection.DefaultCollectionID, cfg.Cache.MaxSize, cfg.Cache.TTL).
			SetGeneration(defaultEngine.Generation))
	}
	if cfg.Audit.Enabled {
		auditLog := audit.NewLogger(cfg.Audit.Output)
		handler.SetAudit(auditLog)
		defer auditLog.Close()
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	collectionHandler := gateway.NewCollectionHandler(mgr)

	opts := gateway.ChainOptions{
		BodyLimit:   10 << 20, // 10 MB request body cap
		LoadShedder: auth.NewLoadShedder(100),
		RateScope:   "search",
	}

	if cfg.Auth.Enabled {
		authn, err := auth.NewFromConfig(cfg.Auth)
		if err != nil {
			slog.Error("Failed to build authenticator", "error", err)
			os.Exit(1)
		}
		if authn != nil {
			handler.SetAuthEnabled(true)
			collectionHandler.SetAuthEnabled(true)
			opts.Authenticator = authn
		}
	}

	// Registered after SetAuthEnabled so the routes carry their scope checks.
	collectionHandler.RegisterRoutes(mux)

	if rl := app.RateLimiter(cfg); rl != nil {
		opts.RateLimiter = rl
	}

	root := gateway.Chain(mux, opts)

	addr := fmt.Sprintf(":%d", cfg.Server.HTTPPort)
	srv := app.NewHTTPServer(addr, root)

	go func() {
		slog.Info("Search gateway listening", "addr", addr, "tls", cfg.TLS.Enabled)
		if err := app.ListenAndServe(srv, cfg.TLS); err != nil {
			slog.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	slog.Info("Shutdown signal received")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	_ = srv.Shutdown(shutCtx)
	// Drain the consumer (final checkpoint + commit); deferred mgr.Close then
	// writes the final engine snapshots.
	cancel()
	<-daemonDone
	slog.Info("Local server shut down cleanly")
}
