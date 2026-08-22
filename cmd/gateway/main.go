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

	"github.com/efathom/yase/internal/gateway"
	"github.com/efathom/yase/pkg/audit"
	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/cache"
	"github.com/efathom/yase/pkg/collection"
	"github.com/efathom/yase/pkg/config"
	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/logging"
	"github.com/efathom/yase/pkg/metrics"
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

	if shutdownTracing, err := tracing.Setup(context.Background(), "gateway", tracing.Config{
		Enabled: cfg.Tracing.Enabled, Exporter: cfg.Tracing.Exporter, Endpoint: cfg.Tracing.Endpoint, SampleRate: cfg.Tracing.SampleRate,
	}); err != nil {
		slog.Warn("tracing setup failed", "error", err)
	} else {
		defer func() { _ = shutdownTracing(context.Background()) }()
	}

	// Start Prometheus metrics endpoint on separate port
	go func() {
		if err := metrics.ServeMetrics(cfg.Metrics.Addr); err != nil {
			slog.Error("Metrics server error", "error", err)
		}
	}()

	// Initialize embedder (gateway still needs this to vectorize queries)
	emb, err := embedder.NewFromConfig(cfg.Embedder)
	if err != nil {
		slog.Error("failed to initialize embedder", "error", err)
		os.Exit(1)
	}

	// Connect to IndexService gRPC server (hosted by the indexer)
	indexAddr := fmt.Sprintf("%s:%d", cfg.Index.GRPCHost, cfg.Index.GRPCPort)
	dialOpts, err := auth.ClientDialOptions(cfg.Auth, cfg.TLS)
	if err != nil {
		slog.Error("failed to configure TLS", "error", err)
		os.Exit(1)
	}
	cc, err := grpc.NewClient(indexAddr, dialOpts...)
	if err != nil {
		slog.Error("Failed to connect to IndexService", "addr", indexAddr, "error", err)
		os.Exit(1)
	}
	defer cc.Close()

	searcher := &gateway.RemoteSearcher{
		Client: ingestionv1.NewIndexServiceClient(cc),
	}

	slog.Info("Gateway connected to IndexService", "addr", indexAddr)

	// Wire up gateway handler
	handler := gateway.NewHandler(searcher, emb)
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

	// Build the HTTP middleware chain. gateway.Chain owns the ordering — the
	// rate limiter has to run after auth to see the tenant.
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
			opts.Authenticator = authn
		}
	}

	if cfg.RateLimit.Enabled {
		opts.RateLimiter = auth.NewRateLimiter(auth.RateLimitConfig{
			Enabled:          cfg.RateLimit.Enabled,
			DefaultSearchQPS: cfg.RateLimit.DefaultSearchQPS,
			DefaultIngestRPS: cfg.RateLimit.DefaultIngestRPS,
		})
	}

	root := gateway.Chain(mux, opts)

	addr := fmt.Sprintf(":%d", cfg.Server.HTTPPort)
	server := &http.Server{
		Addr:              addr,
		Handler:           root,
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		slog.Info("Search gateway listening", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	slog.Info("Shutdown signal received")

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownGrace)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	}
	slog.Info("Gateway shut down cleanly")
}
