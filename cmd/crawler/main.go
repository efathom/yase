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

	"github.com/efathom/yase/internal/coordinator"
	"github.com/efathom/yase/internal/glue"
	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/client"
	"github.com/efathom/yase/pkg/config"
	"github.com/efathom/yase/pkg/crawler"
	"github.com/efathom/yase/pkg/logging"
	"github.com/efathom/yase/pkg/metrics"
	"github.com/redis/go-redis/v9"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	mode := flag.String("mode", "worker", "Run mode: master or worker")
	workerID := flag.String("id", "worker-001", "Unique node identifier")
	masterURL := flag.String("master", "http://localhost:9080", "Master node URL")
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start Prometheus metrics endpoint
	go func() {
		if err := metrics.ServeMetrics(cfg.Metrics.Addr); err != nil {
			slog.Error("Metrics server error", "error", err)
		}
	}()

	// Redis client for rate limiting and bloom filters
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
		PoolSize: cfg.Redis.PoolSize,
	})
	defer rdb.Close()

	slog.Info("Crawler node starting", "worker_id", *workerID, "mode", *mode)

	switch *mode {
	case "master":
		runMaster(ctx, cfg, rdb)
	case "worker":
		runWorker(ctx, cfg, rdb, *workerID, *masterURL)
	default:
		slog.Error("Unknown mode", "mode", *mode)
		os.Exit(1)
	}

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		slog.Info("Received signal, shutting down", "signal", sig)
		cancel()
	case <-ctx.Done():
	}

	slog.Info("Crawler shut down cleanly")
}

func buildDomainFilter(cfg *config.Config) *coordinator.DomainFilter {
	f := &coordinator.DomainFilter{
		AllowedDomains: make(map[string]bool),
		BlockedDomains: make(map[string]bool),
	}

	// Load from inline config
	for _, d := range cfg.Crawler.AllowedDomains {
		f.AllowedDomains[d] = true
	}
	for _, d := range cfg.Crawler.BlockedDomains {
		f.BlockedDomains[d] = true
	}

	// Load from files (merged with inline config)
	if cfg.Crawler.AllowedDomainsFile != "" {
		domains, err := coordinator.LoadDomainsFromFile(cfg.Crawler.AllowedDomainsFile)
		if err != nil {
			slog.Warn("Failed to load allowed domains", "error", err)
		} else {
			for _, d := range domains {
				f.AllowedDomains[d] = true
			}
			slog.Info("Loaded allowed domains", "count", len(domains), "file", cfg.Crawler.AllowedDomainsFile)
		}
	}
	if cfg.Crawler.BlockedDomainsFile != "" {
		domains, err := coordinator.LoadDomainsFromFile(cfg.Crawler.BlockedDomainsFile)
		if err != nil {
			slog.Warn("Failed to load blocked domains", "error", err)
		} else {
			for _, d := range domains {
				f.BlockedDomains[d] = true
			}
			slog.Info("Loaded blocked domains", "count", len(domains), "file", cfg.Crawler.BlockedDomainsFile)
		}
	}

	// Store file paths for hot-reload
	f.SetFiles(cfg.Crawler.AllowedDomainsFile, cfg.Crawler.BlockedDomainsFile)

	if len(f.AllowedDomains) == 0 && len(f.BlockedDomains) == 0 {
		return nil
	}
	return f
}

func runMaster(ctx context.Context, cfg *config.Config, rdb *redis.Client) {
	bloom := crawler.NewBloomFilter(rdb, "crawler:frontier")
	frontierQueue := make(chan string, 10000)

	master := coordinator.NewMasterScheduler(bloom, frontierQueue, buildDomainFilter(cfg))
	master.MaxPages = cfg.Crawler.MaxPages

	// Start HTTP heartbeat + assignment + seed server
	mux := http.NewServeMux()
	mux.HandleFunc("/heartbeat", master.HeartbeatHandler)
	mux.HandleFunc("/assign", master.AssignHandler)
	mux.HandleFunc("/seed", master.SeedHandler)
	mux.HandleFunc("/discover", master.DiscoverHandler)

	addr := ":9080"
	go func() {
		slog.Info("Master scheduler HTTP API", "addr", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Error("Master HTTP error", "error", err)
		}
	}()

	// Start dead-worker reaper
	go master.ReaperDaemon(ctx)
}

func runWorker(ctx context.Context, cfg *config.Config, rdb *redis.Client, workerID, masterURL string) {
	rl := crawler.NewGlobalRateLimiter(rdb)

	jobs := make(chan string, 100)
	results := make(chan coordinator.CrawlResult, 100)

	worker := coordinator.NewWorkerNode(workerID, masterURL, rl, jobs, results)
	worker.RateLimit = cfg.Crawler.RatePerDomain
	worker.RateWindow = cfg.Crawler.RateWindow
	worker.MaxRetries = cfg.Crawler.MaxRetries
	worker.Heartbeat = cfg.Crawler.HeartbeatInterval
	defer worker.Close()
	worker.Start(ctx, cfg.Crawler.MaxWorkers)

	// Stream results to ingestion service
	ingestionAddr := fmt.Sprintf("%s:%d", cfg.Server.GRPCHost, cfg.Server.GRPCPort)
	dialOpt, err := auth.ClientDialOption(cfg.TLS)
	if err != nil {
		slog.Error("failed to configure TLS", "error", err)
		return
	}
	pool, err := client.NewConnectionPool(ingestionAddr, 4, dialOpt)
	if err != nil {
		slog.Warn("could not connect to ingestion service", "error", err)
		return
	}

	go func() {
		if err := glue.StreamToIngestion(ctx, results, pool); err != nil {
			slog.Error("Streamer error", "error", err)
		}
		pool.Close()
	}()
}
