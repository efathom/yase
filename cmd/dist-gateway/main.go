// dist-gateway runs the distributed search gateway. It discovers cluster
// topology by polling a cluster node's HTTP API, fans out search queries
// to shard nodes via gRPC, and merges results using Reciprocal Rank Fusion.
//
// Usage:
//
//	dist-gateway --cluster-http http://localhost:9100 --http-port 8000
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/efathom/yase/internal/query"
	"github.com/efathom/yase/internal/shard"
	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/config"
	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/logging"
	"github.com/efathom/yase/pkg/metrics"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	clusterHTTP := flag.String("cluster-http", "", "Cluster node HTTP address for topology discovery")
	httpPort := flag.Int("http-port", 0, "HTTP port (overrides config)")
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

	if *httpPort != 0 {
		cfg.Server.HTTPPort = *httpPort
	}
	if *clusterHTTP == "" {
		*clusterHTTP = "http://localhost:9100"
	}

	go func() {
		if err := metrics.ServeMetrics(cfg.Metrics.Addr); err != nil {
			slog.Error("Metrics error", "error", err)
		}
	}()

	// Embedder
	emb, err := embedder.NewFromConfig(cfg.Embedder)
	if err != nil {
		slog.Error("failed to initialize embedder", "error", err)
		os.Exit(1)
	}

	// Remote shard searcher — routes queries to cluster nodes via gRPC
	dialOpt, err := auth.ClientDialOption(cfg.TLS)
	if err != nil {
		slog.Error("failed to configure TLS", "error", err)
		os.Exit(1)
	}
	searcher := shard.NewRemoteShardSearcher(dialOpt)
	defer searcher.Close()

	// Topology sync — polls cluster node for shard→node mapping
	topoSync := shard.NewTopologySync(*clusterHTTP, searcher, 2*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go topoSync.Start(ctx)

	// Wait for initial topology sync
	time.Sleep(500 * time.Millisecond)
	slog.Info("Initial topology version", "version", topoSync.Version())

	// Coordinator
	coordinator := &query.Coordinator{
		Resolver: topoSync,
		Searcher: searcher,
		Timeout:  2 * time.Second,
	}

	// HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("POST /search", func(w http.ResponseWriter, r *http.Request) {
		handleSearch(w, r, coordinator, emb)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /topology", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]any{
			"cluster_http": *clusterHTTP,
			"version":      topoSync.Version(),
		})
	})

	addr := fmt.Sprintf(":%d", cfg.Server.HTTPPort)
	server := &http.Server{
		Addr: addr, Handler: mux,
		ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second,
	}

	go func() {
		slog.Info("Distributed gateway", "addr", addr, "cluster", *clusterHTTP)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP", "error", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("Shutting down")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	_ = server.Shutdown(shutCtx)
	cancel()
	slog.Info("Distributed gateway shut down")
}

type searchRequest struct {
	Alias   string            `json:"alias"`
	Query   string            `json:"query"`
	Filters map[string]string `json:"filters,omitempty"`
	TopK    int               `json:"top_k"`
}

func handleSearch(w http.ResponseWriter, r *http.Request, coord *query.Coordinator, emb embedder.Embedder) {
	start := time.Now()

	var req searchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	if req.Query == "" {
		writeJSON(w, 400, map[string]string{"error": "query required"})
		return
	}
	if req.Alias == "" {
		req.Alias = "default"
	}
	if req.TopK <= 0 {
		req.TopK = 10
	}

	queryVec, err := emb.Embed(r.Context(), req.Query)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": "embedding failed"})
		return
	}

	resp, err := coord.Search(r.Context(), &query.SearchRequest{
		Alias: req.Alias, Query: req.Query, QueryVector: queryVec,
		Filters: req.Filters, TopK: req.TopK,
	})
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}

	elapsed := time.Since(start)
	metrics.SearchLatency.WithLabelValues("distributed").Observe(elapsed.Seconds())

	results := make([]map[string]any, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = map[string]any{
			"id": r.ID, "fused_score": r.FusedScore,
			"bm25_score": r.BM25Score, "semantic_score": r.SemanticScore,
		}
	}

	writeJSON(w, 200, map[string]any{
		"status": "success", "count": len(resp.Results),
		"duration_ms": elapsed.Milliseconds(), "shards": resp.ShardCount,
		"results": results,
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
