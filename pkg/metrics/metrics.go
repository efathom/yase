package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Search Gateway metrics
var (
	SearchLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "yase",
			Subsystem: "gateway",
			Name:      "search_duration_seconds",
			Help:      "Histogram of search request latency in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		},
		[]string{"search_type"},
	)

	SearchResultsCount = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "yase",
			Subsystem: "gateway",
			Name:      "search_results_count",
			Help:      "Number of results returned per search query.",
			Buckets:   []float64{0, 1, 5, 10, 25, 50, 100},
		},
		[]string{"search_type"},
	)
)

// Ingestion metrics
var (
	DocumentsIngested = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "yase",
			Subsystem: "ingestion",
			Name:      "documents_total",
			Help:      "Total number of documents ingested.",
		},
		[]string{"status"},
	)

	IngestLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "yase",
			Subsystem: "ingestion",
			Name:      "ingest_duration_seconds",
			Help:      "Time to ingest a single document into the hybrid engine.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0},
		},
	)

	WorkerPoolQueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "yase",
			Subsystem: "ingestion",
			Name:      "queue_depth",
			Help:      "Current number of jobs in the worker pool queue.",
		},
	)
)

// Kafka consumer metrics
var (
	KafkaMessagesConsumed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "yase",
			Subsystem: "kafka",
			Name:      "messages_consumed_total",
			Help:      "Total Kafka messages consumed.",
		},
		[]string{"topic", "status"},
	)

	KafkaConsumerLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "yase",
			Subsystem: "kafka",
			Name:      "consumer_lag",
			Help:      "Consumer lag per partition.",
		},
		[]string{"topic", "partition"},
	)
)

// Crawler metrics
var (
	CrawlerURLsFetched = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "yase",
			Subsystem: "crawler",
			Name:      "urls_fetched_total",
			Help:      "Total URLs fetched by the crawler fleet.",
		},
		[]string{"method", "status_code", "domain"},
	)

	CrawlerActiveGoroutines = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "yase",
			Subsystem: "crawler",
			Name:      "active_goroutines",
			Help:      "Number of currently active crawler goroutines.",
		},
	)

	CrawlerFetchDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "yase",
			Subsystem: "crawler",
			Name:      "fetch_duration_seconds",
			Help:      "HTTP fetch duration per request.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 30.0},
		},
		[]string{"domain"},
	)

	CrawlerRateLimitErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "yase",
			Subsystem: "crawler",
			Name:      "rate_limit_redis_errors_total",
			Help:      "Number of Redis errors encountered by the global rate limiter (fail-closed).",
		},
	)
)

// Cluster/Raft metrics
var (
	RaftIsLeader = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "yase",
			Subsystem: "raft",
			Name:      "is_leader",
			Help:      "1 if this node is the current Raft leader, 0 otherwise.",
		},
	)
)

// HNSW and memory metrics
var (
	HNSWNodeCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "yase",
			Subsystem: "hnsw",
			Name:      "nodes_total",
			Help:      "Total number of nodes in the HNSW graph.",
		},
	)

	HNSWSearchLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "yase",
			Subsystem: "hnsw",
			Name:      "search_duration_seconds",
			Help:      "HNSW graph search latency.",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
	)

	ArenaUsedBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "yase",
			Subsystem: "memory",
			Name:      "arena_used_bytes",
			Help:      "Bytes currently allocated in the mmap arena.",
		},
	)

	ArenaCapacityBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "yase",
			Subsystem: "memory",
			Name:      "arena_capacity_bytes",
			Help:      "Total capacity of the mmap arena in bytes.",
		},
	)
)

// ServeMetrics starts the Prometheus /metrics HTTP endpoint on a separate port.
func ServeMetrics(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadTimeout:       5 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	return server.ListenAndServe()
}
