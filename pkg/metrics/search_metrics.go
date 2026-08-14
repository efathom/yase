package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Search pipeline stage metrics
var (
	SearchStageLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "yase_search_stage_duration_seconds",
		Help:    "Duration of individual search pipeline stages",
		Buckets: prometheus.DefBuckets,
	}, []string{"stage"}) // stages: bm25_prefilter, hnsw_traversal, exact_rescore, rrf_fusion, rerank

	SearchErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "yase_search_errors_total",
		Help: "Total search errors by endpoint and type",
	}, []string{"endpoint", "error_type"})

	EmbedderLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "yase_embedder_latency_seconds",
		Help:    "Embedder API call latency",
		Buckets: prometheus.DefBuckets,
	}, []string{"provider"})

	EmbedderCacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "yase_embedder_cache_hits_total",
		Help: "Embedder cache hits",
	})

	EmbedderCacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "yase_embedder_cache_misses_total",
		Help: "Embedder cache misses",
	})

	RerankerLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "yase_reranker_latency_seconds",
		Help:    "Reranker call latency",
		Buckets: prometheus.DefBuckets,
	})

	IngestDocumentsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "yase_ingest_documents_total",
		Help: "Total documents ingested",
	}, []string{"tenant"})

	IngestErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "yase_ingest_errors_total",
		Help: "Total ingestion errors",
	}, []string{"tenant"})

	DeleteDocumentsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "yase_delete_documents_total",
		Help: "Total documents deleted",
	})

	// ArenaUsedBytes and ArenaCapacityBytes are defined in metrics.go

	SearchCacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "yase_search_cache_hits_total",
		Help: "Search result cache hits",
	})

	SearchCacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "yase_search_cache_misses_total",
		Help: "Search result cache misses",
	})
)
