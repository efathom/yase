# Architecture

## Services

| Service | Port | Role |
|---------|------|------|
| **Gateway** | 8000 (HTTP) | Search, RAG, delete, suggest, health/ready |
| **Indexer** | 50052 (gRPC) | Kafka consumer → chunk → embed → hybrid index |
| **Ingestion** | 50051 (gRPC) | Batch/streaming document ingest → Kafka |
| **Crawler** | 9080 (HTTP) | Master-worker web crawler fleet |
| **Connector Manager** | 9300 (HTTP) | Scheduled data source syncs |
| **TEI Embedder** | 8888 | GTE-ModernBERT-base embeddings |
| **TEI Reranker** | 8081 | Cross-encoder reranking (optional) |

## Search Pipeline (5 Stages)

```
Query → Embed (TEI) →
  Stage 1: BM25 Pre-Filter (Bluge Roaring Bitmaps)
  Stage 2: HNSW Centroid Traversal (approximate nearest neighbors)
  Stage 3: Exact Cosine Rescoring (float32 vectors from arena)
  Stage 4: Reciprocal Rank Fusion (merge BM25 + semantic ranks)
  Stage 5: Cross-Encoder Reranking (optional TEI reranker)
→ Results
```

### How the hybrid index works

YASE co-locates two indexes behind a single document ID:

- **Bluge** — an inverted index (Vellum FSTs + Roaring Bitmaps) for BM25 scoring and exact-match metadata filtering.
- **HNSW-IF** — a custom HNSW vector graph where a subset of documents (centroids, every `centroid_rate`-th by default) live in RAM and the rest are stored in per-centroid inverted-file posting lists. Vectors reside in an off-heap mmap arena, invisible to the Go GC.

Metadata filters are applied **before** any vector math — the "allowed set" is computed by Bluge first, then only documents that pass the filters are rescored. This guarantees correct multi-tenant results even when the nearest vectors belong to a different tenant.

## Project Structure

```
cmd/
  local/          Combined indexer+gateway (development)
  gateway/        Stateless HTTP search API
  indexer/        Kafka consumer → hybrid indexer
  ingestion/      gRPC document ingestion
  crawler/        Master-worker web crawler
  connector/      Enterprise connector manager
  cluster-node/   Distributed cluster node (Raft + sharding)
  dist-gateway/   Distributed scatter-gather gateway
  yase-ctl/       CLI management tool

pkg/
  collection/     Collection manager (CRUD, engine lifecycle, cross-collection search, file store)
  index/          Hybrid engine (Bluge BM25 + HNSW-IF + RRF), facets, highlighting, suggest, spellcheck, persistence, TTL
  hnsw/           HNSW graph (insert, search, BBQ, persistence)
  memory/         Off-heap mmap arena (anonymous + file-backed)
  embedder/       Providers (TEI, Ollama, OpenAI, mock) + LRU cache
  reranker/       Cross-encoder reranking (TEI, mock)
  connector/      Connector framework (interface, registry, auth, HTTP base, scheduler, state, plugin)
  auth/           API auth (API key, JWT), HTTP middleware, gRPC interceptors, rate limiting, load shedding, TLS
  audit/          Structured audit event logging
  cache/          Search result LRU cache
  vector/         Cosine similarity, product quantization, binary quantization
  chunking/       Semantic + AST-aware text splitting
  crawler/        Bloom filter, rate limiter, DNS cache, HTML extractor
  storage/        S3 (real AWS SDK) + filesystem + cached store
  config/         YAML + env config with validation
  metrics/        Prometheus metrics (search, ingest, connector, arena)
  logging/        Structured logging (slog JSON/text)
  tracing/        OpenTelemetry distributed tracing
  routing/        Consistent hash ring
  pipeline/       WASM plugin engine (wazero)
  client/         gRPC connection pool with circuit breaker

client/           Go client SDK for the YASE API

internal/
  connector/      Connector implementations (confluence, jira, s3, postgres, mysql, gdrive, salesforce, sharepoint, slack, twitter)
  gateway/        HTTP handlers (search, RAG, delete, suggest, collection CRUD)
  glue/           Pipeline wiring (Kafka→index, connector→Kafka)
  consensus/      Raft consensus (BoltDB-backed)
  coordinator/    Crawler master/worker
  query/          Distributed scatter-gather coordinator
  shard/          Shard search service
  parser/         Document parser (HTML, PDF, plain text)
  builder/        Offline index builder
  server/         gRPC ingestion handler
  worker/         Bounded worker pool

api/              OpenAPI 3.1 specification
proto/v1/         Protocol Buffer definitions
deploy/           Docker Compose, Dockerfiles, Helm chart, Prometheus alerts
test/
  integration/    Testcontainer-based tests (Postgres, MinIO, MySQL, connector E2E)
  relevance/      Search quality metrics (Recall@K, Precision@K, MRR, nDCG, MAP)
  load/           Load testing scripts (vegeta, grpcurl)
```
