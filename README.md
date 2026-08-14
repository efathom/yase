# YASE — Yet Another Search Engine

A production-grade, distributed hybrid search engine in Go, purpose-built for **retrieval-augmented generation (RAG) and agent-native search**. YASE fuses lexical and semantic retrieval — BM25 full-text search (Bluge) and HNSW vector similarity — into a single pipeline using Reciprocal Rank Fusion and an optional cross-encoder reranker, then serves results as *citable, attributed evidence* for LLMs.

## Key Features

- **Hybrid search** — BM25 + HNSW-IF vector search, fused via Reciprocal Rank Fusion with an optional cross-encoder reranker.
- **Pre-filtering, not post-filtering** — metadata and tenant filters run *before* vector math via Bluge Roaring Bitmaps, so restricted users never see documents they can't access (a common failure mode of pure vector databases).
- **Agentic RAG API** — `/v1/rag` returns inline `[N]` citations, source URLs, chunk text, and per-result scores, ready to inject straight into an LLM prompt.
- **Semantic + AST-aware chunking** — cosine-similarity valley detection groups prose by topic; tree-sitter keeps code units (functions, classes) intact.
- **Local, self-hosted embeddings** — TEI with GTE-ModernBERT-base (768-dim, MTEB 64.38) keeps data in-house at zero per-token cost. Ollama and OpenAI providers included.
- **Collections** — physically isolated search spaces with per-collection embedders and config, plus cross-collection fan-out merged via RRF.
- **10 enterprise connectors** — Confluence, Jira, S3, PostgreSQL, MySQL, Google Drive, Salesforce, SharePoint, Slack, Twitter/X, plus a go-plugin system for custom sources.
- **Distributed scale-out** — Raft consensus, consistent hashing, and scatter-gather across shards.
- **Production-ready** — API-key/JWT auth, TLS, tenant isolation, rate limiting, audit logging, JSON logging, OpenTelemetry tracing, Prometheus metrics, and a Helm chart.

## Why YASE for Agentic RAG

Retrieval for LLM agents differs from classic search: agents need **correctly-scoped, citable evidence with full provenance** — not just a ranked list of links. YASE is built around that contract.

| Agent requirement | How YASE delivers it |
|---|---|
| **Grounded answers** | `/v1/rag` returns citations with `doc_id`, `source_url`, `chunk_text`, and `[N]` markers to pin claims to evidence |
| **Correct access control** | `_tenant` filters are injected server-side and cannot be overridden, scoping every query to the caller's data |
| **Coherent context windows** | Semantic valley + AST-aware chunking keep topics and functions whole — no mid-thought splits |
| **Keyword + semantic precision** | 5-stage pipeline: BM25 pre-filter → HNSW traversal → cosine rescore → RRF → cross-encoder rerank |
| **Whole-corpus recall** | Cross-collection fan-out searches every collection and merges with RRF — no global "superset" duplication |
| **Data sovereignty** | Self-hosted TEI embeddings — no documents or queries leave your infrastructure |
| **Fresh indexes** | Deterministic chunk IDs + delete-before-write mean updated sources never leave stale chunks |
| **Full provenance** | Every result carries its metadata (URL, author, timestamps) for transparent attribution |

**One call, ready for your LLM:**

```bash
curl -s -X POST http://localhost:8000/v1/rag \
  -H "Content-Type: application/json" \
  -d '{"query": "How does HNSW work?", "top_k": 5, "include_text": true}' | jq
```

```json
{
  "status": "success",
  "query": "How does HNSW work?",
  "citations": [
    {
      "doc_id": 12345,
      "chunk_text": "HNSW builds a multi-layer graph where top layers provide long-range shortcuts...",
      "source_url": "https://example.com/hnsw.md",
      "relevance_score": 0.0421,
      "bm25_score": 8.2,
      "semantic_score": 0.87,
      "metadata": { "source": "confluence", "space": "ENG" }
    }
  ],
  "context": "[1] HNSW builds a multi-layer graph where...\n\n[2] Reciprocal Rank Fusion merges...",
  "duration_ms": 42
}
```

The assembled `context` string with `[1]`, `[2]`, … markers can be injected directly into a system or user prompt for grounded generation.

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌──────────────────────┐
│   Connectors    │    │     Crawler      │    │   Direct Ingest      │
│ Confluence/Jira │    │  Master/Worker   │    │   gRPC :50051        │
│ S3/Postgres/... │    │                  │    │                      │
└────────┬────────┘    └────────┬─────────┘    └──────────┬───────────┘
         │ _collection_id       │                          │
         └──────────────┬───────┘──────────────────────────┘
                        │ Kafka (crawl-records)
                        ▼
              ┌─────────────────────┐
              │      Indexer        │     ┌───────────────┐
              │  Collection Manager │────►│  TEI Embedder │
              │  routes by          │     │  :8888        │
              │  _collection_id     │     └───────────────┘
              │  ┌───────┐┌───────┐│
              │  │_default││col-A ││  ← each collection has own
              │  │ Engine ││Engine ││    Bluge + HNSW-IF + Arena
              │  └───────┘└───────┘│
              └─────────┬───────────┘
                        │ gRPC
              ┌─────────▼───────────┐     ┌───────────────┐
              │      Gateway        │────►│ TEI Reranker  │
              │  :8000 (HTTP)       │     │  :8081        │
              │  /search            │     └───────────────┘
              │  /v1/collections/*  │
              └─────────────────────┘
```

## Quick Start

```bash
# 1. Start infrastructure
docker compose -f deploy/docker-compose.yml up -d redis kafka

# 2. Start TEI embedder (builds from source on first run for Apple Silicon)
~/bin/text-embeddings-router --model-id Alibaba-NLP/gte-modernbert-base --port 8888 &

# 3. Start the combined indexer + gateway
YASE_EMBEDDER_PROVIDER=tei go run cmd/local/main.go &

# 4. Start ingestion
go run cmd/ingestion/main.go &

# 5. Search
curl -s -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{"query": "Go programming language", "top_k": 5}' | jq
```

See [Getting Started](docs/getting-started.md) for the full walkthrough (RAG, autocomplete, delete, crawler, connectors).

## Documentation

| Doc | Description |
|---|---|
| [Getting Started](docs/getting-started.md) | Full local setup, RAG, autocomplete, delete, crawler, connectors |
| [Architecture](docs/architecture.md) | Services, search pipeline, project structure |
| [Collections](docs/collections.md) | Data isolation, cross-collection search, management |
| [Connectors & Embeddings](docs/connectors.md) | Enterprise data sources and embedding providers |
| [Configuration & Security](docs/configuration.md) | Config reference, auth, TLS, tenant isolation, observability |
| [API Reference](docs/api-reference.md) | Endpoints + Go client SDK |
| [Deployment & Operations](docs/deployment.md) | Docker Compose, Helm, build & test |

## License

Apache 2.0
