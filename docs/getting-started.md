# Getting Started

## Prerequisites

- Go 1.26+
- Docker & Docker Compose

## Run Locally

```bash
# Start infrastructure
docker compose -f deploy/docker-compose.yml up -d redis kafka

# Start TEI embedder (builds from source on first run for Apple Silicon)
~/bin/text-embeddings-router --model-id Alibaba-NLP/gte-modernbert-base --port 8888 &

# Start combined indexer + gateway
YASE_EMBEDDER_PROVIDER=tei go run cmd/local/main.go &

# Start ingestion service
go run cmd/ingestion/main.go &

# Search
curl -s -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{"query": "Go programming language", "top_k": 5}' | jq
```

## RAG (Retrieval-Augmented Generation)

```bash
curl -s -X POST http://localhost:8000/v1/rag \
  -H "Content-Type: application/json" \
  -d '{"query": "How does HNSW work?", "top_k": 5, "include_text": true}' | jq
```

The response includes citations with source URLs, chunk text, and `[N]` context markers for LLM prompt injection. See the [README](../README.md#why-yase-for-agentic-rag) for the full response shape.

## Autocomplete

```bash
curl -s 'http://localhost:8000/v1/suggest?q=hn&limit=5' | jq
```

## Delete Documents

```bash
# By ID
curl -X DELETE http://localhost:8000/v1/documents \
  -d '{"doc_ids": [123, 456]}'

# By metadata filter
curl -X DELETE http://localhost:8000/v1/documents/query \
  -d '{"filters": {"source": "confluence", "space": "ENG"}}'
```

## Run the Crawler

```bash
go run cmd/crawler/main.go -mode master &
go run cmd/crawler/main.go -mode worker -id worker-001 -master http://localhost:9080 &

curl -X POST http://localhost:9080/seed \
  -d '["https://go.dev/"]'
```

## Run Enterprise Connectors

```bash
# Start connector manager with config
go run cmd/connector/main.go --connectors configs/connectors.json

# Trigger a sync manually
curl -X POST http://localhost:9300/jobs/confluence-engineering/trigger
```
