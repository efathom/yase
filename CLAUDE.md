# Claude Project Rules

## Tech Stack
- Go 1.26+ (latest stable)
- Modules
- Standard Project Layout (cmd/, internal/, pkg/, client/)
- Protocol Buffers (proto/v1/) with vtprotobuf for zero-copy serialization
- Bluge for inverted index / BM25
- Custom HNSW-IF implementation for vector search
- TEI (Text Embeddings Inference) for embedding and reranking
- Kafka for async message passing
- Redis for distributed state (bloom filters, rate limiting, connector state)
- Raft (hashicorp/raft + BoltDB) for cluster consensus

## Coding Conventions
- Idiomatic Go: Use `gofmt` and `golangci-lint` standards.
- Error Handling: Handle errors explicitly; do not panic. Return errors up the call stack.
- Structs: Prefer composition over inheritance.
- Interfaces: Keep interfaces small. Define them where they are used.
- Context: Always pass `context.Context` as the first argument.
- Logging: Use `log/slog` for structured logging (not `log.Printf`). JSON format in production.
- Concurrency: Use `sync.Mutex` for shared state, `sync.Map` only for append-only/disjoint-writer patterns. Prefer typed maps with RWMutex over sync.Map in hot paths.
- Unsafe: Bounds-check before `unsafe.Slice` access. Panics in arena operations indicate data corruption.

## Testing
- Table-driven tests are mandatory.
- Use `assert` for readability.
- Aim for >80% coverage on new code.
- Integration tests use testcontainers-go (Postgres, MinIO, MySQL, Redis, Kafka).
- E2E tests: `SKIP_CRAWLER=1 bash scripts/e2e-local.sh` (24 tests) and `bash scripts/e2e-distributed.sh 5` (21+ tests).
- Run `go test -race ./...` before committing.

## Architecture
- Use `internal/` for code not meant for import.
- Use `pkg/` for reusable libraries (index, hnsw, embedder, connector, collection, auth, etc.).
- Use `client/` for the Go client SDK.
- Keep `main.go` slim — all logic in packages.
- Follow the factory pattern for pluggable providers (embedder, reranker, connector, auth).
- New connectors self-register via `init()` in `connector.DefaultRegistry`.
- Search pipeline has 5 stages: BM25 → HNSW → Rescore → RRF → Rerank.
- The connector bridge converts Records → CrawlRecords → Kafka, reusing the existing indexing pipeline.
- **Collections**: Each collection owns its own HybridEngine (physical isolation). The `_default` collection is auto-created at startup. The `collection.Manager` handles CRUD, engine lifecycle, and connector binding. Three search tiers: `/search` → `_default`, `/v1/collections/{id}/search` → single, `/v1/collections/search` → cross-collection fan-out with RRF merge.

## Key Patterns
- **Provider factories**: `pkg/embedder/factory.go`, `pkg/reranker/factory.go`, `pkg/connector/registry.go`
- **Collection manager**: `pkg/collection/manager.go` — CRUD, engine lifecycle, `_default` bootstrap, connector binding
- **Collection searcher**: `pkg/collection/searcher.go` — single + cross-collection search with RRF merge
- **Collection HTTP CRUD**: `internal/gateway/collection_handler.go` — all `/v1/collections/*` endpoints
- **HTTP connector base**: `pkg/connector/http_connector.go` — reusable for all REST API connectors
- **Auth middleware**: `pkg/auth/middleware.go` — HTTP + gRPC interceptors
- **Arena interface**: `pkg/memory/arena.go` — both `OffHeapArena` (anonymous mmap) and `FileArena` (persistent mmap) implement it
- **Persistence**: `pkg/hnsw/persist.go` + `pkg/index/persist.go` — binary snapshot/restore for graph + inverted files + vector offsets

## Security
- Validate SQL/SOQL identifiers before query construction (`pkg/connector/validate.go`).
- Never interpolate user input into SQL/JQL/SOQL without validation.
- Use `url.Values.Encode()` for URL query parameters, not string concatenation.
- Auth middleware skips `/health` and `/ready` endpoints.
- Management APIs bind to `127.0.0.1` only.
- Redact credentials from API responses.

## Configuration
- YAML config + env var overrides via Viper (`YASE_<SECTION>_<KEY>`).
- Validate config at startup via `config.Validate()`.
- Default embedder: TEI with gte-modernbert-base (768d).
- Default reranker: disabled (enable via `reranker.enabled: true`).
- Default auth: disabled (enable via `auth.enabled: true`).
