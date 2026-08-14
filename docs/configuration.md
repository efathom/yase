# Configuration & Security

## Configuration

YAML config with env var overrides (`YASE_<SECTION>_<KEY>`). See [`configs/default.yaml`](../configs/default.yaml).

```yaml
embedder:
  provider: tei
  model: Alibaba-NLP/gte-modernbert-base
  dimension: 768

reranker:
  enabled: false
  provider: tei
  model: Alibaba-NLP/gte-reranker-modernbert-base
  candidates: 50

auth:
  enabled: false
  method: api_key
  api_keys:
    - key: "your-api-key"
      tenant_id: "your-tenant"
      roles: ["admin"]

index:
  arena_size_bytes: 4294967296  # 4GB
  centroid_rate: 5              # 20% centroids

collections:
  base_path: /data/yase/collections
  default_arena: 1073741824    # 1GB per collection
  max_collections: 100
  default_collection: _default
```

Config is validated at startup (`config.Validate()`); invalid values (bad ports, unknown providers, `hnsw.m < 2`, empty Kafka brokers, etc.) fail fast.

## Security

| Feature | Status |
|---------|--------|
| API Authentication (API key + JWT) | Supported |
| TLS/HTTPS | Supported (HTTP + gRPC) |
| Tenant Isolation | Supported (server-injected `_tenant` filter) |
| Document-Level ACLs | Planned (via `_acl` metadata) |
| Rate Limiting | Per-tenant, configurable |
| Audit Logging | JSON structured events |
| Load Shedding | Concurrent request limiter |

Authentication applies to both the HTTP gateway and gRPC services (indexer, ingestion, cluster-node). Tenant isolation is enforced server-side: the `_tenant` filter is injected into every query from the authenticated context and cannot be overridden by the client.

## Observability

| Tool | Purpose |
|------|---------|
| **Prometheus** | 30+ metrics: search latency, stage breakdown, embedder/reranker latency, cache hits, arena usage, connector sync |
| **Grafana** | Pre-built dashboards at http://localhost:3000 |
| **OpenTelemetry** | Distributed tracing across gateway → indexer → HNSW → embedder (stdout or OTLP/HTTP exporters) |
| **Structured Logging** | JSON logs via `log/slog` with configurable levels |
| **Alerting** | 10 Prometheus alert rules (latency, errors, arena, Kafka lag, Raft) |
