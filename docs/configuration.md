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
  arena_size_bytes: 68719476736 # upper bound; arenas grow in 64 MiB segments
  centroid_rate: 5              # 20% centroids
  checkpoint_interval: 1m       # periodic durable snapshot of the vector index
  compact_dead_ratio: 0.3       # compact when deleted/replaced vectors exceed 30%
  default_ttl: 0s               # 0 = documents never expire by default
  ttl_sweep_interval: 10m
  flat_search_max: 20000        # filters matching <= this many docs are searched exactly
  ann_probes: 0                 # centroids probed per query (0 = scale with corpus)

kafka:
  dlq_topic: crawl-records-dlq  # strongly recommended; without it exhausted messages are skipped
  workers: 4                    # concurrent indexing workers (ordered per URL)
  max_attempts: 3
  commit_interval: 5s           # offsets are committed only after a checkpoint

server:
  trusted_proxies: []           # CIDRs whose X-Forwarded-For is trusted
  health_port: 8090             # /health, /ready on the indexer and cluster nodes

rate_limit:
  enabled: false
  backend: memory               # or redis: one budget shared by all gateways

collections:
  base_path: /data/yase/collections
  default_arena: 68719476736    # per-collection upper bound
  max_collections: 100
  default_collection: _default
  allowed_embedder_hosts: []    # hosts a collection may point its embedder at

cluster:
  http_addr: 127.0.0.1:7080     # management API; non-loopback requires join_token
  join_token: ""                # >= 16 chars; sent as X-Yase-Cluster-Token
  num_shards: 3                 # documents are placed by hash(url) % num_shards
```

Every key can be set from the environment (`index.ann_probes` → `YASE_INDEX_ANN_PROBES`); list values are comma-separated.

Config is validated at startup (`config.Validate()`); invalid values (bad ports, unknown providers, `hnsw.m < 2`, empty Kafka brokers, etc.) fail fast.

## Security

| Feature | Status |
|---------|--------|
| API Authentication (API key; JWT HS256, RS256/ES256 via public key or JWKS) | Supported |
| TLS/HTTPS | Supported (HTTP, gRPC, cluster API; mutual TLS for Raft) |
| Tenant Isolation | Supported (server-injected `_tenant` filter) |
| Document-Level ACLs | Planned (via `_acl` metadata) |
| Rate Limiting | Per-tenant; in-memory or Redis-backed (shared across replicas) |
| SSRF guard | Per-collection embedder URLs must be in `collections.allowed_embedder_hosts` |
| Credential redaction | Collection API responses mask embedder API keys |
| Audit Logging | JSON structured events |
| Load Shedding | Concurrent request limiter |

Authentication applies to both the HTTP gateway and gRPC services (indexer, ingestion, cluster-node). Tenant isolation is enforced server-side: the `_tenant` filter is injected into every query from the authenticated context and cannot be overridden by the client. Alias filters likewise take precedence over query filters.

**Service identity.** In the split deployment the gateway calls the indexer with `auth.client_token`. A token *without* a tenant is treated as a trusted service identity that may act for the tenant it names on each request, so give it to the gateway only, keep the indexer reachable only from gateways (the Helm chart's NetworkPolicy does this), and give the token the `admin` role so remote deletes and collection management work.

**JWT.** Configure exactly the key material you use: `auth.jwt.secret` enables HS256, `auth.jwt.public_key_path` or `auth.jwt.jwks_url` (https only, cached, refreshed on unknown `kid`) enable RS256/ES256. Algorithms without a configured key are rejected, which prevents algorithm-confusion attacks.

## Observability

| Tool | Purpose |
|------|---------|
| **Prometheus** | 30+ metrics: search latency, stage breakdown, embedder/reranker latency, cache hits, arena usage, connector sync |
| **Grafana** | Pre-built dashboards at http://localhost:3000 |
| **OpenTelemetry** | Distributed tracing across gateway → indexer → HNSW → embedder (stdout or OTLP/HTTP exporters) |
| **Structured Logging** | JSON logs via `log/slog` with configurable levels |
| **Alerting** | 10 Prometheus alert rules (latency, errors, arena, Kafka lag, Raft) |
