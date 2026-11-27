# Deployment & Operations

## Docker Compose

```bash
docker compose -f deploy/docker-compose.yml up -d
```

## Kubernetes (Helm)

```bash
helm install yase deploy/helm/yase/ \
  --set embedder.provider=tei \
  --set auth.enabled=true
```

The Helm chart includes:

- Gateway Deployment + HPA (auto-scales on CPU)
- Indexer StatefulSet + PVC (durable arena + snapshots + Bluge index), with
  health/readiness/startup probes and a long termination grace period for the
  final checkpoint. It must run with exactly one replica (the chart refuses
  more) — use cluster mode to scale out.
- NetworkPolicy: only gateway pods reach the indexer's gRPC port
- Non-root pods with a read-only root filesystem and all capabilities dropped
- Ingestion, Crawler, TEI embedder, and optional TEI reranker deployments
- Secret (JWT secret, internal gateway→indexer token), ServiceAccount

## Durability and recovery

Each collection (and each cluster shard) is a durable engine directory:

```
<dir>/bluge/            text index; every write batch is fsynced
<dir>/CURRENT           names the live generation
<dir>/gen-000007/arena.bin, snapshot.bin   vectors + HNSW/IVF snapshot (CRC-checked)
```

- The vector index is checkpointed every `index.checkpoint_interval` and
  before every Kafka offset commit, so a crash re-delivers — never loses —
  documents whose vectors were not yet durable.
- On startup each engine is **reconciled** with its text index: vectors of
  deleted documents are dropped and documents without vectors are re-embedded
  from their stored text. A missing or corrupt snapshot therefore degrades to
  a rebuild, not data loss. Startup can take a while after a crash; the
  readiness probe waits for it.
- Compaction runs automatically when replaced/deleted vectors exceed
  `index.compact_dead_ratio`. It builds a new generation and switches
  `CURRENT` atomically; searches continue during compaction, writes pause.
- Configure `kafka.dlq_topic`. Messages that keep failing are dead-lettered
  with a `yase-error` header; the consumer holds its offset while the DLQ is
  unavailable.

Back up an engine by copying its directory after a checkpoint (or while the
process is stopped).

## Cluster mode, placement and replication

- Documents are assigned to shards by `hash(url) % cluster.num_shards`.
  Changing `num_shards` requires re-indexing.
- Every `cluster-node` consumes the whole topic under its own consumer group
  and indexes only its shard. Run two or more nodes with the same
  `--shard-id` for replication: each replica builds the shard independently
  from the log, and `dist-gateway` rotates across live replicas and fails
  over when one is down. Responses carry `partial: true` if a shard had no
  healthy replica.
- Use a **log-compacted** Kafka topic keyed by URL so a new or replaced
  replica can rebuild its shard from Kafka.
- The cluster management API binds to `cluster.http_addr` (loopback by
  default). Exposing it requires `cluster.join_token`, which nodes,
  `dist-gateway` and `yase-ctl` (`YASE_CLUSTER_JOIN_TOKEN`) present.
- With `tls.enabled` (and `tls.client_auth`), Raft traffic uses mutual TLS
  and the cluster API is served over HTTPS.

## Build & Test

```bash
make build         # Compile all binaries
make test          # Unit + integration tests
make test-race     # Race detector
make bench         # Benchmarks
make lint          # golangci-lint

# End-to-end tests
SKIP_CRAWLER=1 bash scripts/e2e-local.sh          # Local (24 tests)
bash scripts/e2e-distributed.sh 5                  # 3-node cluster

# Load tests
bash test/load/search_load.sh 100 60              # 100 rps for 60s
```

See [End-to-End Testing](end-to-end-testing.md) for the full local distributed
run: standing up Redis/Kafka/TEI, the colima setup, and the auth defect it
uncovered.
