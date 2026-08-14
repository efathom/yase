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
- Indexer StatefulSet + PVC (persistent arena + Bluge index)
- Ingestion, Crawler, TEI embedder, and optional TEI reranker deployments
- ConfigMap, Secret, ServiceAccount

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
