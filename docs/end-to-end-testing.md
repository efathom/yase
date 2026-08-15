# End-to-End Testing (Local Distributed Cluster)

This guide documents the full process of running the 3-node distributed
end-to-end test locally: standing up the shared infrastructure (Redis, Kafka,
TEI embedder), launching the cluster, wiring the topology, crawling,
indexing, and searching — plus the two real defects this exercise surfaced and
how they were handled.

The script under test is [`scripts/e2e-distributed.sh`](../scripts/e2e-distributed.sh).

---

## Prerequisites

| Requirement | Version used | Notes |
|---|---|---|
| Go | 1.26.5 | `go version` |
| macOS | darwin/arm64 (Apple Silicon) | drives several VM choices below |
| RAM | 16 GiB+ | the VM needs ~14 GiB after the TEI fix |
| Free disk | 20 GiB+ | colima disk (80 GiB) + TEI model (~1 GiB) |

The e2e script itself only needs **Redis** and **Kafka**. The **TEI embedder**
is required because the script hard-codes `YASE_EMBEDDER_PROVIDER=tei` and the
default config points `embedder.base_url` at `http://localhost:8888`.

---

## Part 1 — Install Docker (colima)

This machine had no Docker installed. Homebrew was available, so colima + the
Docker CLI were installed:

```bash
brew install colima docker docker-compose
```

`docker-compose` installs as a Docker plugin. Tell the CLI where to find it:

```bash
mkdir -p ~/.docker
cat > ~/.docker/config.json <<'EOF'
{
  "cliPluginsExtraDirs": [
    "/opt/homebrew/lib/docker/cli-plugins"
  ]
}
EOF
```

Start the VM. **Important:** the initial `--memory 8` was not enough (see
Part 3 for the OOM sequel). Use 14 GiB from the start:

```bash
colima start --cpu 4 --memory 14 --disk 80
docker version            # confirm server + client respond
docker compose version    # confirm plugin discovery works
```

---

## Part 2 — Bring up shared infrastructure

`tei` in `deploy/docker-compose.yml` is defined with a `build:` directive
that compiles text-embeddings-inference **from source** — a very large Rust
build that takes a long time. For a local run it is much faster to use the
prebuilt image. A compose **override file** swaps the build for the image and
raises the memory limits (see Part 3):

```bash
docker compose \
  -f deploy/docker-compose.yml \
  -f deploy/docker-compose.tei-override.yml \
  up -d redis kafka tei
```

Starting `tei` downloads the model (`Alibaba-NLP/gte-modernbert-base`) into a
named volume on first run, then warms it up. Verify readiness:

```bash
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.tei-override.yml ps
docker exec deploy-kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null && echo "Kafka OK"
docker exec deploy-redis-1 redis-cli ping                  # PONG
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8888/health  # 200
```

**Gotcha:** TEI's `/health` returns `200` with an *empty body*, so a naive
`curl -s http://localhost:8888/health` prints nothing even when healthy. Use
the HTTP status code (or check container health) instead.

A quick sanity embed of 768 expected dimensions:

```bash
curl -s -m 30 http://localhost:8888/embed \
  -H "Content-Type: application/json" \
  -d '{"inputs":"Go programming language","truncate":true}' | jq '.[0] | length'
# → 768
```

### TEI OOM (exit 137) — the memory fix

The container initially **exited with code 137** (OOM-killed) during model
warmup. Two knobs mattered:

1. The compose file caps TEI at `memory: 4G`, and
2. the colima VM only had 8 GiB to begin with.

Under colima, the amd64 TEI image runs via **qemu x86-64 emulation**, which is
memory-hungry. The fix was two-sided:

- bump the VM: `colima stop && colima start --cpu 4 --memory 14 --disk 80`
- lift the container limit via the override file (12 GiB + `shm_size: 4g`)

Both are captured in `deploy/docker-compose.tei-override.yml`. Without them
the tag is `ghcr.io/huggingface/text-embeddings-inference:cpu-latest`, whose
manifest confirms a multi-arch amd64/arm64 index.

---

## Part 3 — Run the distributed E2E

```bash
bash scripts/e2e-distributed.sh
```

The script, in order:

1. **Reset** Redis frontier + recreate the `crawl-records` topic (6 partitions).
2. **Start ingestion** (`:50051`).
3. **Start 3 cluster nodes** (`cluster-node`), sharing Kafka consumer group
   `yase-cluster`; Kafka auto-assigns 2 partitions per node:
   - node-0 (bootstrap leader): Raft `:7000`, shard gRPC `:50053`, HTTP `:9100`
   - node-1: Raft `:7001`, shard gRPC `:50054`, HTTP `:9101`
   - node-2: Raft `:7002`, shard gRPC `:50055`, HTTP `:9102`
4. **Configure topology** via `yase-ctl` (shard→node assignment + `default` alias).
5. **Start dist-gateway** (`:8001`, scatter-gather → 3 shards → RRF merge).
6. **Start crawler** (master `:9080` + worker), seed `https://go.dev/`.
7. **Wait for indexing** (poll each node's "processed and committed" log line).
8. **Search** through the gateway.
9. **RAG API unit tests** (dist-gateway has no RAG handler wired yet).
10. **Phase 9 enhancements** — arena persistence, HNSW snapshot, engine
    persist, BBQ, DNS TTL, reranker unit tests.
11. **Collections** — manager unit tests, HTTP lifecycle integration, Raft FSM.

Logs land in `/tmp/yase-dist-e2e-logs/` (`ingestion.log`, `node-0/1/2.log`,
`crawler-master.log`, `crawler-worker.log`, `dist-gateway.log`, …); the data
dir is `/tmp/yase-dist-e2e/`. Both are transient.

---

## Part 4 — Defects found along the way

### 4.1 gRPC "missing credentials" — auth middleware ignores `auth.enabled`

**Symptom:** every data path failed with `rpc error: code = Unauthenticated
desc = missing credentials` — crawler→ingestion push, and dist-gateway→shard
search. Zero docs were indexed.

**Root cause:** `pkg/auth/factory.go` `NewFromConfig` dispatched on
`cfg.Method` but never checked `cfg.Enabled`. `configs/default.yaml` has

```yaml
auth:
  enabled: false
  method: api_key
```

(`method: api_key` is intentionally the documented example value, not
commented out). So even with auth disabled, an empty API-key authenticator was
built and every gRPC server eagerly installed its interceptor. HTTP paths were
safe because they guard on `cfg.Auth.Enabled` first (see
`cmd/local/main.go`), but the gRPC path had no such guard.

**Fix:** honor `Enabled` at the source of truth:

```go
func NewFromConfig(cfg config.AuthConfig) (Authenticator, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	switch cfg.Method {
	...
```

Regression tests added in `pkg/auth/auth_test.go:
TestNewFromConfigDisabled` / `TestNewFromConfigEnabled`.

> **Rebuild gotcha:** the e2e script only rebuilds a binary when its own
> `cmd/<bin>/main.go` is newer than the binary. A fix in `pkg/` does **not**
> trigger a rebuild and you silently run stale binaries. Force it:
> `rm -rf bin && bash scripts/e2e-distributed.sh`. Get `go build ./...` green
> first.

### 4.2 Node-2 indexes 0 docs — a low-volume artifact, not a bug

After the auth fix, search results returned but one node sometimes showed
`0 docs (no partitions assigned?)`. Inspecting the topic explained it:

```
crawl-records:0:0   crawl-records:1:1   crawl-records:2:0
crawl-records:3:2   crawl-records:4:0   crawl-records:5:1
```

Only ~4 pages were crawled (`https://go.dev/` yields few candidate links) and
those messages landed on partitions 1/3/5. Node-2's assigned partitions
(0/2/4) were simply empty — Kafka lag was 0 across the board and all 3 shards
served search. This is expected murmur2 key hashing, not loss.

Re-running with more pages proved it:

```bash
CRAWL_WAIT=180 bash scripts/e2e-distributed.sh 80
# → 25 passed, 0 failed — every node indexed ≥ 1 doc
```

Takeaway for the script: the `Node-i: 0 docs` assertion is inherently flaky at
tiny crawl volumes. With enough seed pages the distribution is deterministic.

### 4.3 Verification checklist

```bash
go build ./...               # compiles after the auth fix
go vet ./...
go test -short -count=1 ./pkg/...
go test -race ./pkg/auth/ -count=1
golangci-lint run ./pkg/auth/...
```

---

## Part 5 — Teardown

```bash
docker compose \
  -f deploy/docker-compose.yml \
  -f deploy/docker-compose.tei-override.yml \
  down -v            # -v also removes model/data volumes
colima stop
brew services stop colima   # optional: don't auto-start on login
```

---

## Artifacts worth keeping

| Artifact | Location | Purpose |
|---|---|---|
| TEI override (prebuilt image + memory) | `deploy/docker-compose.tei-override.yml` | avoids the from-source Rust build; required for the 12 GiB/shared-mem fix |
| Distributed E2E harness | `scripts/e2e-distributed.sh` | the 25-check scene described above |
| Local E2E harness | `scripts/e2e-local.sh` | single-node variant (24 checks) |
| Cluster compose (mock embedder) | `deploy/docker-compose.cluster.yml` | all-container cluster; uses `mock` embedder so no TEI needed |
| E2E logs | `/tmp/yase-dist-e2e-logs/` | transient, repopulated per run |
| Auth fix + tests | `pkg/auth/factory.go`, `pkg/auth/auth_test.go` | the code change described in Part 4.1 |