#!/bin/bash
# End-to-end local test suite:
#   Phase 1: Distributed features (K-means, PQ, Raft, sharding, scatter-gather, storage, parser)
#   Phase 2: Live crawler pipeline (crawler → ingestion → Kafka → indexer → gateway → search)
#
# Prerequisites:
#   1. Docker containers for Redis and Kafka must be running:
#      docker compose -f deploy/docker-compose.yml up -d redis kafka
#   2. Binaries must be built (auto-built if missing):
#      make build
#
# Usage:
#   ./scripts/e2e-local.sh                          # full suite: distributed + crawler (10 pages)
#   ./scripts/e2e-local.sh 5 https://example.com    # custom: 5 pages, custom seed URL
#   SKIP_CRAWLER=1 ./scripts/e2e-local.sh           # distributed tests only (no Docker needed)
#   SKIP_DISTRIBUTED=1 ./scripts/e2e-local.sh       # crawler tests only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BIN_DIR="$SCRIPT_DIR/bin"

MAX_PAGES="${1:-10}"
SEED_URL="${2:-https://go.dev/}"
LOG_DIR="/tmp/yase-e2e-logs"
CRAWL_WAIT="${CRAWL_WAIT:-60}"
SKIP_CRAWLER="${SKIP_CRAWLER:-0}"
SKIP_DISTRIBUTED="${SKIP_DISTRIBUTED:-0}"

PASS=0
FAIL=0

pass() { PASS=$((PASS + 1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL + 1)); echo "  FAIL: $1"; }

mkdir -p "$LOG_DIR"

echo "========================================"
echo "  YASE End-to-End Test Suite"
echo "========================================"
echo ""

# ════════════════════════════════════════════
# Phase 1: Distributed features (in-process)
# ════════════════════════════════════════════
if [ "$SKIP_DISTRIBUTED" != "1" ]; then
  echo "=== Phase 1: Distributed Features ==="
  echo ""

  # Run the distributed integration test
  echo "--- 1.1 Distributed pipeline (Raft + sharding + scatter-gather) ---"
  if go test -v -count=1 -run TestDistributedPipelineE2E "$SCRIPT_DIR/test/integration/" > "$LOG_DIR/distributed.log" 2>&1; then
    pass "Distributed pipeline E2E"
    # Extract key metrics from test output
    grep -E "(K-means|PQ|Built|Raft|Alias|Search returned|After alias|HTML parsed)" "$LOG_DIR/distributed.log" | while read -r line; do
      echo "    $line"
    done
  else
    fail "Distributed pipeline E2E"
    echo "    See $LOG_DIR/distributed.log for details"
    tail -20 "$LOG_DIR/distributed.log" | sed 's/^/    /'
  fi
  echo ""

  # Run the crawler domain filter test (needs Docker for Redis)
  echo "--- 1.2 Crawler domain filter ---"
  if go test -v -count=1 -run TestCrawlerDomainFilterE2E "$SCRIPT_DIR/test/integration/" > "$LOG_DIR/crawler-filter.log" 2>&1; then
    pass "Crawler domain filter"
    grep "Crawled:" "$LOG_DIR/crawler-filter.log" | head -5 | while read -r line; do
      echo "    $line"
    done
  else
    fail "Crawler domain filter"
    echo "    See $LOG_DIR/crawler-filter.log"
  fi
  echo ""

  # K-means + PQ unit tests
  echo "--- 1.3 K-means clustering ---"
  if go test -v -count=1 -run "TestKMeans" "$SCRIPT_DIR/pkg/cluster/" > "$LOG_DIR/kmeans.log" 2>&1; then
    pass "K-means clustering"
    grep -E "(Converged|inertia)" "$LOG_DIR/kmeans.log" | head -5 | while read -r line; do
      echo "    $line"
    done
  else
    fail "K-means clustering"
  fi
  echo ""

  echo "--- 1.4 Product Quantization ---"
  if go test -v -count=1 -run "TestPQ" "$SCRIPT_DIR/pkg/vector/" > "$LOG_DIR/pq.log" 2>&1; then
    pass "Product Quantization"
    grep -E "(Recall|Reconstruction)" "$LOG_DIR/pq.log" | head -3 | while read -r line; do
      echo "    $line"
    done
  else
    fail "Product Quantization"
  fi
  echo ""

  echo "--- 1.5 Raft consensus ---"
  if go test -v -count=1 -run "TestRaft" "$SCRIPT_DIR/internal/consensus/" > "$LOG_DIR/raft.log" 2>&1; then
    pass "Raft consensus"
  else
    fail "Raft consensus"
  fi
  echo ""

  echo "--- 1.6 Consistent hash ring ---"
  if go test -v -count=1 -run "TestHashRing" "$SCRIPT_DIR/pkg/routing/" > "$LOG_DIR/hashring.log" 2>&1; then
    pass "Consistent hash ring"
    grep -E "(Balance|Removing|Adding|StdDev)" "$LOG_DIR/hashring.log" | head -4 | while read -r line; do
      echo "    $line"
    done
  else
    fail "Consistent hash ring"
  fi
  echo ""

  echo "--- 1.7 Scatter-gather coordinator ---"
  if go test -v -count=1 -run "TestCoordinator" "$SCRIPT_DIR/internal/query/" > "$LOG_DIR/coordinator.log" 2>&1; then
    pass "Scatter-gather coordinator"
  else
    fail "Scatter-gather coordinator"
  fi
  echo ""

  echo "--- 1.8 Storage + cache ---"
  if go test -v -count=1 "$SCRIPT_DIR/pkg/storage/" > "$LOG_DIR/storage.log" 2>&1; then
    pass "Storage + cache"
  else
    fail "Storage + cache"
  fi
  echo ""

  echo "--- 1.9 Offline index builder ---"
  if go test -v -count=1 "$SCRIPT_DIR/internal/builder/" > "$LOG_DIR/builder.log" 2>&1; then
    pass "Offline index builder"
    grep -E "Built" "$LOG_DIR/builder.log" | head -2 | while read -r line; do
      echo "    $line"
    done
  else
    fail "Offline index builder"
  fi
  echo ""

  echo "--- 1.10 MIME parser router ---"
  if go test -v -count=1 "$SCRIPT_DIR/internal/parser/" > "$LOG_DIR/parser.log" 2>&1; then
    pass "MIME parser router"
  else
    fail "MIME parser router"
  fi
  echo ""

  echo "--- 1.11 SIMD distance functions ---"
  if go test -v -count=1 -run "TestDotProduct|TestCosine|TestDistFunc" "$SCRIPT_DIR/pkg/vector/" > "$LOG_DIR/simd.log" 2>&1; then
    pass "SIMD distance functions"
  else
    fail "SIMD distance functions"
  fi
  echo ""

  # ── Phase 9 features ──
  echo "=== Phase 1b: Phase 9 Enhancements ==="
  echo ""

  echo "--- 1.12 DNS TTL cache ---"
  if go test -v -count=1 -run "TestDNSCache|TestNewTuned" "$SCRIPT_DIR/pkg/crawler/" > "$LOG_DIR/dns-ttl.log" 2>&1; then
    pass "DNS TTL cache"
    grep -E "(PASS|TTL)" "$LOG_DIR/dns-ttl.log" | head -5 | while read -r line; do
      echo "    $line"
    done
  else
    fail "DNS TTL cache"
  fi
  echo ""

  echo "--- 1.13 File-backed arena (persistence) ---"
  if go test -v -count=1 -run "TestFileArena" "$SCRIPT_DIR/pkg/memory/" > "$LOG_DIR/file-arena.log" 2>&1; then
    pass "File-backed arena"
    grep -E "(Persistence|PASS)" "$LOG_DIR/file-arena.log" | head -5 | while read -r line; do
      echo "    $line"
    done
  else
    fail "File-backed arena"
  fi
  echo ""

  echo "--- 1.14 HNSW graph snapshot/restore ---"
  if go test -v -count=1 -run "TestGraphSnapshot" "$SCRIPT_DIR/pkg/hnsw/" > "$LOG_DIR/hnsw-persist.log" 2>&1; then
    pass "HNSW graph snapshot/restore"
    grep -E "(Snapshot size|PASS)" "$LOG_DIR/hnsw-persist.log" | head -3 | while read -r line; do
      echo "    $line"
    done
  else
    fail "HNSW graph snapshot/restore"
  fi
  echo ""

  echo "--- 1.15 HybridEngine persist/restore ---"
  if go test -v -count=1 -run "TestHybridEnginePersist" "$SCRIPT_DIR/pkg/index/" > "$LOG_DIR/engine-persist.log" 2>&1; then
    pass "HybridEngine persist/restore"
    grep -E "(Before|After|PASS)" "$LOG_DIR/engine-persist.log" | head -3 | while read -r line; do
      echo "    $line"
    done
  else
    fail "HybridEngine persist/restore"
  fi
  echo ""

  echo "--- 1.16 BBQ (Binary Quantized search) ---"
  if go test -v -count=1 -run "TestSearchBBQ|TestBBQDisabled" "$SCRIPT_DIR/pkg/hnsw/" > "$LOG_DIR/bbq.log" 2>&1; then
    pass "BBQ search"
    grep -E "(recall|oversample|PASS)" "$LOG_DIR/bbq.log" | head -5 | while read -r line; do
      echo "    $line"
    done
  else
    fail "BBQ search"
  fi
  echo ""

  echo "--- 1.17 Cached store ---"
  if go test -v -count=1 -run "TestCachedStore" "$SCRIPT_DIR/pkg/storage/" > "$LOG_DIR/cached-store.log" 2>&1; then
    pass "Cached store"
  else
    fail "Cached store"
  fi
  echo ""

  echo "--- 1.17b S3 store (MinIO testcontainer) ---"
  if go test -v -count=1 -run "TestS3Store" "$SCRIPT_DIR/test/integration/" > "$LOG_DIR/s3-store.log" 2>&1; then
    pass "S3 store (MinIO)"
    grep -E "(Large file|PASS)" "$LOG_DIR/s3-store.log" | head -5 | while read -r line; do
      echo "    $line"
    done
  else
    fail "S3 store (MinIO)"
    tail -5 "$LOG_DIR/s3-store.log" | sed 's/^/    /'
  fi
  echo ""

  echo "--- 1.18 Reranker ---"
  if go test -v -count=1 "$SCRIPT_DIR/pkg/reranker/" > "$LOG_DIR/reranker.log" 2>&1; then
    pass "Reranker (TEI client + mock + factory)"
    grep -E "(PASS)" "$LOG_DIR/reranker.log" | head -5 | while read -r line; do
      echo "    $line"
    done
  else
    fail "Reranker"
  fi
  echo ""

  echo "--- 1.19b Connector framework ---"
  if go test -v -count=1 "$SCRIPT_DIR/pkg/connector/" > "$LOG_DIR/connector-framework.log" 2>&1; then
    pass "Connector framework (auth, registry, HTTP base, state, paginators)"
    grep -c "PASS" "$LOG_DIR/connector-framework.log" | while read -r line; do
      echo "    $line tests passed"
    done
  else
    fail "Connector framework"
  fi
  echo ""

  echo "--- 1.19c Connector integration (Confluence, Jira, S3, Postgres) ---"
  if go test -v -count=1 -run "TestConfluenceConnector|TestJiraConnector|TestS3Connector|TestPostgresConnector" "$SCRIPT_DIR/test/integration/" > "$LOG_DIR/connector-integration.log" 2>&1; then
    pass "Connector integration tests"
    grep -E "^--- PASS" "$LOG_DIR/connector-integration.log" | head -7 | while read -r line; do
      echo "    $line"
    done
  else
    fail "Connector integration tests"
    tail -10 "$LOG_DIR/connector-integration.log" | sed 's/^/    /'
  fi
  echo ""

  echo "--- 1.20 RAG API handler ---"
  if go test -v -count=1 -run "TestRAG" "$SCRIPT_DIR/internal/gateway/" > "$LOG_DIR/rag.log" 2>&1; then
    pass "RAG API handler"
    grep -E "(Citation|PASS)" "$LOG_DIR/rag.log" | head -5 | while read -r line; do
      echo "    $line"
    done
  else
    fail "RAG API handler"
  fi
  echo ""

  echo "=== Phase 1c: Collections ==="
  echo ""

  echo "--- 1.21 Collection manager (CRUD + _default bootstrap) ---"
  if go test -v -count=1 "$SCRIPT_DIR/pkg/collection/" > "$LOG_DIR/collection.log" 2>&1; then
    CPASS=$(grep -c "^--- PASS" "$LOG_DIR/collection.log" || echo "0")
    pass "Collection manager ($CPASS tests)"
    grep -E "^--- PASS" "$LOG_DIR/collection.log" | head -5 | while read -r line; do
      echo "    $line"
    done
  else
    fail "Collection manager"
    tail -10 "$LOG_DIR/collection.log" | sed 's/^/    /'
  fi
  echo ""

  echo "--- 1.22 Collection HTTP lifecycle + search integration ---"
  if go test -v -count=1 -run "TestCollection" "$SCRIPT_DIR/test/integration/" > "$LOG_DIR/collection-integ.log" 2>&1; then
    pass "Collection integration (lifecycle + search)"
    grep -E "^--- PASS" "$LOG_DIR/collection-integ.log" | while read -r line; do
      echo "    $line"
    done
  else
    fail "Collection integration"
    tail -10 "$LOG_DIR/collection-integ.log" | sed 's/^/    /'
  fi
  echo ""
fi

# ════════════════════════════════════════════
# Phase 2: Live crawler pipeline
# ════════════════════════════════════════════
if [ "$SKIP_CRAWLER" != "1" ]; then
  echo "=== Phase 2: Live Crawler Pipeline ==="
  echo "  MaxPages:  $MAX_PAGES"
  echo "  Seed URL:  $SEED_URL"
  echo ""

  # Build binaries if needed
  for bin in ingestion local crawler; do
    if [ ! -f "$BIN_DIR/$bin" ]; then
      echo "Building $bin..."
      go build -o "$BIN_DIR/$bin" "$SCRIPT_DIR/cmd/$bin"
    fi
  done

  rm -rf /tmp/yase-e2e-local 2>/dev/null || true

  # Cleanup on exit
  PIDS=""
  cleanup_crawler() {
    if [ -n "$PIDS" ]; then
      echo ""
      echo "  Stopping crawler services..."
      kill $PIDS 2>/dev/null || true
      wait 2>/dev/null || true
    fi
  }
  trap cleanup_crawler EXIT

  # Kill leftover processes
  for port in 50051 8000 9080; do
    lsof -ti :$port 2>/dev/null | xargs kill 2>/dev/null || true
  done
  sleep 1

  # Reset Redis
  echo "--- 2.1 Reset Redis ---"
  docker exec deploy-redis-1 redis-cli DEL crawler:frontier > /dev/null 2>&1 || true
  docker exec deploy-redis-1 redis-cli BF.RESERVE crawler:frontier 0.001 10000000 > /dev/null 2>&1 || true
  pass "Redis bloom filter reset"
  echo ""

  # Start services
  echo "--- 2.2 Start services ---"
  YASE_EMBEDDER_PROVIDER=mock \
    "$BIN_DIR/ingestion" > "$LOG_DIR/ingestion.log" 2>&1 &
  PIDS="$!"

  YASE_EMBEDDER_PROVIDER=mock \
    YASE_INDEX_PATH=/tmp/yase-e2e-local/bluge \
    YASE_INDEX_ARENA_SIZE_BYTES=104857600 \
    "$BIN_DIR/local" > "$LOG_DIR/local.log" 2>&1 &
  PIDS="$PIDS $!"

  YASE_CRAWLER_MAX_PAGES="$MAX_PAGES" \
    YASE_EMBEDDER_PROVIDER=mock \
    YASE_METRICS_ADDR=:9091 \
    "$BIN_DIR/crawler" -mode master > "$LOG_DIR/master.log" 2>&1 &
  PIDS="$PIDS $!"

  YASE_EMBEDDER_PROVIDER=mock \
    YASE_METRICS_ADDR=:9093 \
    "$BIN_DIR/crawler" -mode worker -id worker-001 -master http://localhost:9080 > "$LOG_DIR/worker.log" 2>&1 &
  PIDS="$PIDS $!"

  sleep 3

  # Port check
  echo "--- 2.3 Port check ---"
  ALL_PORTS_OK=true
  for port in 50051 8000 9080; do
    if lsof -i :$port 2>/dev/null | grep -q LISTEN; then
      echo "    :$port  OK"
    else
      echo "    :$port  NOT LISTENING"
      ALL_PORTS_OK=false
    fi
  done
  if $ALL_PORTS_OK; then
    pass "All service ports listening"
  else
    fail "Some ports not listening"
  fi
  echo ""

  # Seed
  echo "--- 2.4 Seed crawler ---"
  SEED_RESULT=$(curl -s -X POST http://localhost:9080/seed \
    -H "Content-Type: application/json" \
    -d "[\"$SEED_URL\"]")
  echo "    $SEED_RESULT"
  if echo "$SEED_RESULT" | grep -q '"seeded":1'; then
    pass "Seed URL accepted"
  else
    fail "Seed URL not accepted: $SEED_RESULT"
  fi
  echo ""

  # Wait for crawling
  echo "--- 2.5 Crawling (max ${CRAWL_WAIT}s) ---"
  for i in $(seq 1 $((CRAWL_WAIT / 5))); do
    sleep 5
    printf "    %3ds ..." $((i * 5))
    if grep -q "MaxPages limit" "$LOG_DIR/master.log" 2>/dev/null; then
      echo " (MaxPages limit reached)"
      break
    fi
    INDEXED=$(grep -c "processed and committed" "$LOG_DIR/local.log" 2>/dev/null || echo "0")
    echo " ($INDEXED docs indexed)"
    if [ "$INDEXED" -ge "$MAX_PAGES" ] 2>/dev/null; then
      break
    fi
  done
  echo ""

  # Check indexing
  echo "--- 2.6 Indexing results ---"
  INDEXED=$(grep -c "processed and committed" "$LOG_DIR/local.log" 2>/dev/null || echo "0")
  if [ "$INDEXED" -gt 0 ] 2>/dev/null; then
    pass "Indexed $INDEXED documents"
    grep "processed and committed" "$LOG_DIR/local.log" | head -5 | while read -r line; do
      echo "    $line"
    done
    TOTAL=$(grep -c "processed and committed" "$LOG_DIR/local.log" 2>/dev/null || echo "0")
    if [ "$TOTAL" -gt 5 ]; then
      echo "    ... ($TOTAL total)"
    fi
  else
    fail "No documents indexed"
    echo "    Master log:"
    tail -10 "$LOG_DIR/master.log" | sed 's/^/    /'
    echo "    Worker log:"
    tail -10 "$LOG_DIR/worker.log" | sed 's/^/    /'
  fi
  echo ""

  # Search
  echo "--- 2.7 Search query ---"
  SEARCH_RESULT=$(curl -s -X POST http://localhost:8000/search \
    -H "Content-Type: application/json" \
    -d '{"query": "Go programming language", "top_k": 5}')

  if echo "$SEARCH_RESULT" | grep -q '"status":"success"'; then
    COUNT=$(echo "$SEARCH_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo "?")
    DURATION=$(echo "$SEARCH_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('duration_ms',0))" 2>/dev/null || echo "?")
    pass "Search returned $COUNT results in ${DURATION}ms"
    echo "$SEARCH_RESULT" | python3 -m json.tool 2>/dev/null | head -20 | sed 's/^/    /'
  else
    fail "Search failed"
    echo "    $SEARCH_RESULT"
  fi
  echo ""
fi

# ════════════════════════════════════════════
# Summary
# ════════════════════════════════════════════
echo "========================================"
echo "  Results: $PASS passed, $FAIL failed"
echo "  Logs:    $LOG_DIR/"
echo "========================================"

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
