#!/bin/bash
# Load test for YASE search API using vegeta.
# Install: go install github.com/tsenart/vegeta@latest
#
# Usage:
#   ./test/load/search_load.sh                    # defaults: 50 rps, 30s
#   ./test/load/search_load.sh 100 60             # 100 rps for 60s
#   API_KEY=your-key ./test/load/search_load.sh   # with auth

set -euo pipefail

RATE="${1:-50}"
DURATION="${2:-30}"
URL="${YASE_URL:-http://localhost:8000}"
API_KEY="${API_KEY:-}"

echo "=== YASE Search Load Test ==="
echo "  URL:      $URL"
echo "  Rate:     ${RATE} req/s"
echo "  Duration: ${DURATION}s"
echo ""

# Build request body
BODY='{"query":"Go programming language hybrid search engine","top_k":10}'

# Build headers
HEADERS="Content-Type: application/json"
if [ -n "$API_KEY" ]; then
  HEADERS="$HEADERS\nX-API-Key: $API_KEY"
fi

# Check if vegeta is available
if ! command -v vegeta &> /dev/null; then
  echo "vegeta not found. Install with: go install github.com/tsenart/vegeta@latest"
  echo ""
  echo "Falling back to curl-based benchmark..."

  # Simple curl benchmark
  echo "Warming up (5 requests)..."
  for i in $(seq 1 5); do
    curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" \
      -X POST "$URL/search" \
      -H "Content-Type: application/json" \
      -H "X-API-Key: $API_KEY" \
      -d "$BODY"
  done

  echo ""
  echo "Benchmarking (20 sequential requests)..."
  total=0
  for i in $(seq 1 20); do
    latency=$(curl -s -o /dev/null -w "%{time_total}" \
      -X POST "$URL/search" \
      -H "Content-Type: application/json" \
      -H "X-API-Key: $API_KEY" \
      -d "$BODY")
    ms=$(echo "$latency * 1000" | bc)
    printf "  [%2d] %.1fms\n" $i $ms
    total=$(echo "$total + $latency" | bc)
  done
  avg=$(echo "scale=1; $total / 20 * 1000" | bc)
  echo ""
  echo "  Average: ${avg}ms (20 sequential requests)"
  exit 0
fi

# Vegeta attack
echo "POST $URL/search" | vegeta attack \
  -rate="${RATE}/s" \
  -duration="${DURATION}s" \
  -body=<(echo "$BODY") \
  -header="Content-Type: application/json" \
  ${API_KEY:+-header="X-API-Key: $API_KEY"} \
  | vegeta report

echo ""
echo "=== Latency Histogram ==="
echo "POST $URL/search" | vegeta attack \
  -rate="${RATE}/s" \
  -duration="10s" \
  -body=<(echo "$BODY") \
  -header="Content-Type: application/json" \
  ${API_KEY:+-header="X-API-Key: $API_KEY"} \
  | vegeta report -type=hist[0,10ms,25ms,50ms,100ms,250ms,500ms,1s]
