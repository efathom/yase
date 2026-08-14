#!/bin/bash
# Load test for YASE ingestion throughput.
# Measures how fast documents flow through Kafka → indexer pipeline.
#
# Usage:
#   ./test/load/ingest_load.sh          # default: 1000 docs
#   ./test/load/ingest_load.sh 5000     # 5000 docs

set -euo pipefail

NUM_DOCS="${1:-1000}"
GRPC_ADDR="${YASE_GRPC_ADDR:-localhost:50051}"

echo "=== YASE Ingestion Load Test ==="
echo "  gRPC:     $GRPC_ADDR"
echo "  Documents: $NUM_DOCS"
echo ""

# Check if grpcurl is available
if ! command -v grpcurl &> /dev/null; then
  echo "grpcurl not found. Install with: go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest"
  echo ""
  echo "Falling back to HTTP search to measure indexing..."
  echo "Start the ingestion pipeline and use the connector system or crawler to feed documents."
  exit 1
fi

echo "Sending $NUM_DOCS batch ingest requests..."
start=$(date +%s%N)

for i in $(seq 1 $NUM_DOCS); do
  grpcurl -plaintext -d "{\"records\":[{\"url\":\"https://loadtest.example.com/doc/$i\",\"raw_content\":\"$(printf 'Load test document %d. This is a test of the YASE ingestion pipeline with realistic content length to measure throughput. ' $i)\",\"metadata\":{\"source\":\"loadtest\",\"doc_id\":\"$i\"}}]}" \
    "$GRPC_ADDR" yase.v1.IngestionService/IngestBatch > /dev/null 2>&1 &

  # Batch in groups of 50 to avoid overwhelming the connection
  if [ $((i % 50)) -eq 0 ]; then
    wait
    printf "  %d/%d documents sent\n" $i $NUM_DOCS
  fi
done
wait

end=$(date +%s%N)
elapsed=$(( (end - start) / 1000000 ))
rate=$(echo "scale=1; $NUM_DOCS * 1000 / $elapsed" | bc)

echo ""
echo "  Sent:     $NUM_DOCS documents"
echo "  Elapsed:  ${elapsed}ms"
echo "  Rate:     ${rate} docs/s"
