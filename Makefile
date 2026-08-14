GOBIN ?= $(shell go env GOPATH)/bin
PROTO_DIR := proto/v1
BIN_DIR := bin

.PHONY: all proto build build-all test test-race test-short test-integration lint bench clean \
	deps docker-build docker-up docker-down docker-logs \
	e2e e2e-distributed load-test

all: deps proto build test

# ──────────────────────────────────────────
# Dependencies
# ──────────────────────────────────────────

deps:
	go mod tidy
	@which protoc-gen-go > /dev/null 2>&1 || go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@which protoc-gen-go-grpc > /dev/null 2>&1 || go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@which protoc-gen-go-vtproto > /dev/null 2>&1 || go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@latest

# ──────────────────────────────────────────
# Protobuf
# ──────────────────────────────────────────

proto:
	protoc \
		-I. \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		--go-vtproto_out=. --go-vtproto_opt=paths=source_relative,features=marshal+unmarshal+size \
		$(PROTO_DIR)/ingestion.proto $(PROTO_DIR)/index.proto $(PROTO_DIR)/collection.proto

# ──────────────────────────────────────────
# Build
# ──────────────────────────────────────────

# Build core service binaries
build:
	go build -o $(BIN_DIR)/crawler ./cmd/crawler
	go build -o $(BIN_DIR)/ingestion ./cmd/ingestion
	go build -o $(BIN_DIR)/indexer ./cmd/indexer
	go build -o $(BIN_DIR)/gateway ./cmd/gateway
	go build -o $(BIN_DIR)/local ./cmd/local

# Build all binaries including cluster and connector manager
build-all: build
	go build -o $(BIN_DIR)/cluster-node ./cmd/cluster-node
	go build -o $(BIN_DIR)/dist-gateway ./cmd/dist-gateway
	go build -o $(BIN_DIR)/connector ./cmd/connector
	go build -o $(BIN_DIR)/yase-ctl ./cmd/yase-ctl
	go build -o $(BIN_DIR)/ingest-demo ./cmd/ingest-demo

# ──────────────────────────────────────────
# Test
# ──────────────────────────────────────────

# Run all tests (unit + integration, requires Docker)
test:
	go test ./... -count=1 -v

# Run tests with race detector
test-race:
	go test ./... -count=1 -race -v

# Run only unit tests (no Docker required)
test-short:
	go test ./... -count=1 -short -v

# Run integration tests only (requires Docker)
test-integration:
	go test -v -count=1 ./test/integration/...

# Run relevance metric tests
test-relevance:
	go test -v -count=1 ./test/relevance/...

# Run benchmarks
bench:
	go test ./pkg/hnsw/ ./pkg/vector/ ./pkg/memory/ -bench=. -benchmem -run='^$$'

# ──────────────────────────────────────────
# E2E Tests
# ──────────────────────────────────────────

# Local end-to-end tests (22 tests: distributed features + Phase 9 + connectors)
e2e:
	SKIP_CRAWLER=1 bash scripts/e2e-local.sh

# Distributed 3-node cluster E2E (requires Docker for Redis + Kafka)
e2e-distributed:
	bash scripts/e2e-distributed.sh 5

# ──────────────────────────────────────────
# Load Testing
# ──────────────────────────────────────────

# Search load test (default: 50 rps for 30s)
load-test:
	bash test/load/search_load.sh

load-test-heavy:
	bash test/load/search_load.sh 200 60

# ──────────────────────────────────────────
# Lint & Security
# ──────────────────────────────────────────

lint:
	@which golangci-lint > /dev/null 2>&1 || go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	golangci-lint run ./...

# Vulnerability scanning
vuln:
	@which govulncheck > /dev/null 2>&1 || go install golang.org/x/vuln/cmd/govulncheck@latest
	govulncheck ./...

# ──────────────────────────────────────────
# Docker
# ──────────────────────────────────────────

docker-build:
	docker compose -f deploy/docker-compose.yml build

docker-up:
	docker compose -f deploy/docker-compose.yml up -d

docker-down:
	docker compose -f deploy/docker-compose.yml down

docker-logs:
	docker compose -f deploy/docker-compose.yml logs -f

# Start only infrastructure (Redis + Kafka)
docker-infra:
	docker compose -f deploy/docker-compose.yml up -d redis kafka

# ──────────────────────────────────────────
# Kubernetes
# ──────────────────────────────────────────

helm-install:
	helm install yase deploy/helm/yase/

helm-upgrade:
	helm upgrade yase deploy/helm/yase/

helm-uninstall:
	helm uninstall yase

helm-template:
	helm template yase deploy/helm/yase/

# ──────────────────────────────────────────
# TEI (Text Embeddings Inference)
# ──────────────────────────────────────────

# Start TEI embedder (local binary, Metal on macOS)
tei-start:
	~/bin/text-embeddings-router --model-id Alibaba-NLP/gte-modernbert-base --port 8888 &

# Start TEI reranker
tei-reranker-start:
	~/bin/text-embeddings-router --model-id Alibaba-NLP/gte-reranker-modernbert-base --port 8081 &

# ──────────────────────────────────────────
# Cleanup
# ──────────────────────────────────────────

clean:
	rm -rf $(BIN_DIR)/
	go clean ./...

# Remove all Docker volumes and data
clean-all: clean docker-down
	docker volume prune -f
	rm -rf /tmp/yase-*
