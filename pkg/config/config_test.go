package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load defaults: %v", err)
	}

	if cfg.Server.GRPCPort != 50051 {
		t.Errorf("expected grpc_port 50051, got %d", cfg.Server.GRPCPort)
	}
	if cfg.Server.HTTPPort != 8000 {
		t.Errorf("expected http_port 8000, got %d", cfg.Server.HTTPPort)
	}
	if cfg.Server.MaxRecvMsgSize != 16*1024*1024 {
		t.Errorf("expected max_recv_msg_size 16MB, got %d", cfg.Server.MaxRecvMsgSize)
	}
	if cfg.HNSW.M != 16 {
		t.Errorf("expected hnsw.m 16, got %d", cfg.HNSW.M)
	}
	if cfg.HNSW.Mmax0 != 32 {
		t.Errorf("expected hnsw.mmax0 32, got %d", cfg.HNSW.Mmax0)
	}
	if cfg.HNSW.EfConstruction != 200 {
		t.Errorf("expected hnsw.ef_construction 200, got %d", cfg.HNSW.EfConstruction)
	}
	if cfg.Index.CentroidRate != 5 {
		t.Errorf("expected centroid_rate 5, got %d", cfg.Index.CentroidRate)
	}
	if cfg.Kafka.BatchSize != 500 {
		t.Errorf("expected kafka.batch_size 500, got %d", cfg.Kafka.BatchSize)
	}
	if cfg.Crawler.MaxWorkers != 2000 {
		t.Errorf("expected crawler.max_workers 2000, got %d", cfg.Crawler.MaxWorkers)
	}
	if cfg.Crawler.MaxRetries != 3 {
		t.Errorf("expected crawler.max_retries 3, got %d", cfg.Crawler.MaxRetries)
	}
	if cfg.Embedder.Dimension != 768 {
		t.Errorf("expected embedder.dimension 768, got %d", cfg.Embedder.Dimension)
	}
	if cfg.Metrics.Addr != ":9090" {
		t.Errorf("expected metrics.addr :9090, got %s", cfg.Metrics.Addr)
	}
}

func TestLoadFromYAMLFile(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "test.yaml")

	yaml := `
server:
  grpc_port: 60000
  http_port: 9000
  shutdown_grace: 30s
kafka:
  brokers:
    - broker1:9092
    - broker2:9092
  topic: test-topic
hnsw:
  m: 32
  ef_construction: 400
index:
  arena_size_bytes: 8589934592
crawler:
  max_workers: 500
`
	if err := os.WriteFile(configPath, []byte(yaml), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Server.GRPCPort != 60000 {
		t.Errorf("expected grpc_port 60000, got %d", cfg.Server.GRPCPort)
	}
	if cfg.Server.HTTPPort != 9000 {
		t.Errorf("expected http_port 9000, got %d", cfg.Server.HTTPPort)
	}
	if cfg.Server.ShutdownGrace != 30*time.Second {
		t.Errorf("expected shutdown_grace 30s, got %v", cfg.Server.ShutdownGrace)
	}
	if len(cfg.Kafka.Brokers) != 2 || cfg.Kafka.Brokers[0] != "broker1:9092" {
		t.Errorf("expected 2 brokers starting with broker1:9092, got %v", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.Topic != "test-topic" {
		t.Errorf("expected topic test-topic, got %s", cfg.Kafka.Topic)
	}
	if cfg.HNSW.M != 32 {
		t.Errorf("expected hnsw.m 32, got %d", cfg.HNSW.M)
	}
	if cfg.HNSW.EfConstruction != 400 {
		t.Errorf("expected hnsw.ef_construction 400, got %d", cfg.HNSW.EfConstruction)
	}
	if cfg.Index.ArenaSize != 8589934592 {
		t.Errorf("expected arena_size 8GB, got %d", cfg.Index.ArenaSize)
	}
	if cfg.Crawler.MaxWorkers != 500 {
		t.Errorf("expected max_workers 500, got %d", cfg.Crawler.MaxWorkers)
	}
	// Defaults should still apply for unset fields
	if cfg.HNSW.EfSearch != 50 {
		t.Errorf("expected default ef_search 50, got %d", cfg.HNSW.EfSearch)
	}
	if cfg.Redis.Addr != "localhost:6379" {
		t.Errorf("expected default redis addr, got %s", cfg.Redis.Addr)
	}
}

func TestLoadEnvVarOverride(t *testing.T) {
	t.Setenv("YASE_SERVER_GRPC_PORT", "55555")
	t.Setenv("YASE_HNSW_M", "64")
	t.Setenv("YASE_METRICS_ADDR", ":8888")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Server.GRPCPort != 55555 {
		t.Errorf("expected env override grpc_port 55555, got %d", cfg.Server.GRPCPort)
	}
	if cfg.HNSW.M != 64 {
		t.Errorf("expected env override hnsw.m 64, got %d", cfg.HNSW.M)
	}
	if cfg.Metrics.Addr != ":8888" {
		t.Errorf("expected env override metrics.addr :8888, got %s", cfg.Metrics.Addr)
	}
}

func TestLoadEnvOverridesFile(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "test.yaml")

	yaml := `
server:
  grpc_port: 60000
`
	if err := os.WriteFile(configPath, []byte(yaml), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	// Env var should override the file value
	t.Setenv("YASE_SERVER_GRPC_PORT", "70000")

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Server.GRPCPort != 70000 {
		t.Errorf("expected env override 70000 over file 60000, got %d", cfg.Server.GRPCPort)
	}
}

func TestLoadInvalidFile(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("expected error for invalid config path")
	}
}
