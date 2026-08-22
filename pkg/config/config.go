package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config represents the full application configuration.
type Config struct {
	Server      ServerConfig            `mapstructure:"server"`
	Kafka       KafkaConfig             `mapstructure:"kafka"`
	Redis       RedisConfig             `mapstructure:"redis"`
	HNSW        HNSWConfig              `mapstructure:"hnsw"`
	Index       IndexConfig             `mapstructure:"index"`
	Crawler     CrawlerConfig           `mapstructure:"crawler"`
	Metrics     MetricsConfig           `mapstructure:"metrics"`
	Embedder    EmbedderConfig          `mapstructure:"embedder"`
	Reranker    RerankerConfig          `mapstructure:"reranker"`
	Auth        AuthConfig              `mapstructure:"auth"`
	TLS         TLSConfig               `mapstructure:"tls"`
	RateLimit   RateLimitConfig         `mapstructure:"rate_limit"`
	Connectors  ConnectorManagerConfig  `mapstructure:"connectors"`
	Collections CollectionManagerConfig `mapstructure:"collections"`
	Cluster     ClusterConfig           `mapstructure:"cluster"`
	Audit       AuditConfig             `mapstructure:"audit"`
	Logging     LoggingConfig           `mapstructure:"logging"`
	Tracing     TracingConfig           `mapstructure:"tracing"`
	Cache       CacheConfig             `mapstructure:"cache"`
}

// CacheConfig controls the search result LRU cache.
type CacheConfig struct {
	Enabled bool          `mapstructure:"enabled"`
	TTL     time.Duration `mapstructure:"ttl"`
	MaxSize int           `mapstructure:"max_size"`
}

// LoggingConfig controls structured (slog) logging.
type LoggingConfig struct {
	Format string `mapstructure:"format"` // "json" or "text" (default "json")
	Level  string `mapstructure:"level"`  // "debug", "info", "warn", "error"
	Output string `mapstructure:"output"` // "stderr", "stdout", or file path
}

// TracingConfig controls OpenTelemetry distributed tracing.
type TracingConfig struct {
	Enabled    bool    `mapstructure:"enabled"`
	Exporter   string  `mapstructure:"exporter"`    // "stdout" or "otlp"
	Endpoint   string  `mapstructure:"endpoint"`    // OTLP endpoint (default localhost:4318)
	SampleRate float64 `mapstructure:"sample_rate"` // 0.0-1.0 (default 0.1)
}

// AuditConfig controls audit event logging.
type AuditConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Output  string `mapstructure:"output"` // "stdout", "stderr", or file path
}

type ClusterConfig struct {
	NodeID    string `mapstructure:"node_id"`
	RaftAddr  string `mapstructure:"raft_addr"`
	RaftDir   string `mapstructure:"raft_dir"`
	Bootstrap bool   `mapstructure:"bootstrap"`
	JoinAddr  string `mapstructure:"join_addr"`  // leader address to join
	ShardPort int    `mapstructure:"shard_port"` // gRPC port for ShardSearchService
}

type ServerConfig struct {
	GRPCPort       int           `mapstructure:"grpc_port"`
	GRPCHost       string        `mapstructure:"grpc_host"`
	HTTPPort       int           `mapstructure:"http_port"`
	MaxRecvMsgSize int           `mapstructure:"max_recv_msg_size"`
	ShutdownGrace  time.Duration `mapstructure:"shutdown_grace"`
}

type KafkaConfig struct {
	Brokers           []string      `mapstructure:"brokers"`
	Topic             string        `mapstructure:"topic"`
	GroupID           string        `mapstructure:"group_id"`
	Partitions        int           `mapstructure:"partitions"`
	ReplicationFactor int           `mapstructure:"replication_factor"`
	BatchSize         int           `mapstructure:"batch_size"`
	BatchTimeout      time.Duration `mapstructure:"batch_timeout"`
	RequiredAcks      int           `mapstructure:"required_acks"`
	DLQTopic          string        `mapstructure:"dlq_topic"` // dead-letter topic ("" = disabled)
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
	PoolSize int    `mapstructure:"pool_size"`
}

type HNSWConfig struct {
	M              int `mapstructure:"m"`
	Mmax0          int `mapstructure:"mmax0"`
	EfConstruction int `mapstructure:"ef_construction"`
	EfSearch       int `mapstructure:"ef_search"`
}

type IndexConfig struct {
	Path         string `mapstructure:"path"`
	ArenaSize    uint64 `mapstructure:"arena_size_bytes"`
	CentroidRate int    `mapstructure:"centroid_rate"`
	GRPCHost     string `mapstructure:"grpc_host"`
	GRPCPort     int    `mapstructure:"grpc_port"`
}

type CrawlerConfig struct {
	MaxWorkers         int           `mapstructure:"max_workers"`
	RequestTimeout     time.Duration `mapstructure:"request_timeout"`
	RatePerDomain      int           `mapstructure:"rate_per_domain"`
	RateWindow         time.Duration `mapstructure:"rate_window"`
	HeartbeatInterval  time.Duration `mapstructure:"heartbeat_interval"`
	ReaperInterval     time.Duration `mapstructure:"reaper_interval"`
	ReaperTimeout      time.Duration `mapstructure:"reaper_timeout"`
	MaxRetries         int           `mapstructure:"max_retries"`
	AllowedDomains     []string      `mapstructure:"allowed_domains"`
	BlockedDomains     []string      `mapstructure:"blocked_domains"`
	AllowedDomainsFile string        `mapstructure:"allowed_domains_file"` // one domain per line
	BlockedDomainsFile string        `mapstructure:"blocked_domains_file"` // one domain per line
	MaxPages           int           `mapstructure:"max_pages"`
	DNSTTL             time.Duration `mapstructure:"dns_ttl"`

	// MasterAddr is the listen address for the master scheduler's HTTP API
	// (/seed, /discover, /assign, /heartbeat). Defaults to loopback: these are
	// management endpoints and must not be world-reachable.
	MasterAddr string `mapstructure:"master_addr"`

	// MasterToken is the shared secret workers and operators present to the
	// master scheduler API. Required unless MasterAddr is loopback-only.
	MasterToken string `mapstructure:"master_token"`

	// AllowPrivateAddresses disables the crawler's SSRF guard, permitting
	// fetches of private, loopback, and link-local addresses. Leave false
	// unless deliberately crawling an internal network.
	AllowPrivateAddresses bool `mapstructure:"allow_private_addresses"`
}

type MetricsConfig struct {
	Addr string `mapstructure:"addr"`
}

type EmbedderConfig struct {
	Provider  string        `mapstructure:"provider"`
	Model     string        `mapstructure:"model"`
	APIKey    string        `mapstructure:"api_key"`
	BaseURL   string        `mapstructure:"base_url"`
	Dimension int           `mapstructure:"dimension"`
	Timeout   time.Duration `mapstructure:"timeout"`
	CacheSize int           `mapstructure:"cache_size"`
}

type RerankerConfig struct {
	Enabled    bool          `mapstructure:"enabled"`
	Provider   string        `mapstructure:"provider"`
	Model      string        `mapstructure:"model"`
	BaseURL    string        `mapstructure:"base_url"`
	Timeout    time.Duration `mapstructure:"timeout"`
	Candidates int           `mapstructure:"candidates"` // how many RRF results to feed to reranker
}

type AuthConfig struct {
	Enabled bool           `mapstructure:"enabled"`
	Method  string         `mapstructure:"method"` // "api_key", "jwt", "none"
	APIKeys []APIKeyConfig `mapstructure:"api_keys"`
	JWT     JWTAuthConfig  `mapstructure:"jwt"`

	// ClientToken is the credential this process presents when it calls
	// another YASE service over gRPC (crawler→ingestion, gateway→shard,
	// parser→PDF). Required when auth is enabled in a distributed
	// deployment: without it the servers reject internal traffic.
	ClientToken string `mapstructure:"client_token"`
}

type APIKeyConfig struct {
	Key      string   `mapstructure:"key"`
	TenantID string   `mapstructure:"tenant_id"`
	UserID   string   `mapstructure:"user_id"`
	Roles    []string `mapstructure:"roles"`
}

type JWTAuthConfig struct {
	Secret      string `mapstructure:"secret"`
	Issuer      string `mapstructure:"issuer"`
	Audience    string `mapstructure:"audience"`
	TenantClaim string `mapstructure:"tenant_claim"`
	RolesClaim  string `mapstructure:"roles_claim"`

	// DefaultRoles are granted to a token carrying no roles claim. Empty by
	// default so such a token receives no authority.
	DefaultRoles []string `mapstructure:"default_roles"`

	// RequireTenant rejects tokens with no tenant claim. Enable it with
	// multi-tenancy: an empty tenant yields a context the tenant filter
	// cannot narrow.
	RequireTenant bool `mapstructure:"require_tenant"`
}

type TLSConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	CertPath   string `mapstructure:"cert_path"`
	KeyPath    string `mapstructure:"key_path"`
	CAPath     string `mapstructure:"ca_path"`
	ClientAuth bool   `mapstructure:"client_auth"`
}

type RateLimitConfig struct {
	Enabled          bool    `mapstructure:"enabled"`
	DefaultSearchQPS float64 `mapstructure:"default_search_qps"`
	DefaultIngestRPS float64 `mapstructure:"default_ingest_rps"`
}

// CollectionManagerConfig controls collection storage and resource limits.
type CollectionManagerConfig struct {
	BasePath          string `mapstructure:"base_path"`
	DefaultArena      uint64 `mapstructure:"default_arena"`
	MaxCollections    int    `mapstructure:"max_collections"`
	DefaultCollection string `mapstructure:"default_collection"`
}

type ConnectorManagerConfig struct {
	Enabled       bool   `mapstructure:"enabled"`
	ConfigPath    string `mapstructure:"config_path"`     // path to connectors.json
	StateBackend  string `mapstructure:"state_backend"`   // "file" or "redis"
	StateFilePath string `mapstructure:"state_file_path"` // for file backend
	HTTPPort      int    `mapstructure:"http_port"`       // management API port
}

// Load reads configuration from a YAML file with environment variable overrides.
// Environment variables follow the pattern YASE_KAFKA_BROKERS, YASE_SERVER_GRPC_PORT, etc.
// Priority: env vars > config file > defaults.
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Server defaults
	v.SetDefault("server.grpc_port", 50051)
	v.SetDefault("server.grpc_host", "localhost")
	v.SetDefault("server.http_port", 8000)
	v.SetDefault("server.max_recv_msg_size", 16*1024*1024) // 16MB
	v.SetDefault("server.shutdown_grace", "15s")

	// Kafka defaults
	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.topic", "crawl-records")
	v.SetDefault("kafka.group_id", "yase-indexer")
	v.SetDefault("kafka.partitions", 6)
	v.SetDefault("kafka.replication_factor", 1)
	v.SetDefault("kafka.batch_size", 500)
	v.SetDefault("kafka.batch_timeout", "10ms")
	v.SetDefault("kafka.required_acks", -1) // all ISR
	v.SetDefault("kafka.dlq_topic", "")

	// Redis defaults
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.pool_size", 2000)

	// HNSW defaults
	v.SetDefault("hnsw.m", 16)
	v.SetDefault("hnsw.mmax0", 32)
	v.SetDefault("hnsw.ef_construction", 200)
	v.SetDefault("hnsw.ef_search", 50)

	// Index defaults
	v.SetDefault("index.path", "/data/yase/bluge")
	v.SetDefault("index.arena_size_bytes", uint64(4*1024*1024*1024)) // 4GB
	v.SetDefault("index.centroid_rate", 5)                           // 20% centroids
	v.SetDefault("index.grpc_host", "localhost")
	v.SetDefault("index.grpc_port", 50052)

	// Crawler defaults
	v.SetDefault("crawler.max_workers", 2000)
	v.SetDefault("crawler.request_timeout", "15s")
	v.SetDefault("crawler.rate_per_domain", 1)
	v.SetDefault("crawler.rate_window", "1s")
	v.SetDefault("crawler.heartbeat_interval", "3s")
	v.SetDefault("crawler.reaper_interval", "4s")
	v.SetDefault("crawler.reaper_timeout", "9s")
	v.SetDefault("crawler.max_retries", 3)
	v.SetDefault("crawler.max_pages", 0) // 0 = unlimited
	v.SetDefault("crawler.dns_ttl", "5m")
	v.SetDefault("crawler.master_addr", "127.0.0.1:9080")
	v.SetDefault("crawler.allow_private_addresses", false)

	// Reranker defaults
	v.SetDefault("reranker.enabled", false)
	v.SetDefault("reranker.provider", "tei")
	v.SetDefault("reranker.model", "Alibaba-NLP/gte-reranker-modernbert-base")
	v.SetDefault("reranker.base_url", "http://localhost:8081")
	v.SetDefault("reranker.timeout", "2s")
	v.SetDefault("reranker.candidates", 50)

	// Auth defaults
	v.SetDefault("auth.enabled", false)
	v.SetDefault("auth.method", "api_key")
	v.SetDefault("tls.enabled", false)
	v.SetDefault("rate_limit.enabled", false)
	v.SetDefault("rate_limit.default_search_qps", 100)
	v.SetDefault("rate_limit.default_ingest_rps", 50)

	// Collection defaults
	v.SetDefault("collections.base_path", "/data/yase/collections")
	v.SetDefault("collections.default_arena", uint64(1<<30)) // 1GB per collection
	v.SetDefault("collections.max_collections", 100)
	v.SetDefault("collections.default_collection", "_default")

	// Connector manager defaults
	v.SetDefault("connectors.enabled", false)
	v.SetDefault("connectors.config_path", "configs/connectors.json")
	v.SetDefault("connectors.state_backend", "file")
	v.SetDefault("connectors.state_file_path", "/tmp/yase-connector-state")
	v.SetDefault("connectors.http_port", 9300)

	// Cluster defaults
	v.SetDefault("cluster.node_id", "node-0")
	v.SetDefault("cluster.raft_addr", "localhost:7000")
	v.SetDefault("cluster.raft_dir", "/data/yase/raft")
	v.SetDefault("cluster.bootstrap", false)
	v.SetDefault("cluster.join_addr", "")
	v.SetDefault("cluster.shard_port", 50053)

	// Audit defaults
	v.SetDefault("audit.enabled", false)
	v.SetDefault("audit.output", "stderr")

	// Logging defaults
	v.SetDefault("logging.format", "json")
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.output", "stderr")

	// Tracing defaults
	v.SetDefault("tracing.enabled", false)
	v.SetDefault("tracing.exporter", "stdout")
	v.SetDefault("tracing.endpoint", "")
	v.SetDefault("tracing.sample_rate", 0.1)

	// Cache defaults
	v.SetDefault("cache.enabled", true)
	v.SetDefault("cache.ttl", "5m")
	v.SetDefault("cache.max_size", 10000)

	// Metrics defaults
	v.SetDefault("metrics.addr", ":9090")

	// Embedder defaults
	v.SetDefault("embedder.provider", "tei")
	v.SetDefault("embedder.model", "Alibaba-NLP/gte-modernbert-base")
	v.SetDefault("embedder.base_url", "http://localhost:8888")
	v.SetDefault("embedder.dimension", 768)
	v.SetDefault("embedder.timeout", "30s")
	v.SetDefault("embedder.cache_size", 100000)

	// Read YAML config file
	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		v.SetConfigName("default")
		v.SetConfigType("yaml")
		v.AddConfigPath("./configs")
		v.AddConfigPath("/etc/yase")
	}

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("read config: %w", err)
		}
		// Config file not found is OK if env vars provide everything
	}

	// Environment variable overrides: YASE_SERVER_GRPC_PORT overrides server.grpc_port
	v.SetEnvPrefix("YASE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	return &cfg, nil
}
