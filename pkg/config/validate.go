package config

import (
	"errors"
	"fmt"
)

// knownEmbedderProviders are the valid embedder.provider values.
var knownEmbedderProviders = map[string]bool{
	"mock":   true,
	"ollama": true,
	"tei":    true,
	"openai": true,
}

func validatePort(name string, port int, errs *[]error) {
	if port <= 0 || port > 65535 {
		*errs = append(*errs, fmt.Errorf("%s: invalid port %d", name, port))
	}
}

// Validate checks the configuration for invalid or missing values.
// Returns an error describing all validation failures.
func (c *Config) Validate() error {
	var errs []error

	// Server
	validatePort("server.grpc_port", c.Server.GRPCPort, &errs)
	validatePort("server.http_port", c.Server.HTTPPort, &errs)
	if c.Server.ShutdownGrace <= 0 {
		errs = append(errs, fmt.Errorf("server.shutdown_grace: must be > 0"))
	}

	// Kafka
	if len(c.Kafka.Brokers) == 0 {
		errs = append(errs, fmt.Errorf("kafka.brokers: must contain at least one broker"))
	}
	if c.Kafka.Topic == "" {
		errs = append(errs, fmt.Errorf("kafka.topic: must not be empty"))
	}
	if c.Kafka.Partitions <= 0 {
		errs = append(errs, fmt.Errorf("kafka.partitions: must be > 0"))
	}
	if c.Kafka.ReplicationFactor <= 0 {
		errs = append(errs, fmt.Errorf("kafka.replication_factor: must be > 0"))
	}

	// Redis
	if c.Redis.Addr == "" {
		errs = append(errs, fmt.Errorf("redis.addr: must not be empty"))
	}

	// Embedder
	if c.Embedder.Dimension <= 0 {
		errs = append(errs, fmt.Errorf("embedder.dimension: must be > 0, got %d", c.Embedder.Dimension))
	}
	if c.Embedder.Provider == "" {
		errs = append(errs, fmt.Errorf("embedder.provider: must not be empty"))
	} else if !knownEmbedderProviders[c.Embedder.Provider] {
		errs = append(errs, fmt.Errorf("embedder.provider: unknown provider %q", c.Embedder.Provider))
	}

	// Index
	if c.Index.ArenaSize == 0 {
		errs = append(errs, fmt.Errorf("index.arena_size_bytes: must be > 0"))
	}
	if c.Index.CentroidRate <= 0 {
		errs = append(errs, fmt.Errorf("index.centroid_rate: must be > 0, got %d", c.Index.CentroidRate))
	}
	validatePort("index.grpc_port", c.Index.GRPCPort, &errs)

	// HNSW
	if c.HNSW.M < 2 {
		errs = append(errs, fmt.Errorf("hnsw.m: must be >= 2, got %d", c.HNSW.M))
	}
	if c.HNSW.Mmax0 < c.HNSW.M {
		errs = append(errs, fmt.Errorf("hnsw.mmax0: must be >= m (%d), got %d", c.HNSW.M, c.HNSW.Mmax0))
	}
	if c.HNSW.EfConstruction <= 0 {
		errs = append(errs, fmt.Errorf("hnsw.ef_construction: must be > 0, got %d", c.HNSW.EfConstruction))
	}
	if c.HNSW.EfSearch <= 0 {
		errs = append(errs, fmt.Errorf("hnsw.ef_search: must be > 0, got %d", c.HNSW.EfSearch))
	}

	// Crawler
	if c.Crawler.MaxWorkers <= 0 {
		errs = append(errs, fmt.Errorf("crawler.max_workers: must be > 0, got %d", c.Crawler.MaxWorkers))
	}
	if c.Crawler.MaxRetries < 0 {
		errs = append(errs, fmt.Errorf("crawler.max_retries: must be >= 0"))
	}

	// TLS
	if c.TLS.Enabled {
		if c.TLS.CertPath == "" {
			errs = append(errs, fmt.Errorf("tls.cert_path: required when TLS enabled"))
		}
		if c.TLS.KeyPath == "" {
			errs = append(errs, fmt.Errorf("tls.key_path: required when TLS enabled"))
		}
	}

	// Auth
	if c.Auth.Enabled {
		switch c.Auth.Method {
		case "api_key":
			if len(c.Auth.APIKeys) == 0 {
				errs = append(errs, fmt.Errorf("auth.api_keys: at least one API key required when method=api_key"))
			}
		case "jwt":
			if c.Auth.JWT.Secret == "" {
				errs = append(errs, fmt.Errorf("auth.jwt.secret: required when method=jwt"))
			}
		case "none", "":
			// ok
		default:
			errs = append(errs, fmt.Errorf("auth.method: unknown method %q", c.Auth.Method))
		}
	}

	// Reranker
	if c.Reranker.Enabled {
		if c.Reranker.Candidates <= 0 {
			errs = append(errs, fmt.Errorf("reranker.candidates: must be > 0 when enabled"))
		}
		if c.Reranker.Provider == "" {
			errs = append(errs, fmt.Errorf("reranker.provider: must not be empty when enabled"))
		}
	}

	// Collections
	if c.Collections.BasePath == "" {
		errs = append(errs, fmt.Errorf("collections.base_path: must not be empty"))
	}
	if c.Collections.DefaultCollection == "" {
		errs = append(errs, fmt.Errorf("collections.default_collection: must not be empty"))
	}

	// Cluster
	validatePort("cluster.shard_port", c.Cluster.ShardPort, &errs)

	// Connectors
	if c.Connectors.HTTPPort != 0 {
		validatePort("connectors.http_port", c.Connectors.HTTPPort, &errs)
	}

	return errors.Join(errs...)
}
