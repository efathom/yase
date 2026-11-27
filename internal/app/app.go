// Package app holds process wiring shared by the YASE binaries: HTTP servers
// with sane timeouts and optional TLS, the indexing consumer, collection
// maintenance, and health endpoints.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/efathom/yase/internal/broker"
	"github.com/efathom/yase/internal/glue"
	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/collection"
	"github.com/efathom/yase/pkg/config"
	"github.com/efathom/yase/pkg/index"
	"github.com/redis/go-redis/v9"
)

// NewHTTPServer returns a server with read/write/idle timeouts so slow or
// idle clients cannot hold connections open indefinitely.
func NewHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// ListenAndServe serves srv over TLS when tls.enabled, plain HTTP otherwise.
// It returns nil on graceful shutdown.
func ListenAndServe(srv *http.Server, tlsCfg config.TLSConfig) error {
	var err error
	if tlsCfg.Enabled {
		conf, lerr := auth.LoadTLSConfig(auth.TLSConfig{
			CertPath: tlsCfg.CertPath, KeyPath: tlsCfg.KeyPath, CAPath: tlsCfg.CAPath,
			ClientAuth: tlsCfg.ClientAuth, Enabled: true,
		})
		if lerr != nil {
			return fmt.Errorf("load TLS config: %w", lerr)
		}
		srv.TLSConfig = conf
		err = srv.ListenAndServeTLS("", "")
	} else {
		err = srv.ListenAndServe()
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// NewIndexingDaemon builds the Kafka consumer. When checkpoint is non-nil,
// offsets are committed only after it durably persists the index.
func NewIndexingDaemon(cfg config.KafkaConfig, handler glue.MessageHandler, checkpoint func(context.Context) error) *glue.IndexingDaemon {
	d := glue.NewIndexingDaemon(cfg.Brokers, cfg.Topic, cfg.GroupID, handler)
	if cfg.DLQTopic != "" {
		d.SetDLQ(broker.NewDLQWriter(cfg.Brokers, cfg.DLQTopic))
	} else {
		slog.Warn("kafka.dlq_topic is not set: messages that exhaust retries will be skipped after logging")
	}
	d.SetOptions(glue.DaemonOptions{
		Workers:        cfg.Workers,
		MaxAttempts:    cfg.MaxAttempts,
		CommitInterval: cfg.CommitInterval,
		Checkpoint:     checkpoint,
	})
	return d
}

// Maintenance converts index config into a collection maintenance config.
func Maintenance(cfg config.IndexConfig) collection.MaintenanceConfig {
	if cfg.FlatSearchMax > 0 {
		index.FlatSearchThreshold = cfg.FlatSearchMax
	}
	return collection.MaintenanceConfig{
		CheckpointInterval: cfg.CheckpointInterval,
		CompactDeadRatio:   cfg.CompactDeadRatio,
		TTL:                index.TTLConfig{SweepInterval: cfg.TTLSweepInterval, DefaultTTL: cfg.DefaultTTL},
	}
}

// HealthHandler serves /health (process alive) and /ready (ready reports nil).
func HealthHandler(ready func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := ready(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, `{"status":"not_ready","reason":%q}`, err.Error())
			return
		}
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	return mux
}

// ServeHealth runs the health endpoint on port (0 disables it).
func ServeHealth(port int, ready func(context.Context) error) *http.Server {
	if port == 0 {
		return nil
	}
	srv := NewHTTPServer(fmt.Sprintf(":%d", port), HealthHandler(ready))
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("health server error", "error", err)
		}
	}()
	return srv
}

// RateLimiter builds the configured rate limiter, or nil when disabled.
func RateLimiter(cfg *config.Config) *auth.RateLimiter {
	if !cfg.RateLimit.Enabled {
		return nil
	}
	rl := auth.NewRateLimiter(auth.RateLimitConfig{
		Enabled:          true,
		DefaultSearchQPS: cfg.RateLimit.DefaultSearchQPS,
		DefaultIngestRPS: cfg.RateLimit.DefaultIngestRPS,
	})
	if cfg.RateLimit.Backend == "redis" {
		rl.WithRedis(redis.NewClient(&redis.Options{
			Addr:        cfg.Redis.Addr,
			Password:    cfg.Redis.Password,
			DB:          cfg.Redis.DB,
			DialTimeout: 500 * time.Millisecond,
			ReadTimeout: 200 * time.Millisecond,
		}))
	}
	return rl
}
