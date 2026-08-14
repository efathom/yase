// cmd/connector runs the YASE Connector Manager service.
// It loads connector configurations, runs the sync scheduler, and exposes
// an HTTP management API for monitoring and triggering syncs.
//
// Usage:
//
//	connector --config configs/default.yaml --connectors configs/connectors.json
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/efathom/yase/internal/broker"
	"github.com/efathom/yase/internal/glue"
	"github.com/efathom/yase/pkg/config"
	"github.com/efathom/yase/pkg/connector"
	"github.com/efathom/yase/pkg/logging"
	"github.com/efathom/yase/pkg/metrics"

	// Register connectors via init()
	_ "github.com/efathom/yase/internal/connector/confluence"
	_ "github.com/efathom/yase/internal/connector/gdrive"
	_ "github.com/efathom/yase/internal/connector/jira"
	_ "github.com/efathom/yase/internal/connector/mysql"
	_ "github.com/efathom/yase/internal/connector/postgres"
	_ "github.com/efathom/yase/internal/connector/s3store"
	_ "github.com/efathom/yase/internal/connector/salesforce"
	_ "github.com/efathom/yase/internal/connector/sharepoint"
	_ "github.com/efathom/yase/internal/connector/slack"
	_ "github.com/efathom/yase/internal/connector/twitter"
)

func main() {
	configPath := flag.String("config", "", "Path to YASE config file")
	connectorsPath := flag.String("connectors", "configs/connectors.json", "Path to connectors config file (JSON)")
	httpPort := flag.Int("http-port", 9300, "HTTP port for management API")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		slog.Error("Invalid config", "error", err)
		os.Exit(1)
	}

	logging.Setup(logging.Config{Format: cfg.Logging.Format, Level: cfg.Logging.Level, Output: cfg.Logging.Output})
	defer logging.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Metrics
	go func() {
		if err := metrics.ServeMetrics(cfg.Metrics.Addr); err != nil {
			slog.Error("Metrics error", "error", err)
		}
	}()

	// Kafka producer (connectors output to the same topic as crawler)
	producer := broker.NewKafkaProducer(cfg.Kafka)
	defer producer.Close()

	// State store
	stateStore, err := connector.NewFileStateStore("/tmp/yase-connector-state")
	if err != nil {
		slog.Error("State store", "error", err)
		os.Exit(1)
	}

	// Bridge: connector records → Kafka
	bridge := glue.NewConnectorBridge(producer)

	// Scheduler
	scheduler := connector.NewScheduler(connector.DefaultRegistry, stateStore, bridge.Sink())

	// Load connector configurations
	if err := loadConnectorConfigs(*connectorsPath, scheduler); err != nil {
		slog.Error("Load connectors", "error", err)
		os.Exit(1)
	}

	// HTTP management API
	mux := http.NewServeMux()
	registerManagementAPI(mux, scheduler)

	addr := fmt.Sprintf("127.0.0.1:%d", *httpPort) // bind to localhost only for security
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		slog.Info("Connector manager HTTP API", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP", "error", err)
			os.Exit(1)
		}
	}()

	// Start scheduler
	go func() {
		slog.Info("Connector scheduler started", "jobs", len(scheduler.ListJobs()))
		_ = scheduler.Start(ctx)
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("Shutting down connector manager")
	cancel()
	scheduler.Stop()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	_ = srv.Shutdown(shutCtx)
	slog.Info("Connector manager shut down")
}

// connectorFileConfig represents one connector entry in connectors.yaml
type connectorFileConfig struct {
	ID       string                 `json:"id" yaml:"id"`
	Type     string                 `json:"type" yaml:"type"`
	Schedule string                 `json:"schedule" yaml:"schedule"`
	SyncMode string                 `json:"sync_mode" yaml:"sync_mode"`
	Auth     *connector.AuthConfig  `json:"auth" yaml:"auth"`
	Config   map[string]interface{} `json:"config" yaml:"config"`
	Streams  []string               `json:"streams" yaml:"streams"`
}

type connectorsFile struct {
	Connectors []connectorFileConfig `json:"connectors" yaml:"connectors"`
}

func loadConnectorConfigs(path string, scheduler *connector.Scheduler) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Info("No connectors config, starting with no jobs", "path", path)
			return nil
		}
		return err
	}

	var file connectorsFile
	if err := json.Unmarshal(data, &file); err != nil {
		// Try YAML-style parsing via simple JSON conversion
		// For now, just use JSON
		return fmt.Errorf("parse connectors file: %w", err)
	}

	for _, entry := range file.Connectors {
		mode := connector.Incremental
		if entry.SyncMode == "full_refresh" {
			mode = connector.FullRefresh
		}

		// Build configured streams
		var streams []connector.ConfiguredStream
		if len(entry.Streams) > 0 {
			for _, s := range entry.Streams {
				streams = append(streams, connector.ConfiguredStream{
					Name:     s,
					SyncMode: mode,
				})
			}
		}

		job := &connector.SyncJob{
			ID: entry.ID,
			ConnectorCfg: connector.ConnectorConfig{
				Type:   entry.Type,
				Config: entry.Config,
				Auth:   entry.Auth,
			},
			Streams:  streams,
			Schedule: entry.Schedule,
			Mode:     mode,
			Status:   connector.JobIdle,
		}

		if err := scheduler.Add(job); err != nil {
			slog.Error("Failed to add connector job", "job_id", entry.ID, "error", err)
		} else {
			slog.Info("Added connector job", "job_id", entry.ID, "type", entry.Type, "schedule", entry.Schedule)
		}
	}

	return nil
}

func registerManagementAPI(mux *http.ServeMux, scheduler *connector.Scheduler) {
	// List registered connector types
	mux.HandleFunc("GET /connectors", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]interface{}{
			"types": connector.DefaultRegistry.List(),
		})
	})

	// List all jobs (redact credentials)
	mux.HandleFunc("GET /jobs", func(w http.ResponseWriter, r *http.Request) {
		jobs := scheduler.ListJobs()
		redacted := make([]map[string]interface{}, len(jobs))
		for i := range jobs {
			redacted[i] = jobSummary(&jobs[i])
		}
		writeJSON(w, 200, redacted)
	})

	// Trigger a job
	mux.HandleFunc("POST /jobs/{id}/trigger", func(w http.ResponseWriter, r *http.Request) {
		jobID := r.PathValue("id")
		if err := scheduler.Trigger(jobID); err != nil {
			code := 400
			if errors.Is(err, connector.ErrJobRunning) {
				code = 409
			}
			writeJSON(w, code, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]string{"status": "triggered", "job_id": jobID})
	})

	// Job status (redact credentials)
	mux.HandleFunc("GET /jobs/{id}/status", func(w http.ResponseWriter, r *http.Request) {
		jobID := r.PathValue("id")
		job, err := scheduler.Status(jobID)
		if err != nil || job == nil {
			writeJSON(w, 404, map[string]string{"error": "job not found"})
			return
		}
		writeJSON(w, 200, jobSummary(job))
	})

	// Health
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
}

// jobSummary returns a credential-free view of a sync job for API responses.
func jobSummary(j *connector.SyncJob) map[string]interface{} {
	return map[string]interface{}{
		"id":           j.ID,
		"type":         j.ConnectorCfg.Type,
		"schedule":     j.Schedule,
		"mode":         j.Mode,
		"status":       j.Status,
		"last_sync_at": j.LastSyncAt,
		"last_error":   j.LastError,
		"records_read": j.RecordsRead,
	}
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
