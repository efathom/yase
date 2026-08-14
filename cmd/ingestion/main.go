package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/efathom/yase/internal/broker"
	"github.com/efathom/yase/internal/idempotency"
	"github.com/efathom/yase/internal/server"
	"github.com/efathom/yase/internal/worker"
	"github.com/efathom/yase/pkg/auth"
	"github.com/efathom/yase/pkg/config"
	"github.com/efathom/yase/pkg/logging"
	"github.com/efathom/yase/pkg/metrics"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
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

	// Start Prometheus metrics endpoint
	go func() {
		if err := metrics.ServeMetrics(cfg.Metrics.Addr); err != nil {
			slog.Error("Metrics server error", "error", err)
		}
	}()

	// Ensure Kafka topic exists with the configured partition count
	if err := broker.EnsureTopic(ctx, cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.Partitions, cfg.Kafka.ReplicationFactor); err != nil {
		slog.Warn("ensure topic (may already exist)", "error", err)
	}

	// Initialize Kafka producer
	kafkaProducer := broker.NewKafkaProducer(cfg.Kafka)

	// Initialize worker pool with backpressure
	pool := worker.NewPool(0, 1024, kafkaProducer)
	pool.Start()

	// Initialize Redis-backed idempotency manager
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
		PoolSize: cfg.Redis.PoolSize,
	})
	idemp := idempotency.NewManager(rdb)

	// gRPC server configuration
	opts := []grpc.ServerOption{
		grpc.ReadBufferSize(16 * 1024),
		grpc.WriteBufferSize(16 * 1024),
		grpc.MaxRecvMsgSize(cfg.Server.MaxRecvMsgSize),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Minute,
			PermitWithoutStream: true,
		}),
	}

	authOpts, err := auth.ServerAuthTLS(cfg.Auth, cfg.TLS)
	if err != nil {
		slog.Error("failed to configure auth/TLS", "error", err)
		os.Exit(1)
	}
	opts = append(opts, authOpts...)

	grpcServer := grpc.NewServer(opts...)

	// Register health check service for k8s probes
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("yase.v1.IngestionService", healthpb.HealthCheckResponse_SERVING)

	// Register ingestion service handler
	handler := server.NewIngestionHandler(pool, idemp)
	ingestionv1.RegisterIngestionServiceServer(grpcServer, handler)

	lisAddr := fmt.Sprintf(":%d", cfg.Server.GRPCPort)
	lis, err := net.Listen("tcp", lisAddr)
	if err != nil {
		slog.Error("Failed to listen", "addr", lisAddr, "error", err)
		os.Exit(1)
	}

	go func() {
		slog.Info("gRPC ingestion server listening", "addr", lisAddr)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC serve error", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		slog.Info("Received signal, initiating graceful shutdown", "signal", sig)
	case <-ctx.Done():
	}

	// Mark as NOT_SERVING so k8s readiness probe fails and LB drains traffic
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthServer.SetServingStatus("yase.v1.IngestionService", healthpb.HealthCheckResponse_NOT_SERVING)

	gracefulStop(grpcServer)
	_ = pool.Stop()
	rdb.Close()
	cancel()
	slog.Info("Ingestion server shut down cleanly")
}

// gracefulStop stops the gRPC server, falling back to a hard Stop after a
// bounded grace period so a held stream cannot block shutdown indefinitely.
func gracefulStop(srv *grpc.Server) {
	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		srv.Stop()
	}
}
