package glue

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/efathom/yase/internal/coordinator"
	"github.com/efathom/yase/pkg/client"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// collectingServer captures BatchIngest requests for assertions.
type collectingServer struct {
	ingestionv1.UnimplementedIngestionServiceServer
	mu      sync.Mutex
	records []*ingestionv1.CrawlRecord
}

func (s *collectingServer) IngestBatch(_ context.Context, req *ingestionv1.BatchIngestRequest) (*ingestionv1.BatchIngestResponse, error) {
	s.mu.Lock()
	s.records = append(s.records, req.Records...)
	s.mu.Unlock()
	return &ingestionv1.BatchIngestResponse{
		ProcessedCount: int32(len(req.Records)),
		Status:         "SUCCESS",
	}, nil
}

func TestStreamToIngestion(t *testing.T) {
	// Start a real gRPC server with collecting handler
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	collector := &collectingServer{}
	ingestionv1.RegisterIngestionServiceServer(srv, collector)
	go srv.Serve(lis)
	defer srv.GracefulStop()

	pool, err := client.NewConnectionPool(lis.Addr().String(), 2,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	results := make(chan coordinator.CrawlResult, 3)
	results <- coordinator.CrawlResult{
		SourceURL: "https://example.com/1",
		Markdown:  "# Page 1\nContent here",
	}
	results <- coordinator.CrawlResult{
		Error: fmt.Errorf("fetch failed"), // Should be skipped
	}
	results <- coordinator.CrawlResult{
		SourceURL: "https://example.com/2",
		Markdown:  "# Page 2\nMore content",
	}
	close(results)

	err = StreamToIngestion(context.Background(), results, pool)
	if err != nil {
		t.Fatalf("streamer error: %v", err)
	}

	collector.mu.Lock()
	defer collector.mu.Unlock()
	if len(collector.records) != 2 {
		t.Errorf("expected 2 records (1 skipped error), got %d", len(collector.records))
	}
}

func TestStreamToIngestionCancellation(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	collector := &collectingServer{}
	ingestionv1.RegisterIngestionServiceServer(srv, collector)
	go srv.Serve(lis)
	defer srv.GracefulStop()

	pool, err := client.NewConnectionPool(lis.Addr().String(), 1,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	results := make(chan coordinator.CrawlResult) // Unbuffered — will block

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err = StreamToIngestion(ctx, results, pool)
	if err == nil {
		t.Error("expected context cancellation error")
	}
}
