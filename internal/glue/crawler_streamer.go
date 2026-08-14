package glue

import (
	"context"
	"log/slog"
	"time"

	"github.com/efathom/yase/internal/coordinator"
	"github.com/efathom/yase/pkg/client"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"google.golang.org/grpc"
)

// StreamToIngestion consumes CrawlResults from the crawler and sends them
// to the gRPC ingestion service via the connection pool. When the ingestion
// service is saturated, gRPC blocks → TCP window fills → crawler pauses
// organically (physical backpressure).
func StreamToIngestion(ctx context.Context, results <-chan coordinator.CrawlResult, pool *client.ConnectionPool) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result, ok := <-results:
			if !ok {
				return nil // Channel closed — all results consumed
			}
			if result.Error != nil {
				continue // Skip failed crawls
			}

			record := &ingestionv1.CrawlRecord{
				Url:           result.SourceURL,
				RawContent:    []byte(result.Markdown),
				Metadata:      map[string]string{"source": "distributed_crawler"},
				CrawledAtUnix: time.Now().Unix(),
			}

			err := pool.Execute(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
				client := ingestionv1.NewIngestionServiceClient(conn)
				_, err := client.IngestBatch(ctx, &ingestionv1.BatchIngestRequest{
					Records: []*ingestionv1.CrawlRecord{record},
				})
				return err
			})
			if err != nil {
				slog.Warn("streamer: ingestion error", "url", result.SourceURL, "error", err)
			}
		}
	}
}
