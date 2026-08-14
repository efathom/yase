package glue

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/efathom/yase/internal/broker"
	"github.com/efathom/yase/pkg/connector"
	ingestionv1 "github.com/efathom/yase/proto/v1"
)

// ConnectorBridge converts connector Records to CrawlRecords and produces
// them to Kafka, bridging the connector system to the existing indexing pipeline.
type ConnectorBridge struct {
	Producer *broker.KafkaProducer
}

// NewConnectorBridge creates a bridge that feeds connector output to Kafka.
func NewConnectorBridge(producer *broker.KafkaProducer) *ConnectorBridge {
	return &ConnectorBridge{Producer: producer}
}

// Sink returns a connector.RecordSink function for use with the Scheduler.
// It surfaces the first connector error so the scheduler can mark the job
// failed instead of reporting false success.
func (b *ConnectorBridge) Sink() connector.RecordSink {
	return func(ctx context.Context, records <-chan connector.Record, errs <-chan error) error {
		var (
			errMu    sync.Mutex
			firstErr error
			errWg    sync.WaitGroup
		)

		// Drain connector errors in the background and capture the first one.
		errWg.Add(1)
		go func() {
			defer errWg.Done()
			for err := range errs {
				slog.Error("connector-bridge: record error", "error", err)
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}()

		for record := range records {
			if record.Action == connector.Delete {
				slog.Warn("connector-bridge: delete not yet supported", "record_id", record.ID)
				continue
			}

			// Build CrawlRecord from connector Record
			metadata := record.Metadata
			if metadata == nil {
				metadata = make(map[string]string)
			}
			metadata["_connector_stream"] = record.StreamName
			metadata["_connector_doc_id"] = record.ID
			if record.MimeType != "" {
				metadata["_mime_type"] = record.MimeType
			}

			crawlRecord := &ingestionv1.CrawlRecord{
				Url:           record.URL,
				RawContent:    record.Content,
				Metadata:      metadata,
				CrawledAtUnix: record.EmittedAt.Unix(),
			}
			if crawlRecord.CrawledAtUnix == 0 {
				crawlRecord.CrawledAtUnix = time.Now().Unix()
			}

			if err := b.Producer.Produce(ctx, crawlRecord); err != nil {
				slog.Error("connector-bridge: kafka produce error", "url", record.URL, "error", err)
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				continue
			}
		}

		errWg.Wait()
		errMu.Lock()
		defer errMu.Unlock()
		return firstErr
	}
}
