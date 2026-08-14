package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"

	"github.com/efathom/yase/internal/idempotency"
	"github.com/efathom/yase/internal/worker"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IngestionHandler implements the IngestionServiceServer gRPC interface.
type IngestionHandler struct {
	ingestionv1.UnimplementedIngestionServiceServer
	pool  *worker.Pool
	idemp *idempotency.Manager
}

// NewIngestionHandler creates a handler wired to the worker pool and idempotency manager.
// idemp may be nil to disable idempotency checks.
func NewIngestionHandler(pool *worker.Pool, idemp *idempotency.Manager) *IngestionHandler {
	return &IngestionHandler{pool: pool, idemp: idemp}
}

// IngestBatch processes a batch of CrawlRecords as a unary RPC.
func (h *IngestionHandler) IngestBatch(ctx context.Context, req *ingestionv1.BatchIngestRequest) (*ingestionv1.BatchIngestResponse, error) {
	if req == nil || len(req.Records) == 0 {
		return &ingestionv1.BatchIngestResponse{ProcessedCount: 0, Status: "EMPTY"}, nil
	}

	// Idempotency check
	var idemKey string
	if h.idemp != nil {
		key, dup, err := h.idemp.CheckAndLock(ctx)
		if err != nil {
			return nil, err
		}
		if dup {
			// Replay the original processed count from the completion marker.
			count := int32(0)
			if result, err := h.idemp.GetResult(ctx, key); err == nil && result != "" {
				if c, perr := strconv.Atoi(result); perr == nil {
					count = int32(c)
				}
			}
			return &ingestionv1.BatchIngestResponse{
				ProcessedCount: count,
				Status:         "DUPLICATE",
			}, nil
		}
		idemKey = key
	}

	var submitted int32
	for _, record := range req.Records {
		if err := h.pool.Submit(ctx, record, nil); err != nil {
			// Release the idempotency lock so the client can retry.
			if h.idemp != nil {
				h.idemp.Release(ctx, idemKey)
			}
			return &ingestionv1.BatchIngestResponse{
				ProcessedCount: submitted,
				Status:         "PARTIAL",
			}, status.Errorf(codes.DeadlineExceeded, "context expired after %d records: %v", submitted, err)
		}
		submitted++
	}

	// Mark complete so late retries are deduplicated with the result.
	if h.idemp != nil {
		h.idemp.Complete(ctx, idemKey, fmt.Sprintf("%d", submitted))
	}

	return &ingestionv1.BatchIngestResponse{
		ProcessedCount: submitted,
		Status:         "SUCCESS",
	}, nil
}

// IngestStream handles bidirectional streaming ingestion.
// A dedicated sender goroutine writes acks back to the client,
// ensuring thread-safe access to stream.Send.
func (h *IngestionHandler) IngestStream(stream grpc.BidiStreamingServer[ingestionv1.StreamIngestRequest, ingestionv1.StreamIngestResponse]) error {
	ackCh := make(chan worker.Ack, 100)

	// done is closed exactly once when the stream terminates, signalling the
	// sender goroutine to stop. ackCh is intentionally never closed so that
	// in-flight workers sending acks never panic on a closed channel.
	done := make(chan struct{})
	var doneOnce sync.Once
	stop := func() { doneOnce.Do(func() { close(done) }) }

	// Sender goroutine: sole writer to stream.Send
	senderErr := make(chan error, 1)
	go func() {
		defer close(senderErr)
		for {
			select {
			case <-done:
				return
			case ack := <-ackCh:
				resp := &ingestionv1.StreamIngestResponse{
					Url:    ack.URL,
					Status: ack.Status,
				}
				if err := stream.Send(resp); err != nil {
					senderErr <- err
					stop()
					return
				}
			}
		}
	}()

	// Receive loop
	for {
		// Fail-fast if sender has errored
		select {
		case err := <-senderErr:
			if err != nil {
				stop()
				return status.Errorf(codes.Internal, "stream send error: %v", err)
			}
		default:
		}

		req, err := stream.Recv()
		if err == io.EOF {
			stop()
			return nil
		}
		if err != nil {
			stop()
			return err
		}

		record := req.GetRecord()
		if record == nil {
			continue
		}

		if submitErr := h.pool.Submit(stream.Context(), record, ackCh); submitErr != nil {
			slog.Error("stream submit error", "url", record.GetUrl(), "error", submitErr)
			stop()
			return status.Errorf(codes.ResourceExhausted, "backpressure: %v", submitErr)
		}
	}
}
