package glue

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/efathom/yase/pkg/metrics"
	"github.com/segmentio/kafka-go"
)

// maxRedeliveries is the number of times a message may fail before it is
// committed (skipped) to prevent a poison message from stalling the consumer.
const maxRedeliveries = 3

// MessageHandler processes a single Kafka message. Return nil to commit,
// return error to skip (message will be re-delivered).
type MessageHandler func(ctx context.Context, msg kafka.Message) error

// IndexingDaemon consumes from Kafka with at-least-once semantics using
// manual offset commits. Auto-commit is disabled.
type IndexingDaemon struct {
	reader  *kafka.Reader
	handler MessageHandler
	topic   string

	mu        sync.Mutex
	retries   map[int64]int // offset → failure count
	dlq       *kafka.Writer // optional dead-letter topic writer
	closeOnce sync.Once
	closeErr  error
}

// NewIndexingDaemon creates a Kafka consumer configured for at-least-once
// processing with manual commit.
func NewIndexingDaemon(brokers []string, topic, groupID string, handler MessageHandler) *IndexingDaemon {
	return &IndexingDaemon{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        groupID,
			MinBytes:       1,
			MaxBytes:       10 * 1024 * 1024, // 10MB
			MaxWait:        500 * time.Millisecond,
			StartOffset:    kafka.FirstOffset,
			CommitInterval: 0, // Manual commit only
		}),
		handler: handler,
		topic:   topic,
		retries: make(map[int64]int),
	}
}

// SetDLQ attaches an optional dead-letter topic writer. When set, poison
// messages that exceed maxRedeliveries are written to the DLQ before being
// skipped, instead of being silently dropped.
func (d *IndexingDaemon) SetDLQ(w *kafka.Writer) {
	d.dlq = w
}

// Start runs the consumer loop. Blocks until ctx is cancelled or a fatal error.
func (d *IndexingDaemon) Start(ctx context.Context) error {
	// Report consumer lag as a metric.
	go d.reportLag(ctx)

	for {
		msg, err := d.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return d.Close()
			}
			slog.Warn("indexer: fetch error (transient, retrying)", "error", err)
			select {
			case <-ctx.Done():
				return d.Close()
			case <-time.After(1 * time.Second):
			}
			continue
		}

		if err := d.handler(ctx, msg); err != nil {
			if d.recordFailure(ctx, msg) {
				// Poison message: exceeded max redeliveries, skip it so the
				// consumer can make progress on subsequent messages.
				continue
			}
			continue
		}

		if err := d.reader.CommitMessages(ctx, msg); err != nil {
			slog.Error("indexer: commit error", "offset", msg.Offset, "error", err)
		} else {
			slog.Info("indexer: processed and committed offset", "offset", msg.Offset, "key", string(msg.Key))
			d.clearRetry(msg.Offset)
		}
	}
}

// recordFailure tracks redelivery attempts and, once the limit is exceeded,
// commits (skips) the message so it no longer blocks the consumer.
// Returns true if the message was skipped and should not be retried.
func (d *IndexingDaemon) recordFailure(ctx context.Context, msg kafka.Message) bool {
	d.mu.Lock()
	d.retries[msg.Offset]++
	attempts := d.retries[msg.Offset]
	d.mu.Unlock()

	if attempts < maxRedeliveries {
		slog.Warn("indexer: handler error (will re-deliver)", "offset", msg.Offset, "attempt", attempts)
		return false
	}

	slog.Error("indexer: dropping poison message after max redeliveries",
		"offset", msg.Offset, "attempts", attempts)

	if d.dlq != nil {
		if err := d.dlq.WriteMessages(ctx, kafka.Message{
			Key:   msg.Key,
			Value: msg.Value,
			Time:  msg.Time,
		}); err != nil {
			slog.Error("indexer: failed to write poison message to DLQ", "offset", msg.Offset, "error", err)
		}
	}

	if err := d.reader.CommitMessages(ctx, msg); err != nil {
		slog.Error("indexer: failed to commit poison message", "offset", msg.Offset, "error", err)
		return false
	}
	d.clearRetry(msg.Offset)
	return true
}

func (d *IndexingDaemon) clearRetry(offset int64) {
	d.mu.Lock()
	delete(d.retries, offset)
	d.mu.Unlock()
}

// reportLag periodically publishes the consumer lag to Prometheus.
func (d *IndexingDaemon) reportLag(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lag, err := d.reader.ReadLag(ctx)
			if err == nil {
				metrics.KafkaConsumerLag.WithLabelValues(d.topic, "all").Set(float64(lag))
			}
		}
	}
}

// Close shuts down the Kafka reader cleanly. Idempotent.
func (d *IndexingDaemon) Close() error {
	d.closeOnce.Do(func() {
		d.closeErr = d.reader.Close()
	})
	return d.closeErr
}
