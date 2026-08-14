// Package broker provides Kafka producers, consumers, and topic management for
// the ingestion pipeline.
package broker

import (
	"context"
	"time"

	"github.com/efathom/yase/pkg/config"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// KafkaProducer writes CrawlRecord messages to a Kafka topic using
// segmentio/kafka-go with Murmur2 partitioning for URL-deterministic ordering.
type KafkaProducer struct {
	writer *kafka.Writer
}

// NewKafkaProducer creates a Kafka producer configured for synchronous,
// at-least-once writes. Batch size, batch timeout, and required acks are
// taken from config (falling back to safe defaults).
func NewKafkaProducer(cfg config.KafkaConfig) *KafkaProducer {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	batchTimeout := cfg.BatchTimeout
	if batchTimeout <= 0 {
		batchTimeout = 10 * time.Millisecond
	}

	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        cfg.Topic,
			Balancer:     &kafka.Murmur2Balancer{},
			RequiredAcks: kafka.RequiredAcks(cfg.RequiredAcks),
			BatchSize:    batchSize,
			BatchTimeout: batchTimeout,
			WriteTimeout: 30 * time.Second,
			Async:        false,
		},
	}
}

// NewDLQWriter creates a raw Kafka writer for dead-lettered messages. Poison
// messages are written verbatim (they may be malformed protobuf).
func NewDLQWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Murmur2Balancer{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}
}

// Produce serializes and writes a CrawlRecord to Kafka.
// The URL is used as the message key for deterministic partitioning.
func (k *KafkaProducer) Produce(ctx context.Context, record *ingestionv1.CrawlRecord) error {
	data, err := proto.Marshal(record)
	if err != nil {
		return err
	}

	return k.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(record.Url),
		Value: data,
		Time:  time.Unix(record.CrawledAtUnix, 0),
	})
}

// Close flushes pending messages and shuts down the writer.
func (k *KafkaProducer) Close() error {
	return k.writer.Close()
}
