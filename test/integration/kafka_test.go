package integration

import (
	"context"
	"testing"
	"time"

	"github.com/efathom/yase/internal/broker"
	"github.com/efathom/yase/pkg/config"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"google.golang.org/protobuf/proto"
)

func startKafka(t *testing.T) []string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	ctx := context.Background()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.0")
	if err != nil {
		t.Fatalf("start kafka container: %v", err)
	}
	t.Cleanup(func() { container.Terminate(ctx) })

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}
	return brokers
}

func TestKafkaProduceConsumeRoundTrip(t *testing.T) {
	brokers := startKafka(t)
	ctx := context.Background()
	topic := "test-roundtrip"

	// Create topic
	conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, 0)
	if err != nil {
		t.Fatalf("dial leader: %v", err)
	}
	conn.Close()

	// Produce
	producer := broker.NewKafkaProducer(config.KafkaConfig{Brokers: brokers, Topic: topic, RequiredAcks: -1})
	t.Cleanup(func() { producer.Close() })

	record := &ingestionv1.CrawlRecord{
		Url:           "https://example.com/test",
		RawContent:    []byte("Hello from integration test"),
		Metadata:      map[string]string{"source": "test"},
		CrawledAtUnix: time.Now().Unix(),
	}

	if err := producer.Produce(ctx, record); err != nil {
		t.Fatalf("produce: %v", err)
	}
	producer.Close()

	// Consume
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  "test-consumer",
		MinBytes: 1,
		MaxBytes: 10 * 1024 * 1024,
		MaxWait:  5 * time.Second,
	})
	t.Cleanup(func() { reader.Close() })

	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	msg, err := reader.ReadMessage(readCtx)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	// Verify content
	var consumed ingestionv1.CrawlRecord
	if err := proto.Unmarshal(msg.Value, &consumed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if consumed.Url != record.Url {
		t.Errorf("URL: got %q, want %q", consumed.Url, record.Url)
	}
	if string(consumed.RawContent) != string(record.RawContent) {
		t.Errorf("content mismatch")
	}
	if consumed.Metadata["source"] != "test" {
		t.Errorf("metadata: got %v, want source=test", consumed.Metadata)
	}

	// Verify key is URL (Murmur2 partitioning)
	if string(msg.Key) != record.Url {
		t.Errorf("key: got %q, want %q", string(msg.Key), record.Url)
	}
}

func TestKafkaMultipleMessages(t *testing.T) {
	brokers := startKafka(t)
	ctx := context.Background()
	topic := "test-multi"

	conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, 0)
	if err != nil {
		t.Fatalf("dial leader: %v", err)
	}
	conn.Close()

	producer := broker.NewKafkaProducer(config.KafkaConfig{Brokers: brokers, Topic: topic, RequiredAcks: -1})

	urls := []string{
		"https://example.com/a",
		"https://example.com/b",
		"https://example.com/c",
	}

	for _, u := range urls {
		err := producer.Produce(ctx, &ingestionv1.CrawlRecord{
			Url:           u,
			RawContent:    []byte("content for " + u),
			CrawledAtUnix: time.Now().Unix(),
		})
		if err != nil {
			t.Fatalf("produce %s: %v", u, err)
		}
	}
	producer.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  "test-multi-consumer",
		MinBytes: 1,
		MaxBytes: 10 * 1024 * 1024,
		MaxWait:  5 * time.Second,
	})
	t.Cleanup(func() { reader.Close() })

	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	consumed := make(map[string]bool)
	for i := 0; i < 3; i++ {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("consume message %d: %v", i, err)
		}
		consumed[string(msg.Key)] = true
	}

	for _, u := range urls {
		if !consumed[u] {
			t.Errorf("missing consumed message for %s", u)
		}
	}
}
