package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/efathom/yase/internal/gateway"
	"github.com/efathom/yase/internal/idempotency"
	"github.com/efathom/yase/internal/server"
	"github.com/efathom/yase/internal/worker"
	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/index"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	goredis "github.com/redis/go-redis/v9"
	kafkago "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// TestEndToEndPipeline validates the full path:
// gRPC client → ingestion server → Kafka → consume → index → search gateway
func TestEndToEndPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	ctx := context.Background()

	// ── Start Redis ──
	redisContainer, err := tcredis.Run(ctx, "redis/redis-stack-server:latest")
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	redisURL, _ := redisContainer.ConnectionString(ctx)
	redisOpts, _ := goredis.ParseURL(redisURL)
	rdb := goredis.NewClient(redisOpts)
	defer rdb.Close()

	for i := 0; i < 30; i++ {
		if rdb.Ping(ctx).Err() == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// ── Start Kafka ──
	kafkaContainer, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.0")
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	defer kafkaContainer.Terminate(ctx)

	brokers, _ := kafkaContainer.Brokers(ctx)
	topic := "e2e-test"

	// Create topic by dialing leader
	conn, err := kafkago.DialLeader(ctx, "tcp", brokers[0], topic, 0)
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	conn.Close()

	// ── Kafka Producer as EventBroker ──
	kafkaWriter := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Murmur2Balancer{},
		RequiredAcks: kafkago.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
	kafkaBroker := &e2eKafkaBroker{writer: kafkaWriter}

	// ── Start gRPC Ingestion Server ──
	pool := worker.NewPool(4, 100, kafkaBroker)
	pool.Start()

	idemp := idempotency.NewManager(rdb)
	handler := server.NewIngestionHandler(pool, idemp)

	grpcServer := grpc.NewServer()
	ingestionv1.RegisterIngestionServiceServer(grpcServer, handler)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go grpcServer.Serve(lis)
	defer grpcServer.GracefulStop()

	// ── gRPC Client: Send CrawlRecords ──
	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()

	grpcClient := ingestionv1.NewIngestionServiceClient(cc)

	resp, err := grpcClient.IngestBatch(ctx, &ingestionv1.BatchIngestRequest{
		Records: []*ingestionv1.CrawlRecord{
			{Url: "https://example.com/go", RawContent: []byte("Go is a compiled programming language"), Metadata: map[string]string{"topic": "tech"}, CrawledAtUnix: time.Now().Unix()},
			{Url: "https://example.com/rust", RawContent: []byte("Rust ensures memory safety"), Metadata: map[string]string{"topic": "tech"}, CrawledAtUnix: time.Now().Unix()},
		},
	})
	if err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}
	if resp.ProcessedCount != 2 {
		t.Errorf("expected 2 processed, got %d", resp.ProcessedCount)
	}

	// Flush Kafka writer — Stop drains jobs and calls writer.Close()
	if err := pool.Stop(); err != nil {
		t.Fatalf("pool stop: %v", err)
	}

	// ── Consume from Kafka and verify ──
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     "e2e-consumer",
		StartOffset: kafkago.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10 * 1024 * 1024,
		MaxWait:     5 * time.Second,
	})
	defer reader.Close()

	consumed := make(map[string]*ingestionv1.CrawlRecord)
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for i := 0; i < 2; i++ {
		msg, err := reader.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("consume message %d: %v", i, err)
		}
		var crawlRec ingestionv1.CrawlRecord
		if err := proto.Unmarshal(msg.Value, &crawlRec); err != nil {
			t.Fatalf("unmarshal message %d: %v", i, err)
		}
		consumed[crawlRec.Url] = &crawlRec
	}

	if _, ok := consumed["https://example.com/go"]; !ok {
		t.Error("missing Go record in Kafka")
	}
	if _, ok := consumed["https://example.com/rust"]; !ok {
		t.Error("missing Rust record in Kafka")
	}

	// ── Index consumed records into HybridEngine ──
	dim := 32
	emb := embedder.NewMockEmbedder(dim)
	dir, _ := os.MkdirTemp("", "e2e-index-*")
	defer os.RemoveAll(dir)

	engine, err := index.NewHybridEngine(dir, 64*1024*1024, dim, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	docID := uint32(0)
	for _, crawlRec := range consumed {
		vec, _ := emb.Embed(ctx, string(crawlRec.RawContent))
		engine.Ingest(ctx, index.Document{
			ID:       docID,
			Text:     string(crawlRec.RawContent),
			Vector:   vec,
			Metadata: crawlRec.Metadata,
		})
		docID++
	}

	// ── Search via Gateway Handler ──
	gw := gateway.NewHandler(&gateway.LocalSearcher{Engine: engine}, emb)
	mux := http.NewServeMux()
	gw.RegisterRoutes(mux)

	body, _ := json.Marshal(gateway.SearchRequest{
		Query: "programming language",
		TopK:  5,
	})
	httpReq := httptest.NewRequest("POST", "/search", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	httpRec := httptest.NewRecorder()

	mux.ServeHTTP(httpRec, httpReq)

	if httpRec.Code != http.StatusOK {
		t.Fatalf("gateway: expected 200, got %d: %s", httpRec.Code, httpRec.Body.String())
	}

	var searchResp gateway.SearchResponse
	json.NewDecoder(httpRec.Body).Decode(&searchResp)
	if searchResp.Status != "success" {
		t.Errorf("expected success, got %q", searchResp.Status)
	}
	t.Logf("E2E search returned %d results in %dms", searchResp.Count, searchResp.DurationMs)
}

// e2eKafkaBroker adapts kafka.Writer to the worker.EventBroker interface.
type e2eKafkaBroker struct {
	writer *kafkago.Writer
}

func (b *e2eKafkaBroker) Produce(ctx context.Context, record *ingestionv1.CrawlRecord) error {
	data, err := proto.Marshal(record)
	if err != nil {
		return err
	}
	return b.writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(record.Url),
		Value: data,
		Time:  time.Unix(record.CrawledAtUnix, 0),
	})
}

func (b *e2eKafkaBroker) Close() error {
	return b.writer.Close()
}
