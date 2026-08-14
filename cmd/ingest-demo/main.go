// cmd/ingest-demo sends sample documents to the ingestion server via gRPC.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	ingestionv1 "github.com/efathom/yase/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cc, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("grpc client", "error", err)
		os.Exit(1)
	}
	defer cc.Close()

	client := ingestionv1.NewIngestionServiceClient(cc)

	records := []*ingestionv1.CrawlRecord{
		{Url: "https://example.com/go", RawContent: []byte("Go is a statically typed compiled programming language designed at Google. It has excellent concurrency support through goroutines and channels."), Metadata: map[string]string{"topic": "programming", "lang": "en"}, CrawledAtUnix: time.Now().Unix()},
		{Url: "https://example.com/rust", RawContent: []byte("Rust is a systems programming language focused on safety, speed, and concurrency. It provides memory safety without garbage collection."), Metadata: map[string]string{"topic": "programming", "lang": "en"}, CrawledAtUnix: time.Now().Unix()},
		{Url: "https://example.com/python", RawContent: []byte("Python is a high-level interpreted programming language known for its simplicity. It is widely used in data science, machine learning, and web development."), Metadata: map[string]string{"topic": "data-science", "lang": "en"}, CrawledAtUnix: time.Now().Unix()},
		{Url: "https://example.com/search-engines", RawContent: []byte("Search engines use inverted indexes and vector embeddings to find relevant documents. BM25 is a popular ranking function for text retrieval."), Metadata: map[string]string{"topic": "information-retrieval", "lang": "en"}, CrawledAtUnix: time.Now().Unix()},
		{Url: "https://example.com/distributed-systems", RawContent: []byte("Distributed systems use techniques like consistent hashing, replication, and consensus protocols. Kafka provides durable message streaming for event-driven architectures."), Metadata: map[string]string{"topic": "systems", "lang": "en"}, CrawledAtUnix: time.Now().Unix()},
	}

	resp, err := client.IngestBatch(ctx, &ingestionv1.BatchIngestRequest{Records: records})
	if err != nil {
		slog.Error("IngestBatch failed", "error", err)
		os.Exit(1)
	}

	fmt.Printf("Ingested %d records (status: %s)\n", resp.ProcessedCount, resp.Status)
	fmt.Println("Waiting 5 seconds for Kafka consumer to index...")
	time.Sleep(5 * time.Second)
	fmt.Println("Done! Try: curl -s localhost:8000/search -d '{\"query\":\"programming language\",\"top_k\":5}' | jq")
}
