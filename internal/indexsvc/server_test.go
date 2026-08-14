package indexsvc

import (
	"context"
	"os"
	"testing"

	"github.com/efathom/yase/pkg/index"
	ingestionv1 "github.com/efathom/yase/proto/v1"
)

func makeEngine(t *testing.T, dim int) *index.HybridEngine {
	t.Helper()
	dir, err := os.MkdirTemp("", "indexsvc-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	engine, err := index.NewHybridEngine(dir, 64*1024*1024, dim, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })
	return engine
}

func TestIngestAndSearch(t *testing.T) {
	dim := 8
	engine := makeEngine(t, dim)
	srv := NewServer(engine)
	ctx := context.Background()

	// Ingest a document
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = float32(i) * 0.1
	}
	resp, err := srv.Ingest(ctx, &ingestionv1.IngestRequest{
		DocId:    0,
		Text:     "Go is a compiled programming language",
		Vector:   vec,
		Metadata: map[string]string{"topic": "tech"},
	})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if !resp.Ok {
		t.Error("expected ok=true")
	}

	// Search
	searchResp, err := srv.Search(ctx, &ingestionv1.SearchRequest{
		Query:       "programming",
		QueryVector: vec,
		TopK:        5,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(searchResp.Results) == 0 {
		t.Error("expected at least one result")
	}
}

func TestStats(t *testing.T) {
	dim := 8
	engine := makeEngine(t, dim)
	srv := NewServer(engine)
	ctx := context.Background()

	// Empty stats
	stats, err := srv.Stats(ctx, &ingestionv1.StatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.HnswNodes != 0 {
		t.Errorf("expected 0 nodes, got %d", stats.HnswNodes)
	}

	// Ingest a centroid (ID=0, 0%5==0 → centroid)
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = 0.5
	}
	srv.Ingest(ctx, &ingestionv1.IngestRequest{
		DocId:  0,
		Text:   "centroid document",
		Vector: vec,
	})

	stats, err = srv.Stats(ctx, &ingestionv1.StatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.HnswNodes != 1 {
		t.Errorf("expected 1 node, got %d", stats.HnswNodes)
	}
}
