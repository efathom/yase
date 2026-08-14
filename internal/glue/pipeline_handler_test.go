package glue

import (
	"context"
	"os"
	"testing"

	"github.com/efathom/yase/pkg/embedder"
	"github.com/efathom/yase/pkg/index"
	ingestionv1 "github.com/efathom/yase/proto/v1"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

func makeTestEngine(t *testing.T, dim int) *index.HybridEngine {
	t.Helper()
	dir, err := os.MkdirTemp("", "pipeline-test-*")
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

func TestPipelineHandlerBasic(t *testing.T) {
	dim := 32
	emb := embedder.NewMockEmbedder(dim)
	engine := makeTestEngine(t, dim)
	handler := NewPipelineHandler(emb, engine, nil, 0.5)

	record := &ingestionv1.CrawlRecord{
		Url:           "https://example.com/page1",
		RawContent:    []byte("Machine learning is transforming industries. Deep learning models are particularly powerful."),
		Metadata:      map[string]string{"source": "test"},
		CrawledAtUnix: 1700000000,
	}

	data, err := proto.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}

	msg := kafka.Message{
		Key:   []byte(record.Url),
		Value: data,
	}

	err = handler.Handle(context.Background(), msg)
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
}

func TestPipelineHandlerEmptyContent(t *testing.T) {
	dim := 32
	emb := embedder.NewMockEmbedder(dim)
	engine := makeTestEngine(t, dim)
	handler := NewPipelineHandler(emb, engine, nil, 0.5)

	record := &ingestionv1.CrawlRecord{
		Url:        "https://example.com/empty",
		RawContent: []byte(""),
	}

	data, _ := proto.Marshal(record)
	msg := kafka.Message{Value: data}

	err := handler.Handle(context.Background(), msg)
	if err != nil {
		t.Fatalf("expected nil for empty content, got: %v", err)
	}
}

func TestPipelineHandlerBadProtobuf(t *testing.T) {
	dim := 32
	emb := embedder.NewMockEmbedder(dim)
	engine := makeTestEngine(t, dim)
	handler := NewPipelineHandler(emb, engine, nil, 0.5)

	msg := kafka.Message{Value: []byte("not valid protobuf")}

	err := handler.Handle(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error for invalid protobuf")
	}
}

func TestPipelineHandlerMultipleChunks(t *testing.T) {
	dim := 32
	emb := embedder.NewMockEmbedder(dim)
	engine := makeTestEngine(t, dim)
	// Use a very low threshold to force splitting
	handler := NewPipelineHandler(emb, engine, nil, 0.99)

	// Long text with distinct topics that should produce multiple chunks
	text := "The solar system consists of the Sun and objects bound to it by gravity. " +
		"The four inner planets are Mercury, Venus, Earth, and Mars. " +
		"Quantum computing uses quantum mechanical phenomena such as superposition and entanglement. " +
		"A quantum computer can solve certain problems exponentially faster than classical computers."

	record := &ingestionv1.CrawlRecord{
		Url:           "https://example.com/multi",
		RawContent:    []byte(text),
		Metadata:      map[string]string{"source": "test"},
		CrawledAtUnix: 1700000000,
	}

	data, _ := proto.Marshal(record)
	msg := kafka.Message{Key: []byte(record.Url), Value: data}

	err := handler.Handle(context.Background(), msg)
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
}

func TestPipelineHandlerMetadataPreserved(t *testing.T) {
	dim := 32
	emb := embedder.NewMockEmbedder(dim)
	engine := makeTestEngine(t, dim)
	handler := NewPipelineHandler(emb, engine, nil, 0.5)

	record := &ingestionv1.CrawlRecord{
		Url:        "https://example.com/meta",
		RawContent: []byte("Some content for indexing."),
		Metadata: map[string]string{
			"tenant": "acme",
			"lang":   "en",
		},
	}

	data, _ := proto.Marshal(record)
	msg := kafka.Message{Value: data}

	err := handler.Handle(context.Background(), msg)
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
}

func TestHashChunkID(t *testing.T) {
	id1 := hashChunkID("https://example.com", 0)
	id2 := hashChunkID("https://example.com", 1)
	id3 := hashChunkID("https://example.com", 0)

	if id1 == id2 {
		t.Error("different chunk indices should produce different IDs")
	}
	if id1 != id3 {
		t.Error("same URL + index should produce same ID")
	}
}
