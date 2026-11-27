package glue

import (
	"context"
	"errors"
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

func recordMsg(t *testing.T, url, body string, meta map[string]string) kafka.Message {
	t.Helper()
	data, err := proto.Marshal(&ingestionv1.CrawlRecord{Url: url, RawContent: []byte(body), Metadata: meta})
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Key: []byte(url), Value: data}
}

func TestPipelineReingestReplaces(t *testing.T) {
	tests := []struct {
		name       string
		firstMeta  map[string]string
		secondMeta map[string]string
		wantDocs   int // documents left for the URL across tenants
	}{
		{"same tenant replaces previous chunks", map[string]string{"_tenant": "a"}, map[string]string{"_tenant": "a"}, 1},
		{"other tenant with same URL does not delete", map[string]string{"_tenant": "a"}, map[string]string{"_tenant": "b"}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dim := 16
			engine := makeTestEngine(t, dim)
			handler := NewPipelineHandler(embedder.NewMockEmbedder(dim), engine, nil, 0.5)
			ctx := context.Background()
			url := "https://example.com/a"

			if err := handler.Handle(ctx, recordMsg(t, url, "First version of the page.", tt.firstMeta)); err != nil {
				t.Fatal(err)
			}
			if err := handler.Handle(ctx, recordMsg(t, url, "Second version of the page.", tt.secondMeta)); err != nil {
				t.Fatal(err)
			}
			ids, _, err := engine.BlugeStore.FilteredIDs(ctx, map[string]string{"url": url}, -1)
			if err != nil {
				t.Fatal(err)
			}
			if len(ids) != tt.wantDocs {
				t.Fatalf("got %d docs for url, want %d", len(ids), tt.wantDocs)
			}
		})
	}
}

// failingEmbedder fails EmbedBatch after the first n calls.
type failingEmbedder struct {
	embedder.Embedder
	okCalls int
}

func (f *failingEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	if f.okCalls <= 0 {
		return nil, errors.New("embedder down")
	}
	f.okCalls--
	return f.Embedder.EmbedBatch(ctx, texts)
}

func TestPipelineEmbedFailureLeavesOldVersionIntact(t *testing.T) {
	dim := 16
	engine := makeTestEngine(t, dim)
	ctx := context.Background()
	url := "https://example.com/b"
	good := NewPipelineHandler(embedder.NewMockEmbedder(dim), engine, nil, 0.5)
	if err := good.Handle(ctx, recordMsg(t, url, "Original content stays.", nil)); err != nil {
		t.Fatal(err)
	}

	// Sentence embedding succeeds, chunk embedding fails.
	bad := NewPipelineHandler(&failingEmbedder{Embedder: embedder.NewMockEmbedder(dim), okCalls: 1}, engine, nil, 0.5)
	err := bad.Handle(ctx, recordMsg(t, url, "New content. With two sentences.", nil))
	if err == nil {
		t.Fatal("expected an error so the message is retried, not committed")
	}
	ids, _, _ := engine.BlugeStore.FilteredIDs(ctx, map[string]string{"url": url}, -1)
	if len(ids) != 1 {
		t.Fatalf("old version must survive a failed re-index, got %d docs", len(ids))
	}
}

func TestPipelineMalformedIsPermanent(t *testing.T) {
	engine := makeTestEngine(t, 8)
	handler := NewPipelineHandler(embedder.NewMockEmbedder(8), engine, nil, 0.5)
	err := handler.Handle(context.Background(), kafka.Message{Value: []byte{0xff, 0xff, 0xff}})
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("want permanent error, got %v", err)
	}
}
