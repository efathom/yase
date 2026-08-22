package index

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
)

func TestCompactReclaimsSpace(t *testing.T) {
	const vecDim = 32
	he, cleanup := tempHybridEngine(t, vecDim)
	defer cleanup()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 100; i++ {
		vec := make([]float32, vecDim)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		doc := Document{
			ID:       uint32(i),
			Text:     fmt.Sprintf("document number %d about search", i),
			Vector:   vec,
			Metadata: map[string]string{"group": "test"},
		}
		if err := he.Ingest(ctx, doc); err != nil {
			t.Fatalf("Ingest(%d): %v", i, err)
		}
	}

	before := he.Arena.UsedBytes()

	// Delete every even ID.
	var toDelete []uint32
	for i := 0; i < 100; i += 2 {
		toDelete = append(toDelete, uint32(i))
	}
	if _, err := he.Delete(ctx, toDelete, nil); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if err := he.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	after := he.Arena.UsedBytes()
	if after >= before {
		t.Errorf("expected arena to shrink after compaction: before=%d after=%d", before, after)
	}

	// Remaining docs should still be searchable.
	queryVec := make([]float32, vecDim)
	for j := range queryVec {
		queryVec[j] = rng.Float32()*2 - 1
	}
	results, err := he.HybridSearch(ctx, "document", queryVec, nil, 5)
	if err != nil {
		t.Fatalf("HybridSearch after compact: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected results after compaction")
	}
}

func TestSnapshotCorruptionDetected(t *testing.T) {
	// A frame with a bad checksum must be rejected, not silently parsed.
	payload := []byte{0x01, 0x02, 0x03, 0x04}
	_, err := readFrame(bytes.NewReader(payload), ifMagic)
	if err == nil {
		t.Fatal("expected error for corrupt frame")
	}
}
