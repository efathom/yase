package index

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/efathom/yase/pkg/hnsw"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const persistDim = 16

func persistCfg(dir string) EngineConfig {
	return EngineConfig{Dir: dir, ArenaSize: 64 << 20, HNSW: hnsw.DefaultConfig(persistDim), CentroidRate: 5}
}

// deterministicVec derives a vector from text, standing in for an embedder.
func deterministicVec(text string) []float32 {
	h := int64(0)
	for _, c := range text {
		h = h*31 + int64(c)
	}
	rng := rand.New(rand.NewSource(h))
	v := make([]float32, persistDim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	return v
}

func fakeEmbed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		out[i] = deterministicVec(t)
	}
	return out, nil
}

func ingestN(t *testing.T, he *HybridEngine, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		id, err := he.NextID()
		require.NoError(t, err)
		text := fmt.Sprintf("persisted document number %d", i)
		require.NoError(t, he.Ingest(ctx, Document{ID: id, Text: text, Vector: deterministicVec(text),
			Metadata: map[string]string{"url": fmt.Sprintf("u%d", i)}}))
	}
}

// found reports whether the document with exactly this text is the top hit
// for its own text + vector.
func found(t *testing.T, he *HybridEngine, text string) bool {
	t.Helper()
	res, err := he.HybridSearch(context.Background(), text, deterministicVec(text), nil, 3)
	require.NoError(t, err)
	if len(res) == 0 {
		return false
	}
	docs, err := he.BlugeStore.GetDocumentsByIDs(context.Background(), []uint32{res[0].ID})
	require.NoError(t, err)
	return docs[res[0].ID].Content == text
}

func TestPersistentEngineRecovery(t *testing.T) {
	tests := []struct {
		name string
		// between runs: what happens to the first engine instance
		stop         func(t *testing.T, dir string, he *HybridEngine)
		wantReEmbed  bool
		wantAllFound bool
	}{
		{
			name: "clean close restores snapshot",
			stop: func(t *testing.T, _ string, he *HybridEngine) { require.NoError(t, he.Close()) },
		},
		{
			name: "crash after checkpoint loses post-checkpoint vectors, reconcile rebuilds them",
			stop: func(t *testing.T, dir string, he *HybridEngine) {
				require.NoError(t, he.Checkpoint())
				ingestN(t, he, 0) // nothing new
				// Simulate a crash: release files without the final snapshot.
				he.store = nil
				require.NoError(t, he.Close())
				// Remove the snapshot so the vector state is lost entirely.
				g, _, err := (&engineStore{dir: dir}).readCurrent()
				require.NoError(t, err)
				require.NoError(t, os.Remove((&engineStore{dir: dir}).snapPath(g)))
			},
			wantReEmbed: true,
		},
		{
			name: "corrupt snapshot falls back to rebuild",
			stop: func(t *testing.T, dir string, he *HybridEngine) {
				require.NoError(t, he.Close())
				s := &engineStore{dir: dir}
				g, _, err := s.readCurrent()
				require.NoError(t, err)
				f, err := os.OpenFile(s.snapPath(g), os.O_RDWR, 0)
				require.NoError(t, err)
				_, _ = f.WriteAt([]byte("garbage!"), 40)
				f.Close()
			},
			wantReEmbed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			he, err := OpenHybridEngine(persistCfg(dir))
			require.NoError(t, err)
			ingestN(t, he, 60)
			tt.stop(t, dir, he)

			he2, err := OpenHybridEngine(persistCfg(dir))
			require.NoError(t, err)
			defer he2.Close()

			st, err := he2.Reconcile(context.Background(), fakeEmbed, 16)
			require.NoError(t, err)
			assert.Equal(t, 60, st.Documents)
			if tt.wantReEmbed {
				assert.Equal(t, 60, st.ReEmbedded)
			} else {
				assert.Zero(t, st.ReEmbedded)
			}
			assert.Equal(t, 60, he2.DocCount())
			for i := 0; i < 60; i += 7 {
				assert.True(t, found(t, he2, fmt.Sprintf("persisted document number %d", i)), "doc %d", i)
			}

			// The ID allocator resumes past existing documents.
			id, err := he2.NextID()
			require.NoError(t, err)
			assert.GreaterOrEqual(t, id, uint32(60))
		})
	}
}

func TestReconcileDropsVectorsOfMissingDocs(t *testing.T) {
	dir := t.TempDir()
	he, err := OpenHybridEngine(persistCfg(dir))
	require.NoError(t, err)
	defer he.Close()
	ingestN(t, he, 10)

	// Remove a document from the text index only, as if a delete was
	// persisted by Bluge but not yet reflected in a checkpoint.
	require.NoError(t, he.BlugeStore.ApplyBatch([]uint32{3}, nil))
	st, err := he.Reconcile(context.Background(), nil, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, st.DroppedVectors)
	_, ok := he.VectorOffset(3)
	assert.False(t, ok)
}

func TestPersistentCompactionSwitchesGeneration(t *testing.T) {
	dir := t.TempDir()
	he, err := OpenHybridEngine(persistCfg(dir))
	require.NoError(t, err)
	ingestN(t, he, 50)

	ids := make([]uint32, 0, 25)
	for i := uint32(0); i < 25; i++ {
		ids = append(ids, i)
	}
	n, err := he.Delete(context.Background(), ids, nil)
	require.NoError(t, err)
	assert.Equal(t, 25, n)
	before := he.store.gen
	require.NoError(t, he.Compact())
	assert.Equal(t, before+1, he.store.gen)
	assert.Zero(t, he.DeadBytes())
	_, err = os.Stat(filepath.Join(dir, genName(before)))
	assert.True(t, os.IsNotExist(err), "old generation removed")
	require.NoError(t, he.Close())

	he2, err := OpenHybridEngine(persistCfg(dir))
	require.NoError(t, err)
	defer he2.Close()
	st, err := he2.Reconcile(context.Background(), fakeEmbed, 0)
	require.NoError(t, err)
	assert.Zero(t, st.ReEmbedded, "compacted generation restored from its snapshot")
	assert.Equal(t, 25, he2.DocCount())
	assert.True(t, found(t, he2, "persisted document number 40"))
}
