package chunking

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sameTopic embeds every sentence identically, so only size bounds split.
func sameTopic(calls *int) BatchEmbedFunc {
	return func(texts []string) ([][]float32, error) {
		*calls++
		out := make([][]float32, len(texts))
		for i := range texts {
			out[i] = []float32{1, 0, 0}
		}
		return out, nil
	}
}

func TestChunkerBounds(t *testing.T) {
	sentence := "The quick brown fox jumps over the lazy dog near the river bank."
	long := strings.Repeat(sentence+" ", 200) // ~13k chars, one topic

	tests := []struct {
		name      string
		text      string
		opts      Options
		check     func(t *testing.T, chunks []string)
		wantCalls int
	}{
		{
			name: "single topic is capped at MaxChars",
			text: long,
			opts: Options{Threshold: 0.5, MaxChars: 1000},
			check: func(t *testing.T, chunks []string) {
				assert.Greater(t, len(chunks), 10)
				for _, c := range chunks {
					assert.LessOrEqual(t, len(c), 1000)
				}
			},
			wantCalls: 1,
		},
		{
			name: "a sentence longer than MaxChars is split at word boundaries",
			text: strings.Repeat("word ", 1000) + ".",
			opts: Options{Threshold: 0.5, MaxChars: 300},
			check: func(t *testing.T, chunks []string) {
				for _, c := range chunks {
					assert.LessOrEqual(t, len(c), 300)
					assert.False(t, strings.HasPrefix(c, "ord"), "no mid-word split")
				}
			},
		},
		{
			name: "overlap repeats the previous chunk's last sentence",
			text: long,
			opts: Options{Threshold: 0.5, MaxChars: 500, OverlapSentences: 1},
			check: func(t *testing.T, chunks []string) {
				require.Greater(t, len(chunks), 1)
				assert.True(t, strings.HasPrefix(chunks[1], sentence))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			chunks, err := SemanticChunkerWithOptions(tt.text, sameTopic(&calls), tt.opts)
			require.NoError(t, err)
			tt.check(t, chunks)
			if tt.wantCalls > 0 {
				assert.Equal(t, tt.wantCalls, calls, "all sentences embedded in one batch")
			}
		})
	}
}

func TestChunkerMergesTinyChunks(t *testing.T) {
	// Alternate topics so every sentence would be its own chunk.
	text := "Alpha one. Beta two. Alpha three. Beta four. Alpha five. Beta six."
	flip := func(texts []string) ([][]float32, error) {
		out := make([][]float32, len(texts))
		for i := range texts {
			if i%2 == 0 {
				out[i] = []float32{1, 0}
			} else {
				out[i] = []float32{0, 1}
			}
		}
		return out, nil
	}
	tests := []struct {
		name     string
		minChars int
		want     int
	}{
		{"no merging", 0, 6},
		{"merge below 25 chars", 25, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks, err := SemanticChunkerWithOptions(text, flip, Options{Threshold: 0.5, MaxChars: 2000, MinChars: tt.minChars})
			require.NoError(t, err)
			assert.Len(t, chunks, tt.want)
		})
	}
}
