package memory

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Arenas grow segment by segment: allocations far beyond one segment succeed,
// never straddle a boundary, and earlier views stay valid across growth.
func TestArenaGrowsAcrossSegments(t *testing.T) {
	const capacity = 3 * DefaultSegmentSize

	tests := []struct {
		name string
		open func(t *testing.T) Arena
	}{
		{"offheap", func(t *testing.T) Arena {
			a, err := NewOffHeapArena(capacity)
			require.NoError(t, err)
			return a
		}},
		{"file", func(t *testing.T) Arena {
			a, err := NewFileArena(filepath.Join(t.TempDir(), "arena.bin"), capacity)
			require.NoError(t, err)
			return a
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := tt.open(t)
			defer a.Close()

			// 1 MiB vectors: ~64 per segment, so 150 of them span 3 segments.
			vec := make([]float32, 1<<18)
			first := []float32(nil)
			var offsets []uint64
			for i := 0; i < 150; i++ {
				vec[0] = float32(i)
				off, err := a.AllocFloat32(vec)
				require.NoError(t, err)
				assert.LessOrEqual(t, off%DefaultSegmentSize+uint64(len(vec)*4), DefaultSegmentSize,
					"allocation must not straddle a segment")
				offsets = append(offsets, off)
				if i == 0 {
					first = a.GetFloat32(off, len(vec))
				}
			}
			for i, off := range offsets {
				assert.Equal(t, float32(i), a.GetFloat32(off, len(vec))[0])
			}
			assert.Equal(t, float32(0), first[0], "early view must survive growth")

			// Capacity is a hard bound.
			for {
				if _, err := a.AllocFloat32(vec); err != nil {
					assert.ErrorIs(t, err, ErrArenaFull)
					break
				}
			}
		})
	}
}

func TestFileArenaReopensMultiSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arena.bin")
	a, err := NewFileArena(path, 4*DefaultSegmentSize)
	require.NoError(t, err)

	vec := make([]float32, 1<<18)
	var offs []uint64
	for i := 0; i < 100; i++ {
		vec[1] = float32(i)
		off, err := a.AllocFloat32(vec)
		require.NoError(t, err)
		offs = append(offs, off)
	}
	used := a.UsedBytes()
	require.NoError(t, a.Close())

	b, err := OpenFileArena(path)
	require.NoError(t, err)
	defer b.Close()
	assert.Equal(t, used, b.UsedBytes())
	for i, off := range offs {
		assert.Equal(t, float32(i), b.GetFloat32(off, len(vec))[1])
	}
	_, err = b.AllocFloat32(vec)
	assert.NoError(t, err, "a reopened arena keeps growing")
}

func TestAllocLargerThanSegmentFails(t *testing.T) {
	a, err := NewOffHeapArena(4096)
	require.NoError(t, err)
	defer a.Close()
	_, err = a.AllocBytes(make([]byte, 8192))
	assert.ErrorIs(t, err, ErrAllocTooLarge)
}
