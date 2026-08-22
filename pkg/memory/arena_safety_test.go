package memory

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestArena(t *testing.T, size uint64) *OffHeapArena {
	t.Helper()
	a, err := NewOffHeapArena(size)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	return a
}

// M-10: the guard computed offset + length*4, which wraps on uint64. A length
// large enough to overflow produced a small "end", passed the check, and
// reached unsafe.Slice anyway.
func TestGetFloat32RejectsOverflowingLength(t *testing.T) {
	a := newTestArena(t, 4096)

	// length * 4 == 2^64 exactly, so the old check computed end == offset.
	overflowing := int(1) << 62

	assert.PanicsWithValue(t,
		panicMessage(0, overflowing, 4096),
		func() { a.GetFloat32(0, overflowing) },
		"an overflowing length must be caught by the bounds check")
}

func TestGetFloat32RejectsNegativeLength(t *testing.T) {
	a := newTestArena(t, 4096)

	assert.Panics(t, func() { a.GetFloat32(0, -1) },
		"a negative length must not reach unsafe.Slice")
}

func TestGetBytesRejectsOverflowingLength(t *testing.T) {
	a := newTestArena(t, 4096)

	assert.Panics(t, func() { a.GetBytes(8, math.MaxInt) },
		"offset + length must not be allowed to wrap")
}

// A legitimate read still works.
func TestGetFloat32ReadsBackWhatWasWritten(t *testing.T) {
	a := newTestArena(t, 4096)

	want := []float32{1.5, -2.25, 3.75}
	off, err := a.AllocFloat32(want)
	require.NoError(t, err)

	assert.Equal(t, want, a.GetFloat32(off, len(want)))
}

// M-11: Close unmapped the region with no lock, so a concurrent reader could
// touch freed pages — a SIGSEGV that recover() cannot catch.
func TestCloseIsSafeAgainstConcurrentReaders(t *testing.T) {
	a := newTestArena(t, 1<<20)

	vec := []float32{1, 2, 3, 4}
	off, err := a.AllocFloat32(vec)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				// After Close this must fail cleanly rather than crash.
				func() {
					defer func() { _ = recover() }()
					_ = a.GetFloat32(off, len(vec))
				}()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = a.Close()
	}()

	wg.Wait()
}

// M-11: Close must be idempotent — a second call previously dereferenced a nil
// slice or double-unmapped.
func TestCloseIsIdempotent(t *testing.T) {
	a, err := NewOffHeapArena(4096)
	require.NoError(t, err)

	require.NoError(t, a.Close())
	assert.ErrorIs(t, a.Close(), ErrArenaClosed)
}

// M-11: allocating into a closed arena must return an error, not panic on a
// nil backing slice.
func TestAllocAfterCloseReturnsError(t *testing.T) {
	a, err := NewOffHeapArena(4096)
	require.NoError(t, err)
	require.NoError(t, a.Close())

	_, err = a.AllocFloat32([]float32{1, 2, 3})
	assert.ErrorIs(t, err, ErrArenaClosed)

	_, err = a.AllocBytes([]byte{1, 2, 3})
	assert.ErrorIs(t, err, ErrArenaClosed)
}
