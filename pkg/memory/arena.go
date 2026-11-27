package memory

import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"
)

var (
	ErrArenaFull   = errors.New("arena out of memory")
	ErrArenaClosed = errors.New("arena already closed")
)

// Arena abstracts off-heap vector storage. Both OffHeapArena (anonymous mmap)
// and FileArena (file-backed mmap with persistence) implement this interface.
type Arena interface {
	AllocFloat32(vec []float32) (uint64, error)
	GetFloat32(offset uint64, length int) []float32
	AllocBytes(data []byte) (uint64, error)
	GetBytes(offset uint64, length int) []byte
	UsedBytes() uint64
	Close() error
}

// panicMessage formats the out-of-bounds panic value. Shared so tests can
// assert on the exact message rather than duplicating the format string.
func panicMessage(offset uint64, length int, size uint64) string {
	return fmt.Sprintf("arena read out of bounds: offset=%d len=%d size=%d", offset, length, size)
}

// checkBounds verifies that a read of length elements of elemSize bytes at
// offset stays inside an arena of size bytes.
//
// The comparison is written to avoid computing offset+length*elemSize, which
// wraps on uint64: a large enough length produced a small sum, passed the
// check, and let an out-of-bounds unsafe.Slice through.
func checkBounds(offset uint64, length int, elemSize uint64, size uint64) {
	if length < 0 || offset > size || uint64(length) > (size-offset)/elemSize {
		panic(panicMessage(offset, length, size))
	}
}

// OffHeapArena stores float32 vectors outside Go's heap via anonymous mmap so
// the GC sees zero pointers. It grows one segment at a time up to its capacity,
// so a large configured capacity costs nothing until it is used. A lock-free
// bump allocator (CAS loop) allows safe concurrent allocation.
//
// GetFloat32/GetBytes return views into the mapping, so a caller must not
// retain the returned slice past a possible Close — the engine holds its own
// lifecycle lock for the duration of a search to guarantee that.
type OffHeapArena struct {
	s *segments
}

// NewOffHeapArena creates an anonymous-mmap arena that can hold up to
// sizeBytes. Memory is mapped lazily in segments as allocations require it.
func NewOffHeapArena(sizeBytes uint64) (*OffHeapArena, error) {
	if sizeBytes == 0 {
		return nil, errors.New("arena size must be > 0")
	}
	segSize := segmentSizeFor(sizeBytes)
	s := newSegments(segSize, sizeBytes, 0, func(int) ([]byte, error) {
		return mmapAnon(int(segSize))
	})
	// Map the first segment eagerly so configuration errors surface at startup.
	if err := s.ensure(0); err != nil {
		return nil, err
	}
	return &OffHeapArena{s: s}, nil
}

// alignUp rounds offset up to the given alignment (must be power of 2).
func alignUp(offset, alignment uint64) uint64 {
	return (offset + alignment - 1) &^ (alignment - 1)
}

// AllocFloat32 stores a vector off-heap and returns its byte offset.
func (a *OffHeapArena) AllocFloat32(vec []float32) (uint64, error) {
	if len(vec) == 0 {
		return 0, errors.New("cannot allocate zero-length vector")
	}
	return a.s.alloc(float32Bytes(vec), 4)
}

// GetFloat32 returns a zero-copy view into the arena at the given byte offset.
// Panics if the read would exceed the arena bounds (indicates data corruption)
// or if the arena has been closed.
func (a *OffHeapArena) GetFloat32(offset uint64, length int) []float32 {
	return a.s.getFloat32(offset, length)
}

// AllocBytes stores raw bytes in the arena with 8-byte alignment and returns the offset.
func (a *OffHeapArena) AllocBytes(data []byte) (uint64, error) {
	return a.s.alloc(data, 8)
}

// GetBytes returns a view into the arena at the given byte offset.
// Panics if the read would exceed the arena bounds or the arena is closed.
func (a *OffHeapArena) GetBytes(offset uint64, length int) []byte {
	return a.s.get(offset, length, 1)
}

// UsedBytes returns the current allocation watermark.
func (a *OffHeapArena) UsedBytes() uint64 {
	return a.s.offset.Load()
}

// Close releases every mapped segment. It waits for in-flight reads and
// allocations to finish, and is safe to call more than once.
func (a *OffHeapArena) Close() error {
	_, err := a.s.closeWith(syscall.Munmap)
	return err
}

// float32Bytes reinterprets a float32 slice as bytes without copying.
func float32Bytes(vec []float32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&vec[0])), len(vec)*4)
}
