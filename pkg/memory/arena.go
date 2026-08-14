package memory

import (
	"errors"
	"fmt"
	"sync/atomic"
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

// OffHeapArena stores float32 vectors outside Go's heap via mmap so the GC
// sees zero pointers. A lock-free bump allocator (CAS loop) allows safe
// concurrent allocation from multiple goroutines.
type OffHeapArena struct {
	data   []byte
	size   uint64
	offset atomic.Uint64
}

// NewOffHeapArena allocates an anonymous mmap region of sizeBytes.
// The memory is invisible to Go's tri-color GC.
func NewOffHeapArena(sizeBytes uint64) (*OffHeapArena, error) {
	if sizeBytes == 0 {
		return nil, errors.New("arena size must be > 0")
	}
	data, err := syscall.Mmap(-1, 0, int(sizeBytes),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	return &OffHeapArena{data: data, size: sizeBytes}, nil
}

// alignUp rounds offset up to the given alignment (must be power of 2).
func alignUp(offset, alignment uint64) uint64 {
	return (offset + alignment - 1) &^ (alignment - 1)
}

// AllocFloat32 stores a vector off-heap and returns its byte offset.
// Uses a CAS loop for lock-free concurrent allocation with proper alignment.
func (a *OffHeapArena) AllocFloat32(vec []float32) (uint64, error) {
	byteLen := uint64(len(vec)) * 4 // 4 bytes per float32
	if byteLen == 0 {
		return 0, errors.New("cannot allocate zero-length vector")
	}

	for {
		old := a.offset.Load()
		aligned := alignUp(old, 4) // float32 requires 4-byte alignment
		newOffset := aligned + byteLen
		if newOffset > a.size {
			return 0, ErrArenaFull
		}
		if a.offset.CompareAndSwap(old, newOffset) {
			// Write vector via zero-copy cast
			target := unsafe.Slice((*float32)(unsafe.Pointer(&a.data[aligned])), len(vec))
			copy(target, vec)
			return aligned, nil
		}
		// CAS failed — another goroutine allocated concurrently; retry
	}
}

// GetFloat32 returns a zero-copy view into the arena at the given byte offset.
// Panics if the read would exceed the arena bounds (indicates data corruption).
func (a *OffHeapArena) GetFloat32(offset uint64, length int) []float32 {
	end := offset + uint64(length)*4
	if end > a.size || offset > a.size {
		panic(fmt.Sprintf("arena read out of bounds: offset=%d len=%d size=%d", offset, length, a.size))
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&a.data[offset])), length)
}

// AllocBytes stores raw bytes in the arena with 8-byte alignment and returns the offset.
func (a *OffHeapArena) AllocBytes(data []byte) (uint64, error) {
	byteLen := uint64(len(data))
	if byteLen == 0 {
		return 0, errors.New("cannot allocate zero-length data")
	}
	for {
		old := a.offset.Load()
		aligned := alignUp(old, 8)
		newOffset := aligned + byteLen
		if newOffset > a.size {
			return 0, ErrArenaFull
		}
		if a.offset.CompareAndSwap(old, newOffset) {
			copy(a.data[aligned:aligned+byteLen], data)
			return aligned, nil
		}
	}
}

// GetBytes returns a view into the arena at the given byte offset.
func (a *OffHeapArena) GetBytes(offset uint64, length int) []byte {
	end := offset + uint64(length)
	if end > a.size || offset > a.size {
		panic(fmt.Sprintf("arena read out of bounds: offset=%d len=%d size=%d", offset, length, a.size))
	}
	return a.data[offset : offset+uint64(length)]
}

// UsedBytes returns the current allocation watermark.
func (a *OffHeapArena) UsedBytes() uint64 {
	return a.offset.Load()
}

// Close releases the mmap region back to the OS.
func (a *OffHeapArena) Close() error {
	if a.data == nil {
		return ErrArenaClosed
	}
	err := syscall.Munmap(a.data)
	a.data = nil
	return err
}
