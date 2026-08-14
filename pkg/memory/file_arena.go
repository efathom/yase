package memory

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"unsafe"
)

const (
	fileArenaHeaderSize = 64
	fileArenaMagic      = 0x59415345 // "YASE"
	fileArenaVersion    = 1
)

// FileArena is a file-backed mmap arena that persists vectors to disk.
// Uses MAP_SHARED so writes are visible to the OS and can be flushed with msync.
// The first 64 bytes are reserved for a header containing magic, version, and
// the allocation offset watermark.
type FileArena struct {
	data     []byte
	size     uint64
	offset   atomic.Uint64
	filePath string
	fd       int
}

// NewFileArena creates a new file-backed arena at the given path.
// If the file exists, it is truncated. Data allocation starts after the header.
func NewFileArena(path string, sizeBytes uint64) (*FileArena, error) {
	if sizeBytes == 0 {
		return nil, errors.New("arena size must be > 0")
	}
	totalSize := sizeBytes + fileArenaHeaderSize

	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CREAT|syscall.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	if err := syscall.Ftruncate(fd, int64(totalSize)); err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("ftruncate: %w", err)
	}

	data, err := syscall.Mmap(fd, 0, int(totalSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("mmap: %w", err)
	}

	a := &FileArena{
		data:     data,
		size:     totalSize,
		filePath: path,
		fd:       fd,
	}
	a.offset.Store(fileArenaHeaderSize)

	// Write header
	binary.LittleEndian.PutUint32(data[0:4], fileArenaMagic)
	binary.LittleEndian.PutUint32(data[4:8], fileArenaVersion)
	binary.LittleEndian.PutUint64(data[8:16], fileArenaHeaderSize)

	return a, nil
}

// OpenFileArena opens an existing file-backed arena and restores the offset.
func OpenFileArena(path string) (*FileArena, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat: %w", err)
	}
	totalSize := uint64(info.Size())
	if totalSize < fileArenaHeaderSize {
		return nil, errors.New("file too small to contain arena header")
	}

	fd, err := syscall.Open(path, syscall.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	data, err := syscall.Mmap(fd, 0, int(totalSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("mmap: %w", err)
	}

	// Validate header
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != fileArenaMagic {
		syscall.Munmap(data)
		syscall.Close(fd)
		return nil, fmt.Errorf("invalid arena magic: %x (expected %x)", magic, fileArenaMagic)
	}

	savedOffset := binary.LittleEndian.Uint64(data[8:16])
	if savedOffset < fileArenaHeaderSize || savedOffset > totalSize {
		syscall.Munmap(data)
		syscall.Close(fd)
		return nil, fmt.Errorf("invalid saved offset: %d (size=%d)", savedOffset, totalSize)
	}

	a := &FileArena{
		data:     data,
		size:     totalSize,
		filePath: path,
		fd:       fd,
	}
	a.offset.Store(savedOffset)

	return a, nil
}

// AllocFloat32 stores a vector in the file-backed arena and returns its byte offset.
func (a *FileArena) AllocFloat32(vec []float32) (uint64, error) {
	byteLen := uint64(len(vec)) * 4
	if byteLen == 0 {
		return 0, errors.New("cannot allocate zero-length vector")
	}

	for {
		old := a.offset.Load()
		aligned := alignUp(old, 4)
		newOffset := aligned + byteLen
		if newOffset > a.size {
			return 0, ErrArenaFull
		}
		if a.offset.CompareAndSwap(old, newOffset) {
			target := unsafe.Slice((*float32)(unsafe.Pointer(&a.data[aligned])), len(vec))
			copy(target, vec)
			return aligned, nil
		}
	}
}

// GetFloat32 returns a zero-copy view into the file-backed arena.
func (a *FileArena) GetFloat32(offset uint64, length int) []float32 {
	end := offset + uint64(length)*4
	if end > a.size || offset > a.size {
		panic(fmt.Sprintf("file arena read out of bounds: offset=%d len=%d size=%d", offset, length, a.size))
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&a.data[offset])), length)
}

// AllocBytes stores raw bytes with 8-byte alignment in the file-backed arena.
func (a *FileArena) AllocBytes(data []byte) (uint64, error) {
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

// GetBytes returns a view into the file-backed arena at the given byte offset.
func (a *FileArena) GetBytes(offset uint64, length int) []byte {
	end := offset + uint64(length)
	if end > a.size || offset > a.size {
		panic(fmt.Sprintf("file arena read out of bounds: offset=%d len=%d size=%d", offset, length, a.size))
	}
	return a.data[offset : offset+uint64(length)]
}

// UsedBytes returns the current allocation watermark (including header).
func (a *FileArena) UsedBytes() uint64 {
	return a.offset.Load()
}

// Sync flushes dirty pages to disk via msync.
func (a *FileArena) Sync() error {
	// Update offset watermark in header before sync
	binary.LittleEndian.PutUint64(a.data[8:16], a.offset.Load())

	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&a.data[0])),
		uintptr(a.size),
		uintptr(syscall.MS_SYNC))
	if errno != 0 {
		return errno
	}
	return nil
}

// Close writes the offset watermark, syncs to disk, and releases the mmap.
func (a *FileArena) Close() error {
	if a.data == nil {
		return ErrArenaClosed
	}

	// Write final offset to header
	binary.LittleEndian.PutUint64(a.data[8:16], a.offset.Load())

	syncErr := a.Sync()
	unmapErr := syscall.Munmap(a.data)
	closeErr := syscall.Close(a.fd)
	a.data = nil

	return errors.Join(syncErr, unmapErr, closeErr)
}
