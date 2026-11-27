package memory

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

const (
	fileArenaHeaderSize = 64
	fileArenaMagic      = 0x59415345 // "YASE"
	fileArenaVersion    = 2
)

// Header layout (little endian):
//
//	[0:4]   magic
//	[4:8]   version
//	[8:16]  allocation watermark (global offset)
//	[16:24] segment size
//	[24:32] capacity (0 = unlimited)

// FileArena is a file-backed mmap arena that persists vectors to disk.
//
// The backing file grows one segment at a time (ftruncate) and each segment is
// mapped separately with MAP_SHARED at its own file offset, so growth never
// remaps — and never invalidates — memory that readers already hold. The
// first 64 bytes of segment 0 hold the header.
type FileArena struct {
	s        *segments
	filePath string
	fd       int
}

// NewFileArena creates a new file-backed arena at the given path that can hold
// up to sizeBytes of data. If the file exists, it is truncated.
func NewFileArena(path string, sizeBytes uint64) (*FileArena, error) {
	if sizeBytes == 0 {
		return nil, errors.New("arena size must be > 0")
	}
	capacity := sizeBytes + fileArenaHeaderSize
	segSize := segmentSizeFor(capacity)

	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CREAT|syscall.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	a := &FileArena{filePath: path, fd: fd}
	a.s = newSegments(segSize, capacity, fileArenaHeaderSize, a.mapper(segSize))
	if err := a.s.ensure(0); err != nil {
		syscall.Close(fd)
		return nil, err
	}

	hdr := (*a.s.segs.Load())[0]
	binary.LittleEndian.PutUint32(hdr[0:4], fileArenaMagic)
	binary.LittleEndian.PutUint32(hdr[4:8], fileArenaVersion)
	binary.LittleEndian.PutUint64(hdr[8:16], fileArenaHeaderSize)
	binary.LittleEndian.PutUint64(hdr[16:24], segSize)
	binary.LittleEndian.PutUint64(hdr[24:32], capacity)
	return a, nil
}

// mapper returns a segment mapper that extends the file as needed.
func (a *FileArena) mapper(segSize uint64) segmentMapper {
	return func(idx int) ([]byte, error) {
		need := int64(uint64(idx+1) * segSize)
		var st syscall.Stat_t
		if err := syscall.Fstat(a.fd, &st); err != nil {
			return nil, fmt.Errorf("fstat: %w", err)
		}
		if st.Size < need {
			if err := syscall.Ftruncate(a.fd, need); err != nil {
				return nil, fmt.Errorf("ftruncate: %w", err)
			}
		}
		return syscall.Mmap(a.fd, int64(uint64(idx)*segSize), int(segSize),
			syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	}
}

// OpenFileArena opens an existing file-backed arena and restores the offset.
func OpenFileArena(path string) (*FileArena, error) {
	hdr := make([]byte, fileArenaHeaderSize)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	info, statErr := f.Stat()
	_, readErr := f.ReadAt(hdr, 0)
	f.Close()
	if statErr != nil {
		return nil, fmt.Errorf("stat: %w", statErr)
	}
	if readErr != nil {
		return nil, errors.New("file too small to contain arena header")
	}

	if magic := binary.LittleEndian.Uint32(hdr[0:4]); magic != fileArenaMagic {
		return nil, fmt.Errorf("invalid arena magic: %x (expected %x)", magic, fileArenaMagic)
	}
	if v := binary.LittleEndian.Uint32(hdr[4:8]); v != fileArenaVersion {
		return nil, fmt.Errorf("unsupported arena version %d (expected %d)", v, fileArenaVersion)
	}
	savedOffset := binary.LittleEndian.Uint64(hdr[8:16])
	segSize := binary.LittleEndian.Uint64(hdr[16:24])
	capacity := binary.LittleEndian.Uint64(hdr[24:32])
	fileSize := uint64(info.Size())

	if segSize == 0 || fileSize%segSize != 0 {
		return nil, fmt.Errorf("invalid arena segment size %d for file size %d", segSize, fileSize)
	}
	if savedOffset < fileArenaHeaderSize || savedOffset > fileSize {
		return nil, fmt.Errorf("invalid saved offset: %d (size=%d)", savedOffset, fileSize)
	}

	fd, err := syscall.Open(path, syscall.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	a := &FileArena{filePath: path, fd: fd}
	a.s = newSegments(segSize, capacity, fileArenaHeaderSize, a.mapper(segSize))
	if err := a.s.ensure(int(fileSize/segSize) - 1); err != nil {
		_, _ = a.s.closeWith(syscall.Munmap)
		syscall.Close(fd)
		return nil, err
	}
	a.s.offset.Store(savedOffset)
	return a, nil
}

// AllocFloat32 stores a vector in the file-backed arena and returns its byte offset.
func (a *FileArena) AllocFloat32(vec []float32) (uint64, error) {
	if len(vec) == 0 {
		return 0, errors.New("cannot allocate zero-length vector")
	}
	return a.s.alloc(float32Bytes(vec), 4)
}

// GetFloat32 returns a zero-copy view into the file-backed arena.
// Panics if the read would exceed the arena bounds or the arena is closed.
func (a *FileArena) GetFloat32(offset uint64, length int) []float32 {
	return a.s.getFloat32(offset, length)
}

// AllocBytes stores raw bytes with 8-byte alignment in the file-backed arena.
func (a *FileArena) AllocBytes(data []byte) (uint64, error) {
	return a.s.alloc(data, 8)
}

// GetBytes returns a view into the file-backed arena at the given byte offset.
// Panics if the read would exceed the arena bounds or the arena is closed.
func (a *FileArena) GetBytes(offset uint64, length int) []byte {
	return a.s.get(offset, length, 1)
}

// UsedBytes returns the current allocation watermark (including header).
func (a *FileArena) UsedBytes() uint64 {
	return a.s.offset.Load()
}

// Path returns the backing file path.
func (a *FileArena) Path() string { return a.filePath }

// Sync records the watermark in the header and flushes dirty pages via msync.
func (a *FileArena) Sync() error {
	a.s.mu.RLock()
	defer a.s.mu.RUnlock()
	if a.s.closed {
		return ErrArenaClosed
	}
	return a.syncLocked()
}

// syncLocked is Sync without acquiring the lock. Callers must already hold s.mu.
func (a *FileArena) syncLocked() error {
	segs := *a.s.segs.Load()
	// Flush data segments first, then publish the watermark, so a crash can
	// never leave a header pointing past unflushed vectors.
	for _, seg := range segs {
		if err := msync(seg); err != nil {
			return err
		}
	}
	binary.LittleEndian.PutUint64(segs[0][8:16], a.s.offset.Load())
	return msync(segs[0][:fileArenaHeaderSize])
}

func msync(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(syscall.MS_SYNC))
	if errno != 0 {
		return errno
	}
	return nil
}

// Close writes the offset watermark, syncs to disk, and releases the mmap.
// It waits for in-flight reads and allocations, and is safe to call twice.
func (a *FileArena) Close() error {
	a.s.mu.Lock()
	if a.s.closed {
		a.s.mu.Unlock()
		return ErrArenaClosed
	}
	syncErr := a.syncLocked()
	a.s.mu.Unlock()

	_, unmapErr := a.s.closeWith(syscall.Munmap)
	closeErr := syscall.Close(a.fd)
	return errors.Join(syncErr, unmapErr, closeErr)
}

// SegmentSize returns the size of each mapped segment.
func (a *FileArena) SegmentSize() uint64 { return a.s.segSize }
