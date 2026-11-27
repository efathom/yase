package memory

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

// DefaultSegmentSize is the size of each mmap segment. Arenas grow one
// segment at a time, so memory (or disk) is only consumed as vectors are
// actually stored, while the configured arena size acts as an upper bound.
const DefaultSegmentSize uint64 = 64 << 20 // 64 MiB

// ErrAllocTooLarge is returned when a single allocation exceeds the segment size.
var ErrAllocTooLarge = errors.New("arena allocation larger than segment size")

// segmentMapper maps segment idx (creating backing storage if needed).
type segmentMapper func(idx int) ([]byte, error)

// segments is a growable, append-only list of equally sized mmap regions
// addressed by a single global byte offset. An allocation never straddles two
// segments, so a returned offset always resolves to one contiguous region.
//
// Growth never moves existing segments, so slices handed out by get remain
// valid until Close — this is what makes growth safe without stopping readers.
type segments struct {
	// mu guards the mapping lifetime: readers/allocators hold it for read so
	// that Close cannot unmap memory out from under them.
	mu      sync.RWMutex
	closed  bool
	segSize uint64
	maxSize uint64 // total capacity bound (0 = unlimited)
	base    uint64 // first allocatable offset (header size for file arenas)

	segs   atomic.Pointer[[][]byte]
	offset atomic.Uint64 // bump allocation watermark

	growMu sync.Mutex
	mapSeg segmentMapper
}

func newSegments(segSize, maxSize, base uint64, mapper segmentMapper) *segments {
	s := &segments{segSize: segSize, maxSize: maxSize, base: base, mapSeg: mapper}
	empty := [][]byte{}
	s.segs.Store(&empty)
	s.offset.Store(base)
	return s
}

// ensure makes sure segments [0..idx] are mapped.
func (s *segments) ensure(idx int) error {
	if idx < len(*s.segs.Load()) {
		return nil
	}
	s.growMu.Lock()
	defer s.growMu.Unlock()
	cur := *s.segs.Load()
	for len(cur) <= idx {
		data, err := s.mapSeg(len(cur))
		if err != nil {
			return fmt.Errorf("map arena segment %d: %w", len(cur), err)
		}
		next := make([][]byte, len(cur)+1)
		copy(next, cur)
		next[len(cur)] = data
		s.segs.Store(&next)
		cur = next
	}
	return nil
}

// alloc reserves n bytes aligned to align and copies src into them.
func (s *segments) alloc(src []byte, align uint64) (uint64, error) {
	n := uint64(len(src))
	if n == 0 {
		return 0, errors.New("cannot allocate zero-length data")
	}
	if n > s.segSize {
		return 0, ErrAllocTooLarge
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, ErrArenaClosed
	}

	for {
		old := s.offset.Load()
		aligned := alignUp(old, align)
		if aligned%s.segSize+n > s.segSize {
			// Would straddle a segment boundary: start at the next segment.
			aligned = (aligned/s.segSize + 1) * s.segSize
		}
		end := aligned + n
		if s.maxSize > 0 && end > s.maxSize {
			return 0, ErrArenaFull
		}
		idx := int(aligned / s.segSize)
		if err := s.ensure(idx); err != nil {
			return 0, err
		}
		if s.offset.CompareAndSwap(old, end) {
			seg := (*s.segs.Load())[idx]
			inner := aligned % s.segSize
			copy(seg[inner:inner+n], src)
			return aligned, nil
		}
	}
}

// get returns a view of n bytes at offset. Panics on out-of-bounds or closed
// arenas: both indicate corruption or a lifecycle bug in the caller.
func (s *segments) get(offset uint64, length int, elemSize uint64) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		panic("arena read after close")
	}
	segs := *s.segs.Load()
	capacity := uint64(len(segs)) * s.segSize
	checkBounds(offset, length, elemSize, capacity)
	if length == 0 {
		return nil
	}
	n := uint64(length) * elemSize
	idx := offset / s.segSize
	inner := offset % s.segSize
	if inner+n > s.segSize {
		panic(panicMessage(offset, length, capacity))
	}
	return segs[idx][inner : inner+n]
}

func (s *segments) getFloat32(offset uint64, length int) []float32 {
	b := s.get(offset, length, 4)
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), length)
}

// closeWith marks the arena closed, waiting for in-flight operations, then
// releases every segment with unmap.
func (s *segments) closeWith(unmap func([]byte) error) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrArenaClosed
	}
	s.closed = true
	var errs []error
	for _, seg := range *s.segs.Load() {
		if err := unmap(seg); err != nil {
			errs = append(errs, err)
		}
	}
	empty := [][]byte{}
	s.segs.Store(&empty)
	return true, errors.Join(errs...)
}

func segmentSizeFor(capacity uint64) uint64 {
	if capacity > 0 && capacity < DefaultSegmentSize {
		return capacity
	}
	return DefaultSegmentSize
}
