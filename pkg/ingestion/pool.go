package ingestion

import (
	"math/bits"
	"sync"
)

// bufferPools provides power-of-two sized sync.Pools for zero-copy
// protobuf serialization. Avoids per-message heap allocation.
var bufferPools [32]*sync.Pool

func init() {
	for i := 0; i < 32; i++ {
		size := 1 << i
		bufferPools[i] = &sync.Pool{
			New: func() interface{} {
				b := make([]byte, size)
				return &b
			},
		}
	}
}

// GetBuffer retrieves a buffer from the nearest power-of-two pool.
// Sizes >= 2^32 bypass the pool and allocate directly.
func GetBuffer(size int) *[]byte {
	if size <= 0 {
		return nil
	}
	idx := bits.Len(uint(size - 1))
	if size == 1 {
		idx = 0
	}
	if idx >= 32 {
		b := make([]byte, size)
		return &b
	}
	return bufferPools[idx].Get().(*[]byte)
}

// PutBuffer returns a buffer to the appropriate power-of-two pool.
func PutBuffer(b *[]byte) {
	if b == nil || cap(*b) == 0 {
		return
	}
	c := cap(*b)
	idx := bits.Len(uint(c - 1))
	if c == 1 {
		idx = 0
	}
	if idx < 32 {
		bufferPools[idx].Put(b)
	}
}
