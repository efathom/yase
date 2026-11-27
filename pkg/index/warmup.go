package index

import (
	"log/slog"
	"os"
	"time"

	"github.com/efathom/yase/pkg/memory"
)

// Warmup prefaults mmap pages into memory to avoid cold-start latency.
// Touches every page in the arena to trigger page faults upfront.
func (he *HybridEngine) Warmup() {
	if he.acquire() != nil {
		return
	}
	defer he.release()
	start := time.Now()

	switch a := he.Arena.(type) {
	case *memory.FileArena:
		warmupArena(a)
	case *memory.OffHeapArena:
		// Anonymous mmap pages are already resident (zeroed by kernel)
		slog.Info("warmup: anonymous arena, skipping prefault")
		return
	default:
		return
	}

	elapsed := time.Since(start)
	slog.Info("warmup: arena prefaulted", "duration", elapsed, "nodes", he.Graph.Len())
}

func warmupArena(a *memory.FileArena) {
	used := a.UsedBytes()
	if used == 0 {
		return
	}
	pageSize := uint64(os.Getpagesize())
	seg := a.SegmentSize()
	var sink byte
	// Touch one byte per page, one segment at a time (a view never spans
	// two segments).
	for off := uint64(0); off < used; off += seg {
		n := seg
		if used-off < n {
			n = used - off
		}
		data := a.GetBytes(off, int(n))
		for i := uint64(0); i < uint64(len(data)); i += pageSize {
			sink ^= data[i]
		}
	}
	_ = sink
}
