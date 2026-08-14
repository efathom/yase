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

	pageSize := os.Getpagesize()
	// Read one byte per page to trigger page fault
	data := a.GetBytes(0, int(used))
	var sink byte
	for i := 0; i < len(data); i += pageSize {
		sink ^= data[i]
	}
	_ = sink
}
