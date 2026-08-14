package index

import (
	"fmt"
	"sort"

	"github.com/efathom/yase/pkg/hnsw"
	"github.com/efathom/yase/pkg/memory"
)

// Compact reclaims arena space and rebuilds the HNSW graph + inverted files
// from the live (non-deleted) documents. Deleted documents are not in
// VectorOffsets, so they are naturally dropped. The Bluge index is unchanged
// (it already excludes deleted documents).
//
// Currently supported only for in-memory (OffHeapArena) engines. The caller
// must not hold any engine locks; Compact acquires lifecycleMu exclusively.
func (he *HybridEngine) Compact() error {
	he.lifecycleMu.Lock()
	defer he.lifecycleMu.Unlock()

	if he.closed.Load() {
		return ErrEngineClosed
	}

	oldArena := he.Arena
	if _, ok := oldArena.(*memory.OffHeapArena); !ok {
		return fmt.Errorf("compaction is only supported for in-memory (OffHeapArena) engines")
	}

	// Collect live vectors, copying out of the old arena before it is released.
	type liveDoc struct {
		id  uint32
		vec []float32
	}
	var live []liveDoc
	he.VectorOffsets.Range(func(k, v any) bool {
		id := k.(uint32)
		off := v.(uint64)
		raw := oldArena.GetFloat32(off, he.VecDim)
		cp := make([]float32, he.VecDim)
		copy(cp, raw)
		live = append(live, liveDoc{id: id, vec: cp})
		return true
	})

	if len(live) == 0 {
		return nil
	}

	// Deterministic order so compaction is reproducible.
	sort.Slice(live, func(i, j int) bool { return live[i].id < live[j].id })

	// Determine centroid set: every CentroidRate-th doc, and always the first
	// doc so at least one centroid exists (mirrors the Ingest routing rule).
	isCentroid := make(map[uint32]bool, len(live)/max(he.CentroidRate, 1)+1)
	haveCentroid := false
	for i, d := range live {
		if d.id%uint32(he.CentroidRate) == 0 || i == 0 {
			isCentroid[d.id] = true
			haveCentroid = true
		}
	}
	if !haveCentroid {
		isCentroid[live[0].id] = true
	}

	// New arena sized to the live set with headroom.
	liveBytes := uint64(len(live) * he.VecDim * 4)
	newSize := liveBytes * 2
	if newSize < 1<<20 {
		newSize = 1 << 20
	}
	newArena, err := memory.NewOffHeapArena(newSize)
	if err != nil {
		return fmt.Errorf("new arena: %w", err)
	}

	graph := hnsw.NewGraph(newArena, hnsw.DefaultConfig(he.VecDim))
	newOffsets := make(map[uint32]uint64)
	newIF := make(map[uint32][]uint32)

	// Pass 1: insert centroids.
	for _, d := range live {
		if !isCentroid[d.id] {
			continue
		}
		off, err := newArena.AllocFloat32(d.vec)
		if err != nil {
			newArena.Close()
			return fmt.Errorf("alloc centroid %d: %w", d.id, err)
		}
		if err := graph.InsertAt(d.id, d.vec, off); err != nil {
			newArena.Close()
			return fmt.Errorf("insert centroid %d: %w", d.id, err)
		}
		newOffsets[d.id] = off
	}

	// Pass 2: leaves routed to their nearest centroid.
	for _, d := range live {
		if isCentroid[d.id] {
			continue
		}
		off, err := newArena.AllocFloat32(d.vec)
		if err != nil {
			newArena.Close()
			return fmt.Errorf("alloc leaf %d: %w", d.id, err)
		}
		newOffsets[d.id] = off
		if graph.Len() > 0 {
			if res := graph.Search(d.vec, 1, 10); len(res) > 0 {
				centroidID := res[0].ID
				newIF[centroidID] = append(newIF[centroidID], d.id)
			}
		}
	}

	// Swap in the new structures (safe: exclusive lifecycle lock held).
	he.Arena = newArena
	he.Graph = graph
	he.invertedFiles = newIF

	// Rebuild the offset map in place (sync.Map cannot be copied).
	he.VectorOffsets.Range(func(k, _ any) bool {
		he.VectorOffsets.Delete(k)
		return true
	})
	for id, off := range newOffsets {
		he.VectorOffsets.Store(id, off)
	}

	oldArena.Close()
	return nil
}
