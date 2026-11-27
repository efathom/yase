package index

import (
	"fmt"
	"os"
	"sort"

	"github.com/efathom/yase/pkg/hnsw"
	"github.com/efathom/yase/pkg/memory"
)

// compactMinDeadBytes is the smallest amount of garbage worth compacting.
const compactMinDeadBytes = 16 << 20

// MaybeCompact compacts the engine when dead vector bytes exceed ratio of the
// arena watermark (and at least compactMinDeadBytes). It reports whether a
// compaction ran.
func (he *HybridEngine) MaybeCompact(ratio float64) (bool, error) {
	dead := he.deadBytes.Load()
	used := he.ArenaUsedBytes()
	if dead < compactMinDeadBytes || used == 0 || float64(dead) < ratio*float64(used) {
		return false, nil
	}
	return true, he.Compact()
}

// Compact reclaims arena space held by deleted or replaced vectors and
// rebuilds the HNSW graph and inverted files from the live documents, using
// the engine's configured HNSW parameters.
//
// Writes are blocked for the duration; searches keep running against the old
// structures until the new ones are swapped in. Persistent engines build a new
// generation on disk and switch CURRENT to it only once it is complete, so a
// crash mid-compaction leaves the previous generation intact.
func (he *HybridEngine) Compact() error {
	he.writeMu.Lock()
	defer he.writeMu.Unlock()
	if err := he.acquire(); err != nil {
		return err
	}

	newArena, newGraph, newIF, newLeaf, newOffsets, newGen, err := he.rebuild()
	he.release()
	if err != nil {
		return err
	}

	// Swap under the exclusive lifecycle lock (waits for in-flight searches).
	he.lifecycleMu.Lock()
	if he.closed.Load() {
		he.lifecycleMu.Unlock()
		newArena.Close()
		return ErrEngineClosed
	}
	oldArena := he.Arena
	oldGen := 0
	if he.store != nil {
		oldGen = he.store.gen
	}
	he.Arena = newArena
	he.Graph = newGraph
	he.ifMu.Lock()
	he.invertedFiles, he.leafCentroid, he.offsets = newIF, newLeaf, newOffsets
	he.ifMu.Unlock()
	he.deadBytes.Store(0)
	he.gen.Add(1)

	var persistErr error
	if he.store != nil {
		he.store.gen = newGen
		if persistErr = he.checkpointLocked(newGen); persistErr == nil {
			persistErr = he.store.writeCurrent(newGen)
		}
	}
	he.lifecycleMu.Unlock()

	oldArena.Close()
	if he.store != nil && persistErr == nil {
		_ = os.RemoveAll(he.store.genDir(oldGen))
	}
	if persistErr != nil {
		return fmt.Errorf("persist compacted generation: %w", persistErr)
	}
	return nil
}

func (he *HybridEngine) rebuild() (memory.Arena, *hnsw.Graph, map[uint32][]uint32, map[uint32]uint32, map[uint32]uint64, int, error) {
	type liveDoc struct {
		id  uint32
		vec []float32
	}
	he.ifMu.RLock()
	live := make([]liveDoc, 0, len(he.offsets))
	for id, off := range he.offsets {
		cp := make([]float32, he.VecDim)
		copy(cp, he.Arena.GetFloat32(off, he.VecDim))
		live = append(live, liveDoc{id: id, vec: cp})
	}
	he.ifMu.RUnlock()
	sort.Slice(live, func(i, j int) bool { return live[i].id < live[j].id })

	var (
		arena  memory.Arena
		newGen int
		err    error
	)
	if he.store != nil {
		newGen = he.store.gen + 1
		if err = os.MkdirAll(he.store.genDir(newGen), 0o755); err != nil {
			return nil, nil, nil, nil, nil, 0, err
		}
		arena, err = memory.NewFileArena(he.store.arenaPath(newGen), he.arenaCap)
	} else {
		arena, err = memory.NewOffHeapArena(he.arenaCap)
	}
	if err != nil {
		return nil, nil, nil, nil, nil, 0, fmt.Errorf("new arena: %w", err)
	}
	fail := func(e error) (memory.Arena, *hnsw.Graph, map[uint32][]uint32, map[uint32]uint32, map[uint32]uint64, int, error) {
		arena.Close()
		if he.store != nil {
			_ = os.RemoveAll(he.store.genDir(newGen))
		}
		return nil, nil, nil, nil, nil, 0, e
	}

	cfg := engineGraphConfig(he.Graph.Config())
	graph := hnsw.NewGraph(arena, cfg)
	offsets := make(map[uint32]uint64, len(live))
	inv := make(map[uint32][]uint32)
	leafOf := make(map[uint32]uint32)
	rate := uint32(he.CentroidRate)

	// Pass 1: centroids (every rate-th ID, plus the first doc so one exists).
	for i, d := range live {
		if d.id%rate != 0 && i != 0 {
			continue
		}
		off, err := arena.AllocFloat32(d.vec)
		if err != nil {
			return fail(fmt.Errorf("alloc centroid %d: %w", d.id, err))
		}
		if err := graph.InsertAt(d.id, d.vec, off); err != nil {
			return fail(fmt.Errorf("insert centroid %d: %w", d.id, err))
		}
		offsets[d.id] = off
	}
	// Pass 2: leaves routed to their nearest centroid.
	for i, d := range live {
		if d.id%rate == 0 || i == 0 {
			continue
		}
		off, err := arena.AllocFloat32(d.vec)
		if err != nil {
			return fail(fmt.Errorf("alloc leaf %d: %w", d.id, err))
		}
		offsets[d.id] = off
		if res := graph.Search(d.vec, 1, 10); len(res) > 0 {
			c := res[0].ID
			inv[c] = append(inv[c], d.id)
			leafOf[d.id] = c
		}
	}
	return arena, graph, inv, leafOf, offsets, newGen, nil
}
