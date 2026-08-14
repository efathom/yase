package hnsw

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync/atomic"

	"github.com/efathom/yase/pkg/memory"
)

// Binary format:
//   Header: magic(4) + version(4) + nodeCount(4) + maxLevel(4) + entryPointID(4)
//           + vecDim(4) + M(4) + Mmax0(4) + EfConstruction(4) + EfSearch(4) = 40 bytes
//   Per node: ID(4) + Level(4) + VectorOffset(8) + numLayers(4)
//     Per layer: edgeCount(4) + edges(edgeCount * 4)

var graphMagic = [4]byte{'H', 'N', 'S', 'W'}

const graphVersion uint32 = 2

const graphHeaderSize = 40

// Snapshot serializes the graph topology to a binary format.
// Acquires a read lock for the duration of the write.
func (g *Graph) Snapshot(w io.Writer) error {
	g.mu.RLock()
	defer g.mu.RUnlock()

	ep := g.entryPoint.Load()
	var entryPointID uint32
	if ep != nil {
		entryPointID = ep.ID
	}

	// Header
	header := make([]byte, graphHeaderSize)
	copy(header[0:4], graphMagic[:])
	binary.LittleEndian.PutUint32(header[4:8], graphVersion)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(g.nodes)))
	binary.LittleEndian.PutUint32(header[12:16], uint32(g.maxLevel))
	binary.LittleEndian.PutUint32(header[16:20], entryPointID)
	binary.LittleEndian.PutUint32(header[20:24], uint32(g.cfg.VecDim))
	binary.LittleEndian.PutUint32(header[24:28], uint32(g.cfg.M))
	binary.LittleEndian.PutUint32(header[28:32], uint32(g.cfg.Mmax0))
	binary.LittleEndian.PutUint32(header[32:36], uint32(g.cfg.EfConstruction))
	binary.LittleEndian.PutUint32(header[36:40], uint32(g.cfg.EfSearch))

	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Sort node IDs for deterministic output
	ids := make([]uint32, 0, len(g.nodes))
	for id := range g.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Per node
	for _, id := range ids {
		node := g.nodes[id]
		numLayers := node.Level + 1

		// Node header: ID(4) + Level(4) + VectorOffset(8) + BinaryVectorOffset(8) + BinaryVectorLen(4) + numLayers(4) = 32 bytes
		nodeBuf := make([]byte, 32)
		binary.LittleEndian.PutUint32(nodeBuf[0:4], node.ID)
		binary.LittleEndian.PutUint32(nodeBuf[4:8], uint32(node.Level))
		binary.LittleEndian.PutUint64(nodeBuf[8:16], node.VectorOffset)
		binary.LittleEndian.PutUint64(nodeBuf[16:24], node.BinaryVectorOffset)
		binary.LittleEndian.PutUint32(nodeBuf[24:28], uint32(node.BinaryVectorLen))
		binary.LittleEndian.PutUint32(nodeBuf[28:32], uint32(numLayers))

		if _, err := w.Write(nodeBuf); err != nil {
			return fmt.Errorf("write node %d: %w", id, err)
		}

		// Per layer: edgeCount(4) + edges
		for layer := 0; layer < numLayers; layer++ {
			edges := node.GetEdges(layer)
			edgeCountBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(edgeCountBuf, uint32(len(edges)))
			if _, err := w.Write(edgeCountBuf); err != nil {
				return fmt.Errorf("write edge count node %d layer %d: %w", id, layer, err)
			}

			if len(edges) > 0 {
				edgeBuf := make([]byte, len(edges)*4)
				for i, e := range edges {
					binary.LittleEndian.PutUint32(edgeBuf[i*4:(i+1)*4], e)
				}
				if _, err := w.Write(edgeBuf); err != nil {
					return fmt.Errorf("write edges node %d layer %d: %w", id, layer, err)
				}
			}
		}
	}

	return nil
}

// RestoreGraph deserializes a graph from its binary snapshot format.
// The persisted hyperparameters (M/Mmax0/EfConstruction/EfSearch) are applied
// to the returned graph; the persisted vecDim must match cfg.VecDim.
func RestoreGraph(r io.Reader, arena memory.Arena, cfg Config) (*Graph, error) {
	// Read header
	header := make([]byte, graphHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	var magic [4]byte
	copy(magic[:], header[0:4])
	if magic != graphMagic {
		return nil, fmt.Errorf("invalid graph magic: %v", magic)
	}

	version := binary.LittleEndian.Uint32(header[4:8])
	if version != graphVersion {
		return nil, fmt.Errorf("unsupported graph version: %d", version)
	}

	nodeCount := binary.LittleEndian.Uint32(header[8:12])
	maxLevel := int(binary.LittleEndian.Uint32(header[12:16]))
	entryPointID := binary.LittleEndian.Uint32(header[16:20])
	persistedVecDim := int(binary.LittleEndian.Uint32(header[20:24]))

	if persistedVecDim != cfg.VecDim {
		return nil, fmt.Errorf("vecDim mismatch: snapshot=%d config=%d", persistedVecDim, cfg.VecDim)
	}

	// Apply persisted hyperparameters so future inserts are consistent.
	cfg.M = int(binary.LittleEndian.Uint32(header[24:28]))
	cfg.Mmax0 = int(binary.LittleEndian.Uint32(header[28:32]))
	cfg.EfConstruction = int(binary.LittleEndian.Uint32(header[32:36]))
	cfg.EfSearch = int(binary.LittleEndian.Uint32(header[36:40]))
	if cfg.M < 2 {
		cfg.M = 16
	}
	if cfg.Mmax0 < cfg.M {
		cfg.Mmax0 = 2 * cfg.M
	}
	if cfg.EfConstruction <= 0 {
		cfg.EfConstruction = 200
	}
	if cfg.EfSearch <= 0 {
		cfg.EfSearch = 50
	}

	g := NewGraph(arena, cfg)
	g.maxLevel = maxLevel

	// Read nodes
	for i := uint32(0); i < nodeCount; i++ {
		nodeBuf := make([]byte, 32)
		if _, err := io.ReadFull(r, nodeBuf); err != nil {
			return nil, fmt.Errorf("read node %d: %w", i, err)
		}

		id := binary.LittleEndian.Uint32(nodeBuf[0:4])
		level := int(binary.LittleEndian.Uint32(nodeBuf[4:8]))
		vectorOffset := binary.LittleEndian.Uint64(nodeBuf[8:16])
		binaryVectorOffset := binary.LittleEndian.Uint64(nodeBuf[16:24])
		binaryVectorLen := int(binary.LittleEndian.Uint32(nodeBuf[24:28]))
		numLayers := int(binary.LittleEndian.Uint32(nodeBuf[28:32]))

		node := &Node{
			ID:                 id,
			Level:              level,
			VectorOffset:       vectorOffset,
			BinaryVectorOffset: binaryVectorOffset,
			BinaryVectorLen:    binaryVectorLen,
			Edges:              make([]atomic.Pointer[[]uint32], numLayers),
		}

		for layer := 0; layer < numLayers; layer++ {
			edgeCountBuf := make([]byte, 4)
			if _, err := io.ReadFull(r, edgeCountBuf); err != nil {
				return nil, fmt.Errorf("read edge count node %d layer %d: %w", id, layer, err)
			}
			edgeCount := int(binary.LittleEndian.Uint32(edgeCountBuf))

			edges := make([]uint32, edgeCount)
			if edgeCount > 0 {
				edgeBuf := make([]byte, edgeCount*4)
				if _, err := io.ReadFull(r, edgeBuf); err != nil {
					return nil, fmt.Errorf("read edges node %d layer %d: %w", id, layer, err)
				}
				for j := 0; j < edgeCount; j++ {
					edges[j] = binary.LittleEndian.Uint32(edgeBuf[j*4 : (j+1)*4])
				}
			}
			node.Edges[layer].Store(&edges)
		}

		g.nodes[id] = node
	}

	// Set entry point
	if ep, ok := g.nodes[entryPointID]; ok {
		g.entryPoint.Store(ep)
	}

	return g, nil
}
