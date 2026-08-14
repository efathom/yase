package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/efathom/yase/pkg/hnsw"
	"github.com/efathom/yase/pkg/memory"
)

// PersistConfig specifies paths for HybridEngine snapshot files.
type PersistConfig struct {
	Dir       string // directory containing graph/IF/offsets snapshot files
	ArenaPath string // path to the file-backed arena (separate, since it's mmap'd in-place)
}

func (pc PersistConfig) graphPath() string   { return filepath.Join(pc.Dir, "graph.bin") }
func (pc PersistConfig) ifPath() string      { return filepath.Join(pc.Dir, "inverted_files.bin") }
func (pc PersistConfig) offsetsPath() string { return filepath.Join(pc.Dir, "offsets.bin") }

// SaveSnapshot persists the HybridEngine state to disk.
// The arena is synced via FileArena.Sync() if file-backed, otherwise skipped.
// Graph topology, inverted files, and vector offsets are written atomically
// via temp files + rename.
func (he *HybridEngine) SaveSnapshot(cfg PersistConfig) error {
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	// Take a full lock so no Ingest/Delete can interleave with the snapshot,
	// guaranteeing a consistent cut across arena, graph, inverted files, and
	// vector offsets.
	he.lifecycleMu.Lock()
	defer he.lifecycleMu.Unlock()

	// Sync arena if file-backed
	if fa, ok := he.Arena.(*memory.FileArena); ok {
		if err := fa.Sync(); err != nil {
			return fmt.Errorf("arena sync: %w", err)
		}
	}

	// Graph topology
	if err := writeAtomicFile(cfg.graphPath(), func(w io.Writer) error {
		return he.Graph.Snapshot(w)
	}); err != nil {
		return fmt.Errorf("graph snapshot: %w", err)
	}

	// Inverted files
	if err := writeAtomicFile(cfg.ifPath(), func(w io.Writer) error {
		return he.snapshotInvertedFiles(w)
	}); err != nil {
		return fmt.Errorf("inverted files snapshot: %w", err)
	}

	// Vector offsets
	if err := writeAtomicFile(cfg.offsetsPath(), func(w io.Writer) error {
		return he.snapshotVectorOffsets(w)
	}); err != nil {
		return fmt.Errorf("vector offsets snapshot: %w", err)
	}

	return nil
}

// snapshotInvertedFiles writes centroid→leaf posting lists, framed with
// magic + version + CRC32 so corruption is detected on restore.
// Format: magic(4) + version(4) + count(4) + entries + crc32(4).
func (he *HybridEngine) snapshotInvertedFiles(w io.Writer) error {
	he.ifMu.RLock()
	defer he.ifMu.RUnlock()

	var payload bytes.Buffer
	var countBuf [4]byte
	binary.LittleEndian.PutUint32(countBuf[:], uint32(len(he.invertedFiles)))
	payload.Write(countBuf[:])

	for centroidID, leaves := range he.invertedFiles {
		var entry [8]byte
		binary.LittleEndian.PutUint32(entry[0:4], centroidID)
		binary.LittleEndian.PutUint32(entry[4:8], uint32(len(leaves)))
		payload.Write(entry[:])

		leafBuf := make([]byte, len(leaves)*4)
		for i, leafID := range leaves {
			binary.LittleEndian.PutUint32(leafBuf[i*4:(i+1)*4], leafID)
		}
		payload.Write(leafBuf)
	}

	return writeFrame(w, ifMagic, payload.Bytes())
}

// snapshotVectorOffsets writes docID→arena offset pairs, framed with
// magic + version + CRC32.
func (he *HybridEngine) snapshotVectorOffsets(w io.Writer) error {
	var entries []struct {
		id     uint32
		offset uint64
	}
	he.VectorOffsets.Range(func(key, value any) bool {
		entries = append(entries, struct {
			id     uint32
			offset uint64
		}{id: key.(uint32), offset: value.(uint64)})
		return true
	})

	var payload bytes.Buffer
	var countBuf [4]byte
	binary.LittleEndian.PutUint32(countBuf[:], uint32(len(entries)))
	payload.Write(countBuf[:])

	for _, e := range entries {
		var entry [12]byte
		binary.LittleEndian.PutUint32(entry[0:4], e.id)
		binary.LittleEndian.PutUint64(entry[4:12], e.offset)
		payload.Write(entry[:])
	}

	return writeFrame(w, offMagic, payload.Bytes())
}

// LoadHybridEngine restores a HybridEngine from a snapshot directory.
// The arena must be a FileArena opened from the persisted arena file.
// Bluge indexes are restored from indexPath (Bluge handles its own persistence).
func LoadHybridEngine(cfg PersistConfig, indexPath string, vecDim int, centroidRate int) (*HybridEngine, error) {
	// Open arena
	arena, err := memory.OpenFileArena(cfg.ArenaPath)
	if err != nil {
		return nil, fmt.Errorf("open arena: %w", err)
	}

	// Restore graph
	graphFile, err := os.Open(cfg.graphPath())
	if err != nil {
		arena.Close()
		return nil, fmt.Errorf("open graph: %w", err)
	}
	hnswCfg := hnsw.DefaultConfig(vecDim)
	graph, err := hnsw.RestoreGraph(graphFile, arena, hnswCfg)
	graphFile.Close()
	if err != nil {
		arena.Close()
		return nil, fmt.Errorf("restore graph: %w", err)
	}

	// Open Bluge (it persists its own state)
	bs, err := NewBlugeStore(indexPath)
	if err != nil {
		arena.Close()
		return nil, fmt.Errorf("bluge store: %w", err)
	}

	he := &HybridEngine{
		BlugeStore:    bs,
		Arena:         arena,
		Graph:         graph,
		invertedFiles: make(map[uint32][]uint32),
		VecDim:        vecDim,
		CentroidRate:  centroidRate,
		EfSearch:      graph.EfSearch(),
	}

	// Restore inverted files
	ifFile, err := os.Open(cfg.ifPath())
	if err != nil {
		he.Close()
		return nil, fmt.Errorf("open inverted files: %w", err)
	}
	if err := he.restoreInvertedFiles(ifFile); err != nil {
		ifFile.Close()
		he.Close()
		return nil, fmt.Errorf("restore inverted files: %w", err)
	}
	ifFile.Close()

	// Restore vector offsets
	offsetsFile, err := os.Open(cfg.offsetsPath())
	if err != nil {
		he.Close()
		return nil, fmt.Errorf("open offsets: %w", err)
	}
	if err := he.restoreVectorOffsets(offsetsFile); err != nil {
		offsetsFile.Close()
		he.Close()
		return nil, fmt.Errorf("restore offsets: %w", err)
	}
	offsetsFile.Close()

	return he, nil
}

func (he *HybridEngine) restoreInvertedFiles(r io.Reader) error {
	payload, err := readFrame(r, ifMagic)
	if err != nil {
		return err
	}
	if len(payload) < 4 {
		return fmt.Errorf("inverted files snapshot truncated")
	}
	count := int(binary.LittleEndian.Uint32(payload[0:4]))
	pos := 4

	he.ifMu.Lock()
	defer he.ifMu.Unlock()

	for i := 0; i < count; i++ {
		if pos+8 > len(payload) {
			return fmt.Errorf("inverted files snapshot truncated at entry %d", i)
		}
		centroidID := binary.LittleEndian.Uint32(payload[pos : pos+4])
		leafCount := int(binary.LittleEndian.Uint32(payload[pos+4 : pos+8]))
		pos += 8

		// Bounds-check before allocating to avoid OOM on corrupt files.
		if leafCount < 0 || leafCount > (len(payload)-pos)/4 {
			return fmt.Errorf("inverted files snapshot: corrupt leafCount %d", leafCount)
		}

		leaves := make([]uint32, leafCount)
		for j := 0; j < leafCount; j++ {
			leaves[j] = binary.LittleEndian.Uint32(payload[pos+j*4 : pos+(j+1)*4])
		}
		pos += leafCount * 4
		he.invertedFiles[centroidID] = leaves
	}
	return nil
}

func (he *HybridEngine) restoreVectorOffsets(r io.Reader) error {
	payload, err := readFrame(r, offMagic)
	if err != nil {
		return err
	}
	if len(payload) < 4 {
		return fmt.Errorf("offsets snapshot truncated")
	}
	count := int(binary.LittleEndian.Uint32(payload[0:4]))
	pos := 4

	for i := 0; i < count; i++ {
		if pos+12 > len(payload) {
			return fmt.Errorf("offsets snapshot truncated at entry %d", i)
		}
		docID := binary.LittleEndian.Uint32(payload[pos : pos+4])
		offset := binary.LittleEndian.Uint64(payload[pos+4 : pos+12])
		pos += 12
		he.VectorOffsets.Store(docID, offset)
	}
	return nil
}

// Snapshot framing: magic(4) + version(4) + payload + crc32(4).
const snapshotVersion = 1

const (
	ifMagic  = "YSIF" // inverted files
	offMagic = "YSOF" // vector offsets
)

func writeFrame(w io.Writer, magic string, payload []byte) error {
	var buf bytes.Buffer
	buf.WriteString(magic)
	var ver [4]byte
	binary.LittleEndian.PutUint32(ver[:], snapshotVersion)
	buf.Write(ver[:])
	buf.Write(payload)

	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.ChecksumIEEE(buf.Bytes()))
	buf.Write(crc[:])

	_, err := w.Write(buf.Bytes())
	return err
}

func readFrame(r io.Reader, magic string) ([]byte, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(data) < 12 {
		return nil, fmt.Errorf("snapshot too short")
	}
	if string(data[0:4]) != magic {
		return nil, fmt.Errorf("bad snapshot magic %q", string(data[0:4]))
	}
	if v := binary.LittleEndian.Uint32(data[4:8]); v != snapshotVersion {
		return nil, fmt.Errorf("unsupported snapshot version %d", v)
	}
	want := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(data[:len(data)-4]) != want {
		return nil, fmt.Errorf("snapshot checksum mismatch")
	}
	return data[8 : len(data)-4], nil
}

// writeAtomicFile writes via a temp file then renames for crash safety,
// fsyncing the file and its containing directory for durability.
func writeAtomicFile(path string, writeFn func(w io.Writer) error) error {
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	if err := writeFn(f); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	f.Close()

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// Fsync the directory so the rename itself is durable.
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		dir.Close()
	}
	return nil
}

// NewHybridEngineWithArena creates a HybridEngine using a provided arena
// (either OffHeapArena or FileArena).
func NewHybridEngineWithArena(indexPath string, arena memory.Arena, vecDim int, centroidRate int) (*HybridEngine, error) {
	return NewHybridEngineWithArenaConfig(indexPath, arena, hnsw.DefaultConfig(vecDim), centroidRate)
}

// NewHybridEngineWithArenaConfig creates a HybridEngine using a provided arena
// and custom HNSW hyperparameters.
func NewHybridEngineWithArenaConfig(indexPath string, arena memory.Arena, hnswCfg hnsw.Config, centroidRate int) (*HybridEngine, error) {
	bs, err := NewBlugeStore(indexPath)
	if err != nil {
		return nil, fmt.Errorf("bluge store: %w", err)
	}

	graph := hnsw.NewGraph(arena, hnswCfg)

	return &HybridEngine{
		BlugeStore:    bs,
		Arena:         arena,
		Graph:         graph,
		invertedFiles: make(map[uint32][]uint32),
		VecDim:        hnswCfg.VecDim,
		CentroidRate:  centroidRate,
		EfSearch:      hnswCfg.EfSearch,
		ifMu:          sync.RWMutex{},
	}, nil
}
