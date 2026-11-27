package index

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/efathom/yase/pkg/hnsw"
	"github.com/efathom/yase/pkg/memory"
	"github.com/efathom/yase/pkg/vector"
)

// On-disk layout of a persistent engine directory:
//
//	<dir>/bluge/               Bluge inverted index (durable per batch)
//	<dir>/CURRENT              name of the live generation, e.g. "gen-000003"
//	<dir>/gen-000003/arena.bin file-backed vector arena
//	<dir>/gen-000003/snapshot.bin
//
// A generation is replaced as a whole by compaction: the new generation is
// fully written before CURRENT is atomically switched to it.

const (
	currentFile  = "CURRENT"
	arenaFile    = "arena.bin"
	snapshotFile = "snapshot.bin"
	genPrefix    = "gen-"
)

var snapshotMagic = [4]byte{'Y', 'S', 'N', 'P'}

const snapshotVersion uint32 = 1

// EngineConfig configures a persistent HybridEngine.
type EngineConfig struct {
	Dir          string      // engine directory (created if missing)
	ArenaSize    uint64      // maximum vector arena size in bytes
	HNSW         hnsw.Config // graph hyperparameters (VecDim required)
	CentroidRate int
}

type engineStore struct {
	dir string
	gen int
}

func genName(g int) string { return fmt.Sprintf("%s%06d", genPrefix, g) }

func (s *engineStore) genDir(g int) string    { return filepath.Join(s.dir, genName(g)) }
func (s *engineStore) arenaPath(g int) string { return filepath.Join(s.genDir(g), arenaFile) }
func (s *engineStore) snapPath(g int) string  { return filepath.Join(s.genDir(g), snapshotFile) }

func (s *engineStore) readCurrent() (int, bool, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, currentFile))
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	name := strings.TrimSpace(string(b))
	g, err := strconv.Atoi(strings.TrimPrefix(name, genPrefix))
	if err != nil || !strings.HasPrefix(name, genPrefix) {
		return 0, false, fmt.Errorf("corrupt %s: %q", currentFile, name)
	}
	return g, true, nil
}

func (s *engineStore) writeCurrent(g int) error {
	return writeAtomicFile(filepath.Join(s.dir, currentFile), func(w io.Writer) error {
		_, err := io.WriteString(w, genName(g)+"\n")
		return err
	})
}

// removeStaleGenerations deletes every generation directory except keep.
func (s *engineStore) removeStaleGenerations(keep int) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), genPrefix) && e.Name() != genName(keep) {
			_ = os.RemoveAll(filepath.Join(s.dir, e.Name()))
		}
	}
}

// OpenHybridEngine opens (or creates) a durable engine in cfg.Dir.
//
// If a valid snapshot exists it is restored. Otherwise a fresh vector index is
// created; any documents already in the Bluge index are then missing vectors,
// which Reconcile rebuilds from their stored text. Callers should always run
// Reconcile after opening.
func OpenHybridEngine(cfg EngineConfig) (*HybridEngine, error) {
	if cfg.Dir == "" {
		return nil, errors.New("engine dir is required")
	}
	if cfg.HNSW.VecDim <= 0 {
		return nil, errors.New("engine vector dimension must be > 0")
	}
	if cfg.ArenaSize == 0 {
		cfg.ArenaSize = 64 << 30
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	store := &engineStore{dir: cfg.Dir}
	hnswCfg := engineGraphConfig(cfg.HNSW)

	bs, err := NewBlugeStore(filepath.Join(cfg.Dir, "bluge"))
	if err != nil {
		return nil, fmt.Errorf("bluge store: %w", err)
	}

	gen, ok, err := store.readCurrent()
	if err != nil {
		bs.Close()
		return nil, err
	}
	if ok {
		he, lerr := loadGeneration(store, gen, bs, hnswCfg, cfg)
		if lerr == nil {
			store.removeStaleGenerations(gen)
			return he, nil
		}
		if !errors.Is(lerr, os.ErrNotExist) {
			slog.Error("engine snapshot unusable; rebuilding vectors from the text index",
				"dir", cfg.Dir, "generation", gen, "error", lerr)
		}
	}

	// Fresh generation.
	gen++
	store.gen = gen
	if err := os.MkdirAll(store.genDir(gen), 0o755); err != nil {
		bs.Close()
		return nil, fmt.Errorf("mkdir generation: %w", err)
	}
	arena, err := memory.NewFileArena(store.arenaPath(gen), cfg.ArenaSize)
	if err != nil {
		bs.Close()
		return nil, fmt.Errorf("arena: %w", err)
	}
	he := newEngine(bs, arena, hnsw.NewGraph(arena, hnswCfg), hnswCfg, cfg.CentroidRate)
	he.store = store
	he.arenaCap = cfg.ArenaSize
	if err := he.Checkpoint(); err != nil {
		he.Close()
		return nil, fmt.Errorf("initial snapshot: %w", err)
	}
	if err := store.writeCurrent(gen); err != nil {
		he.Close()
		return nil, fmt.Errorf("write %s: %w", currentFile, err)
	}
	store.removeStaleGenerations(gen)
	return he, nil
}

func loadGeneration(store *engineStore, gen int, bs *BlugeStore, hnswCfg hnsw.Config, cfg EngineConfig) (*HybridEngine, error) {
	if _, err := os.Stat(store.snapPath(gen)); err != nil {
		return nil, err
	}
	arena, err := memory.OpenFileArena(store.arenaPath(gen))
	if err != nil {
		return nil, fmt.Errorf("open arena: %w", err)
	}
	he := newEngine(bs, arena, nil, hnswCfg, cfg.CentroidRate)
	he.store = &engineStore{dir: store.dir, gen: gen}
	he.arenaCap = cfg.ArenaSize
	if err := he.readSnapshot(store.snapPath(gen), hnswCfg); err != nil {
		arena.Close()
		return nil, err
	}
	*store = *he.store
	return he, nil
}

// Checkpoint durably persists the vector index. It blocks writers (not
// searches) for the duration. A no-op for in-memory engines.
func (he *HybridEngine) Checkpoint() error {
	if he.store == nil {
		return nil
	}
	he.writeMu.Lock()
	defer he.writeMu.Unlock()
	if err := he.acquire(); err != nil {
		return err
	}
	defer he.release()
	return he.checkpointLocked(he.store.gen)
}

// checkpointLocked writes the snapshot for generation g. Callers hold writeMu.
func (he *HybridEngine) checkpointLocked(g int) error {
	fa, ok := he.Arena.(*memory.FileArena)
	if !ok {
		return errors.New("persistent engine requires a file-backed arena")
	}
	if err := fa.Sync(); err != nil {
		return fmt.Errorf("arena sync: %w", err)
	}
	return writeAtomicFile(he.store.snapPath(g), he.writeSnapshot)
}

// SaveSnapshot is retained for callers of the earlier API; it is Checkpoint.
func (he *HybridEngine) SaveSnapshot() error { return he.Checkpoint() }

// IsPersistent reports whether the engine is backed by durable storage.
func (he *HybridEngine) IsPersistent() bool { return he.store != nil }

// writeSnapshot streams the vector-index state with a trailing CRC32.
func (he *HybridEngine) writeSnapshot(out io.Writer) error {
	crc := crc32.NewIEEE()
	w := io.MultiWriter(out, crc)

	hdr := make([]byte, 4+4+4+4+8+8)
	copy(hdr[0:4], snapshotMagic[:])
	binary.LittleEndian.PutUint32(hdr[4:8], snapshotVersion)
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(he.VecDim))
	binary.LittleEndian.PutUint32(hdr[12:16], uint32(he.CentroidRate))
	binary.LittleEndian.PutUint64(hdr[16:24], he.nextID.Load())
	binary.LittleEndian.PutUint64(hdr[24:32], he.deadBytes.Load())
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	if err := he.Graph.Snapshot(w); err != nil {
		return fmt.Errorf("graph: %w", err)
	}

	he.ifMu.RLock()
	defer he.ifMu.RUnlock()

	var b4 [4]byte
	var b12 [12]byte
	binary.LittleEndian.PutUint32(b4[:], uint32(len(he.invertedFiles)))
	if _, err := w.Write(b4[:]); err != nil {
		return err
	}
	for c, leaves := range he.invertedFiles {
		var e [8]byte
		binary.LittleEndian.PutUint32(e[0:4], c)
		binary.LittleEndian.PutUint32(e[4:8], uint32(len(leaves)))
		if _, err := w.Write(e[:]); err != nil {
			return err
		}
		for _, l := range leaves {
			binary.LittleEndian.PutUint32(b4[:], l)
			if _, err := w.Write(b4[:]); err != nil {
				return err
			}
		}
	}

	binary.LittleEndian.PutUint32(b4[:], uint32(len(he.offsets)))
	if _, err := w.Write(b4[:]); err != nil {
		return err
	}
	for id, off := range he.offsets {
		binary.LittleEndian.PutUint32(b12[0:4], id)
		binary.LittleEndian.PutUint64(b12[4:12], off)
		if _, err := w.Write(b12[:]); err != nil {
			return err
		}
	}

	binary.LittleEndian.PutUint32(b4[:], crc.Sum32())
	_, err := out.Write(b4[:])
	return err
}

// crcReader tees everything read through a CRC.
type crcReader struct {
	r   io.Reader
	crc hash.Hash32
}

func (c *crcReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.crc.Write(p[:n])
	return n, err
}

func (he *HybridEngine) readSnapshot(path string, hnswCfg hnsw.Config) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20)
	r := &crcReader{r: br, crc: crc32.NewIEEE()}

	hdr := make([]byte, 32)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	if [4]byte(hdr[0:4]) != snapshotMagic {
		return fmt.Errorf("bad snapshot magic %q", hdr[0:4])
	}
	if v := binary.LittleEndian.Uint32(hdr[4:8]); v != snapshotVersion {
		return fmt.Errorf("unsupported snapshot version %d", v)
	}
	if dim := int(binary.LittleEndian.Uint32(hdr[8:12])); dim != he.VecDim {
		return fmt.Errorf("snapshot vecDim %d != configured %d", dim, he.VecDim)
	}
	nextID := binary.LittleEndian.Uint64(hdr[16:24])
	dead := binary.LittleEndian.Uint64(hdr[24:32])

	g, err := hnsw.RestoreGraph(r, he.Arena, hnswCfg)
	if err != nil {
		return fmt.Errorf("restore graph: %w", err)
	}

	used := he.Arena.UsedBytes()
	vecBytes := uint64(he.VecDim * 4)
	readU32 := func() (uint32, error) {
		var b [4]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint32(b[:]), nil
	}

	nIF, err := readU32()
	if err != nil {
		return fmt.Errorf("read inverted files: %w", err)
	}
	inv := make(map[uint32][]uint32)
	leafOf := make(map[uint32]uint32)
	for i := uint32(0); i < nIF; i++ {
		c, err1 := readU32()
		n, err2 := readU32()
		if err := errors.Join(err1, err2); err != nil {
			return fmt.Errorf("read inverted file %d: %w", i, err)
		}
		if n > 1<<28 {
			return fmt.Errorf("corrupt inverted file %d: %d leaves", c, n)
		}
		leaves := make([]uint32, 0, n)
		for j := uint32(0); j < n; j++ {
			l, err := readU32()
			if err != nil {
				return fmt.Errorf("read leaf: %w", err)
			}
			leaves = append(leaves, l)
			leafOf[l] = c
		}
		inv[c] = leaves
	}

	nOff, err := readU32()
	if err != nil {
		return fmt.Errorf("read offsets: %w", err)
	}
	offsets := make(map[uint32]uint64, nOff)
	for i := uint32(0); i < nOff; i++ {
		var b [12]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return fmt.Errorf("read offset %d: %w", i, err)
		}
		off := binary.LittleEndian.Uint64(b[4:12])
		if off+vecBytes > used {
			return fmt.Errorf("corrupt snapshot: offset %d beyond arena watermark %d", off, used)
		}
		offsets[binary.LittleEndian.Uint32(b[0:4])] = off
	}

	want := r.crc.Sum32()
	var tail [4]byte
	if _, err := io.ReadFull(br, tail[:]); err != nil {
		return fmt.Errorf("read checksum: %w", err)
	}
	if binary.LittleEndian.Uint32(tail[:]) != want {
		return errors.New("snapshot checksum mismatch")
	}

	he.Graph = g
	he.EfSearch = g.EfSearch()
	he.hnswCfg = g.Config()
	he.invertedFiles = inv
	he.leafCentroid = leafOf
	he.offsets = offsets
	he.nextID.Store(nextID)
	he.deadBytes.Store(dead)
	return nil
}

// ReconcileStats summarizes a Reconcile pass.
type ReconcileStats struct {
	Documents      int // documents in the text index
	ReEmbedded     int // documents whose missing vectors were rebuilt
	MissingVectors int // documents still without a vector (no embedder given or embed failed)
	DroppedVectors int // vectors whose document no longer exists
}

// EmbedBatchFunc embeds texts, returning one vector per text.
type EmbedBatchFunc func(ctx context.Context, texts []string) ([][]float32, error)

// Reconcile makes the vector index agree with the (durable) text index:
// vectors of documents that are gone are dropped, and documents that have no
// vector — written after the last checkpoint before a crash, or from before
// persistence was enabled — are re-embedded from their stored text. It also
// advances the ID allocator past every existing document.
func (he *HybridEngine) Reconcile(ctx context.Context, embed EmbedBatchFunc, batchSize int) (ReconcileStats, error) {
	var st ReconcileStats
	he.writeMu.Lock()
	defer he.writeMu.Unlock()
	if err := he.acquire(); err != nil {
		return st, err
	}
	defer he.release()
	if batchSize <= 0 {
		batchSize = 64
	}

	present := roaring.New()
	type pending struct {
		id   uint32
		text string
	}
	var batch []pending
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		texts := make([]string, len(batch))
		for i, p := range batch {
			texts[i] = p.text
		}
		vecs, err := embed(ctx, texts)
		if err != nil || len(vecs) != len(batch) {
			st.MissingVectors += len(batch)
			slog.Warn("reconcile: embedding failed", "docs", len(batch), "error", err)
			batch = batch[:0]
			return nil
		}
		for i, p := range batch {
			if len(vecs[i]) != he.VecDim {
				st.MissingVectors++
				continue
			}
			if err := he.restoreVector(p.id, vecs[i]); err != nil {
				return err
			}
			st.ReEmbedded++
		}
		batch = batch[:0]
		return nil
	}

	err := he.BlugeStore.ForEach(ctx, nil, true, func(h SearchHit) error {
		id, ok := parseDocID(h.ID)
		if !ok {
			return nil
		}
		st.Documents++
		present.Add(id)
		he.observeID(id)
		if _, has := he.VectorOffset(id); has {
			return nil
		}
		if embed == nil {
			st.MissingVectors++
			return nil
		}
		batch = append(batch, pending{id: id, text: h.Content})
		if len(batch) >= batchSize {
			return flush()
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err != nil {
		return st, err
	}

	var drop []uint32
	he.ifMu.RLock()
	for id := range he.offsets {
		if !present.Contains(id) {
			drop = append(drop, id)
		}
	}
	he.ifMu.RUnlock()
	st.DroppedVectors = he.removeVectors(drop)
	if st.ReEmbedded > 0 || st.DroppedVectors > 0 {
		he.gen.Add(1)
	}
	return st, nil
}

// restoreVector inserts a vector for a document already in the text index.
func (he *HybridEngine) restoreVector(id uint32, vec []float32) error {
	nv := vector.Normalized(vec)
	off, err := he.Arena.AllocFloat32(nv)
	if err != nil {
		return fmt.Errorf("arena alloc: %w", err)
	}
	isCentroid := id%uint32(he.CentroidRate) == 0 || he.Graph.Len() == 0
	return he.insertVector(id, nv, off, isCentroid)
}

// writeAtomicFile writes via a temp file then renames for crash safety,
// fsyncing the file and its containing directory for durability.
func writeAtomicFile(path string, writeFn func(w io.Writer) error) error {
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	if err := writeFn(bw); err != nil {
		f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		dir.Close()
	}
	return nil
}
