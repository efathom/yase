package memory

import (
	"path/filepath"
	"sync"
	"testing"
)

func TestFileArenaAllocAndRetrieve(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arena.bin")

	arena, err := NewFileArena(path, 4096)
	if err != nil {
		t.Fatalf("NewFileArena: %v", err)
	}

	vec := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	offset, err := arena.AllocFloat32(vec)
	if err != nil {
		t.Fatalf("AllocFloat32: %v", err)
	}

	got := arena.GetFloat32(offset, len(vec))
	for i := range vec {
		if got[i] != vec[i] {
			t.Errorf("index %d: got %f, want %f", i, got[i], vec[i])
		}
	}

	if err := arena.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestFileArenaPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arena.bin")

	// Write
	arena, err := NewFileArena(path, 4096)
	if err != nil {
		t.Fatalf("NewFileArena: %v", err)
	}

	vecs := [][]float32{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
		{7.0, 8.0, 9.0},
	}
	offsets := make([]uint64, len(vecs))
	for i, v := range vecs {
		off, err := arena.AllocFloat32(v)
		if err != nil {
			t.Fatalf("AllocFloat32[%d]: %v", i, err)
		}
		offsets[i] = off
	}

	usedBefore := arena.UsedBytes()
	if err := arena.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen
	arena2, err := OpenFileArena(path)
	if err != nil {
		t.Fatalf("OpenFileArena: %v", err)
	}
	defer arena2.Close()

	if arena2.UsedBytes() != usedBefore {
		t.Errorf("UsedBytes: got %d, want %d", arena2.UsedBytes(), usedBefore)
	}

	// Verify all vectors survived
	for i, v := range vecs {
		got := arena2.GetFloat32(offsets[i], len(v))
		for j := range v {
			if got[j] != v[j] {
				t.Errorf("vec[%d][%d]: got %f, want %f", i, j, got[j], v[j])
			}
		}
	}
}

func TestFileArenaFull(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arena.bin")

	// 64 header + 20 data = 84 bytes total, but we specify data size
	arena, err := NewFileArena(path, 20) // 20 bytes = 5 float32s
	if err != nil {
		t.Fatalf("NewFileArena: %v", err)
	}
	defer arena.Close()

	vec := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	_, err = arena.AllocFloat32(vec)
	if err != nil {
		t.Fatalf("first alloc should succeed: %v", err)
	}

	_, err = arena.AllocFloat32([]float32{6.0})
	if err != ErrArenaFull {
		t.Errorf("expected ErrArenaFull, got %v", err)
	}
}

func TestFileArenaConcurrentAlloc(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arena.bin")
	const numGoroutines = 50
	const vecLen = 10

	arenaSize := uint64(numGoroutines * vecLen * 4 * 2)
	arena, err := NewFileArena(path, arenaSize)
	if err != nil {
		t.Fatalf("NewFileArena: %v", err)
	}
	defer arena.Close()

	type result struct {
		offset uint64
		vec    []float32
	}
	results := make([]result, numGoroutines)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			vec := make([]float32, vecLen)
			for i := range vec {
				vec[i] = float32(id*1000 + i)
			}
			off, err := arena.AllocFloat32(vec)
			if err != nil {
				t.Errorf("goroutine %d: %v", id, err)
				return
			}
			results[id] = result{offset: off, vec: vec}
		}(g)
	}
	wg.Wait()

	for id, r := range results {
		if r.vec == nil {
			continue
		}
		got := arena.GetFloat32(r.offset, vecLen)
		for i := range r.vec {
			if got[i] != r.vec[i] {
				t.Errorf("goroutine %d index %d: got %f, want %f", id, i, got[i], r.vec[i])
			}
		}
	}
}

func TestFileArenaInvalidMagic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arena.bin")

	// Create a valid arena first
	arena, _ := NewFileArena(path, 256)
	arena.Close()

	// Corrupt the magic bytes by reopening and writing garbage
	arena2, _ := OpenFileArena(path)
	arena2.data[0] = 0xFF
	arena2.data[1] = 0xFF
	arena2.Close()

	// Should fail to open
	_, err := OpenFileArena(path)
	if err == nil {
		t.Error("expected error for corrupted magic")
	}
}

func TestFileArenaImplementsInterface(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arena.bin")
	arena, err := NewFileArena(path, 4096)
	if err != nil {
		t.Fatalf("NewFileArena: %v", err)
	}
	defer arena.Close()

	// Verify FileArena satisfies the Arena interface
	var _ Arena = arena
}
