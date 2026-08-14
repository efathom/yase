package memory

import (
	"sync"
	"testing"
)

func TestAllocAndRetrieve(t *testing.T) {
	arena, err := NewOffHeapArena(4096)
	if err != nil {
		t.Fatalf("NewOffHeapArena: %v", err)
	}
	defer arena.Close()

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
}

func TestAlignmentCorrectness(t *testing.T) {
	arena, err := NewOffHeapArena(4096)
	if err != nil {
		t.Fatalf("NewOffHeapArena: %v", err)
	}
	defer arena.Close()

	// Allocate vectors of varying lengths to exercise alignment padding
	lengths := []int{1, 3, 5, 7, 2, 4, 6}
	for _, l := range lengths {
		vec := make([]float32, l)
		for i := range vec {
			vec[i] = float32(i + 1)
		}
		offset, err := arena.AllocFloat32(vec)
		if err != nil {
			t.Fatalf("AllocFloat32 (len=%d): %v", l, err)
		}
		if offset%4 != 0 {
			t.Errorf("offset %d is not 4-byte aligned for vector of length %d", offset, l)
		}
		got := arena.GetFloat32(offset, l)
		for i := range vec {
			if got[i] != vec[i] {
				t.Errorf("len=%d index %d: got %f, want %f", l, i, got[i], vec[i])
			}
		}
	}
}

func TestBoundaryError(t *testing.T) {
	// Arena of exactly 20 bytes = 5 float32s
	arena, err := NewOffHeapArena(20)
	if err != nil {
		t.Fatalf("NewOffHeapArena: %v", err)
	}
	defer arena.Close()

	vec := []float32{1.0, 2.0, 3.0, 4.0, 5.0} // 20 bytes
	_, err = arena.AllocFloat32(vec)
	if err != nil {
		t.Fatalf("first alloc should succeed: %v", err)
	}

	// Next alloc should fail
	_, err = arena.AllocFloat32([]float32{6.0})
	if err != ErrArenaFull {
		t.Errorf("expected ErrArenaFull, got %v", err)
	}
}

func TestConcurrentAlloc(t *testing.T) {
	const numGoroutines = 100
	const vecLen = 10
	arenaSize := uint64(numGoroutines * vecLen * 4 * 2) // 2x headroom for alignment

	arena, err := NewOffHeapArena(arenaSize)
	if err != nil {
		t.Fatalf("NewOffHeapArena: %v", err)
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
				t.Errorf("goroutine %d: AllocFloat32: %v", id, err)
				return
			}
			results[id] = result{offset: off, vec: vec}
		}(g)
	}
	wg.Wait()

	// Verify no overlapping offsets and all vectors retrievable correctly
	offsets := make(map[uint64]int)
	for id, r := range results {
		if r.vec == nil {
			continue // errored goroutine
		}
		if prev, exists := offsets[r.offset]; exists {
			t.Errorf("goroutines %d and %d share offset %d", prev, id, r.offset)
		}
		offsets[r.offset] = id

		got := arena.GetFloat32(r.offset, vecLen)
		for i := range r.vec {
			if got[i] != r.vec[i] {
				t.Errorf("goroutine %d index %d: got %f, want %f", id, i, got[i], r.vec[i])
			}
		}
	}
}

func TestZeroSizeArena(t *testing.T) {
	_, err := NewOffHeapArena(0)
	if err == nil {
		t.Error("expected error for zero-size arena")
	}
}

func TestZeroLengthVector(t *testing.T) {
	arena, err := NewOffHeapArena(4096)
	if err != nil {
		t.Fatalf("NewOffHeapArena: %v", err)
	}
	defer arena.Close()

	_, err = arena.AllocFloat32([]float32{})
	if err == nil {
		t.Error("expected error for zero-length vector")
	}
}

func TestUsedBytes(t *testing.T) {
	arena, err := NewOffHeapArena(4096)
	if err != nil {
		t.Fatalf("NewOffHeapArena: %v", err)
	}
	defer arena.Close()

	if arena.UsedBytes() != 0 {
		t.Errorf("expected 0 used bytes, got %d", arena.UsedBytes())
	}

	arena.AllocFloat32([]float32{1.0, 2.0, 3.0}) // 12 bytes
	if arena.UsedBytes() != 12 {
		t.Errorf("expected 12 used bytes, got %d", arena.UsedBytes())
	}
}

func BenchmarkAllocFloat32(b *testing.B) {
	arena, err := NewOffHeapArena(uint64(b.N) * 768 * 4 * 2)
	if err != nil {
		b.Fatalf("NewOffHeapArena: %v", err)
	}
	defer arena.Close()

	vec := make([]float32, 768)
	for i := range vec {
		vec[i] = float32(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arena.AllocFloat32(vec)
	}
}

func BenchmarkAllocFloat32Concurrent(b *testing.B) {
	arena, err := NewOffHeapArena(uint64(b.N) * 768 * 4 * 2)
	if err != nil {
		b.Fatalf("NewOffHeapArena: %v", err)
	}
	defer arena.Close()

	vec := make([]float32, 768)
	for i := range vec {
		vec[i] = float32(i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			arena.AllocFloat32(vec)
		}
	})
}
