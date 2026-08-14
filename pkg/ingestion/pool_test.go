package ingestion

import (
	"testing"
)

func TestGetPutRoundTrip(t *testing.T) {
	buf := GetBuffer(100)
	if buf == nil {
		t.Fatal("expected non-nil buffer")
	}
	if cap(*buf) < 100 {
		t.Errorf("buffer capacity %d < requested 100", cap(*buf))
	}

	// Write some data
	(*buf)[0] = 42
	PutBuffer(buf)

	// Get again — should work (may or may not be same buffer)
	buf2 := GetBuffer(100)
	if buf2 == nil {
		t.Fatal("expected non-nil buffer on second get")
	}
	if cap(*buf2) < 100 {
		t.Errorf("buffer capacity %d < requested 100", cap(*buf2))
	}
	PutBuffer(buf2)
}

func TestPowerOfTwoSizing(t *testing.T) {
	tests := []struct {
		requested   int
		minExpected int
	}{
		{1, 1},
		{2, 2},
		{3, 4},
		{5, 8},
		{100, 128},
		{1000, 1024},
		{1025, 2048},
	}

	for _, tt := range tests {
		buf := GetBuffer(tt.requested)
		if buf == nil {
			t.Fatalf("GetBuffer(%d) returned nil", tt.requested)
		}
		if cap(*buf) < tt.minExpected {
			t.Errorf("GetBuffer(%d): cap=%d, want >= %d", tt.requested, cap(*buf), tt.minExpected)
		}
		PutBuffer(buf)
	}
}

func TestGetBufferZero(t *testing.T) {
	buf := GetBuffer(0)
	if buf != nil {
		t.Error("expected nil for size 0")
	}
}

func TestGetBufferNegative(t *testing.T) {
	buf := GetBuffer(-1)
	if buf != nil {
		t.Error("expected nil for negative size")
	}
}

func TestPutBufferNil(t *testing.T) {
	// Should not panic
	PutBuffer(nil)
	empty := make([]byte, 0)
	PutBuffer(&empty)
}

func BenchmarkGetPutBuffer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := GetBuffer(4096)
		PutBuffer(buf)
	}
}

func BenchmarkMakeSlice(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = make([]byte, 4096)
	}
}
