package pipeline

import (
	"context"
	"os"
	"testing"
)

func TestNewWasmEngine(t *testing.T) {
	ctx := context.Background()
	engine := NewWasmEngine(ctx)
	defer engine.Close(ctx)

	if engine == nil {
		t.Fatal("expected non-nil engine")
	}
	if engine.plugins == nil {
		t.Fatal("expected non-nil plugins map")
	}
}

func TestLoadPluginMissingExport(t *testing.T) {
	ctx := context.Background()
	engine := NewWasmEngine(ctx)
	defer engine.Close(ctx)

	// A minimal valid WASM module that exports nothing useful.
	// This is the smallest valid wasm: (module)
	// magic + version + empty
	minimalWasm := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}

	err := engine.LoadPlugin(ctx, minimalWasm, PostChunk)
	if err == nil {
		t.Error("expected error for WASM module missing extract_metadata export")
	}
}

func TestLoadPluginInvalidWasm(t *testing.T) {
	ctx := context.Background()
	engine := NewWasmEngine(ctx)
	defer engine.Close(ctx)

	err := engine.LoadPlugin(ctx, []byte("not wasm"), PostChunk)
	if err == nil {
		t.Error("expected error for invalid WASM bytes")
	}
}

func TestExecuteHooksNoPlugins(t *testing.T) {
	ctx := context.Background()
	engine := NewWasmEngine(ctx)
	defer engine.Close(ctx)

	result, err := engine.ExecuteHooks(ctx, PostChunk, "hello world")
	if err != nil {
		t.Fatalf("ExecuteHooks: %v", err)
	}
	if result != "hello world" {
		t.Errorf("expected unchanged text, got %q", result)
	}
}

func TestLoadAndExecutePlugin(t *testing.T) {
	ctx := context.Background()
	engine := NewWasmEngine(ctx)
	defer engine.Close(ctx)

	// Load the test WASM plugin if it exists
	wasmPath := "testdata/uppercase.wasm"
	wasmBytes, err := os.ReadFile(wasmPath)
	if err != nil {
		t.Skipf("skipping: test WASM plugin not found at %s (build it with TinyGo/Rust)", wasmPath)
	}

	err = engine.LoadPlugin(ctx, wasmBytes, PostChunk)
	if err != nil {
		t.Fatalf("LoadPlugin: %v", err)
	}

	result, err := engine.ExecuteHooks(ctx, PostChunk, "hello world")
	if err != nil {
		t.Fatalf("ExecuteHooks: %v", err)
	}
	if result != "HELLO WORLD" {
		t.Errorf("expected %q, got %q", "HELLO WORLD", result)
	}
}
