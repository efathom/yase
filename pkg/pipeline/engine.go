package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// HookEvent defines the lifecycle events where WASM plugins can be invoked.
type HookEvent string

const (
	PostChunk HookEvent = "PostChunk"
	PreEmbed  HookEvent = "PreEmbed"
	PreIndex  HookEvent = "PreIndex"
)

// WasmEngine manages embedded WASM plugin execution with zero IPC latency.
// Plugins execute inside Go's process memory in sandboxed linear memory.
type WasmEngine struct {
	runtime wazero.Runtime
	plugins map[HookEvent][]api.Module
	mu      sync.RWMutex
}

// NewWasmEngine creates a new WASM runtime for plugin management.
func NewWasmEngine(ctx context.Context) *WasmEngine {
	return &WasmEngine{
		runtime: wazero.NewRuntime(ctx),
		plugins: make(map[HookEvent][]api.Module),
	}
}

// LoadPlugin compiles and instantiates a sandboxed WASM module for the given hook event.
// The module must export: malloc, free, and extract_metadata.
func (w *WasmEngine) LoadPlugin(ctx context.Context, wasmBytes []byte, event HookEvent) error {
	mod, err := w.runtime.Instantiate(ctx, wasmBytes)
	if err != nil {
		return fmt.Errorf("wasm instantiate: %w", err)
	}

	// Verify required exports
	if mod.ExportedFunction("extract_metadata") == nil {
		mod.Close(ctx)
		return fmt.Errorf("WASM module missing required export: extract_metadata")
	}
	if mod.ExportedFunction("malloc") == nil {
		mod.Close(ctx)
		return fmt.Errorf("WASM module missing required export: malloc")
	}

	w.mu.Lock()
	w.plugins[event] = append(w.plugins[event], mod)
	w.mu.Unlock()
	return nil
}

// ExecuteHooks runs all plugins registered for the given event, chaining their
// outputs. Uses linear memory interop: allocate in WASM → write Go string →
// call extract_metadata → read result back → free both allocations.
func (w *WasmEngine) ExecuteHooks(ctx context.Context, event HookEvent, chunkText string) (string, error) {
	w.mu.RLock()
	modules := w.plugins[event]
	w.mu.RUnlock()

	processedText := chunkText

	for _, mod := range modules {
		malloc := mod.ExportedFunction("malloc")
		free := mod.ExportedFunction("free")
		extractFn := mod.ExportedFunction("extract_metadata")

		if extractFn == nil || malloc == nil {
			continue
		}

		textBytes := []byte(processedText)
		textLen := uint64(len(textBytes))

		// 1. Allocate memory inside the WASM sandbox
		allocRes, err := malloc.Call(ctx, textLen)
		if err != nil {
			return "", fmt.Errorf("wasm malloc: %w", err)
		}
		ptr := allocRes[0]

		// 2. Write Go string into WASM linear memory
		if !mod.Memory().Write(uint32(ptr), textBytes) {
			if free != nil {
				_, _ = free.Call(ctx, ptr)
			}
			return "", fmt.Errorf("failed to write to WASM memory space")
		}

		// 3. Execute the hook (e.g., NER, PII redaction)
		res, err := extractFn.Call(ctx, ptr, textLen)
		if err != nil {
			if free != nil {
				_, _ = free.Call(ctx, ptr)
			}
			return "", fmt.Errorf("wasm extract_metadata: %w", err)
		}

		// 4. Read result from WASM memory
		// ABI: returns packed 64-bit uint (pointer << 32 | size)
		newPtr := uint32(res[0] >> 32)
		newSize := uint32(res[0] & 0xFFFFFFFF)

		outBytes, ok := mod.Memory().Read(newPtr, newSize)
		if !ok {
			return "", fmt.Errorf("failed to read WASM result memory")
		}
		processedText = string(outBytes)

		// 5. Free allocations to prevent sandbox OOM
		if free != nil {
			_, _ = free.Call(ctx, ptr)
			if uint32(ptr) != newPtr {
				_, _ = free.Call(ctx, uint64(newPtr))
			}
		}
	}

	return processedText, nil
}

// Close shuts down the WASM runtime and all loaded modules.
func (w *WasmEngine) Close(ctx context.Context) error {
	return w.runtime.Close(ctx)
}
