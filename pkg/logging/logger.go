// Package logging provides structured logging for YASE services.
// Uses Go 1.21+ log/slog with JSON or text output.
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
)

// Config configures the structured logger.
type Config struct {
	Format string `mapstructure:"format"` // "json" or "text" (default "json")
	Level  string `mapstructure:"level"`  // "debug", "info", "warn", "error" (default "info")
	Output string `mapstructure:"output"` // "stderr", "stdout", or file path (default "stderr")
}

var (
	currentLogFile   *os.File
	currentLogFileMu sync.Mutex
)

// Setup initializes the global slog logger based on configuration.
// Closes any previously opened log file to prevent file descriptor leaks.
func Setup(cfg Config) {
	level := parseLevel(cfg.Level)
	output := parseOutput(cfg.Output)

	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: level}

	switch strings.ToLower(cfg.Format) {
	case "text":
		handler = slog.NewTextHandler(output, opts)
	default: // "json"
		handler = slog.NewJSONHandler(output, opts)
	}

	slog.SetDefault(slog.New(handler))
}

// Close releases any file handle opened by Setup.
func Close() {
	currentLogFileMu.Lock()
	defer currentLogFileMu.Unlock()
	if currentLogFile != nil {
		currentLogFile.Close()
		currentLogFile = nil
	}
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func parseOutput(s string) io.Writer {
	switch strings.ToLower(s) {
	case "stdout":
		return os.Stdout
	case "", "stderr":
		return os.Stderr
	default:
		f, err := os.OpenFile(s, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return os.Stderr
		}
		currentLogFileMu.Lock()
		if currentLogFile != nil {
			currentLogFile.Close()
		}
		currentLogFile = f
		currentLogFileMu.Unlock()
		return f
	}
}
