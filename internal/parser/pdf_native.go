package parser

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ledongthuc/pdf"
)

// NativePDFParser extracts text from PDFs using a pure Go library.
// This is a lightweight fallback when the Docling sidecar is unavailable.
// It handles simple text-based PDFs but lacks layout analysis, table
// extraction, and OCR capabilities of the sidecar.
type NativePDFParser struct{}

// NewNativePDFParser creates a Go-native PDF text extractor.
func NewNativePDFParser() *NativePDFParser {
	return &NativePDFParser{}
}

func (p *NativePDFParser) Parse(_ context.Context, content []byte, _ string) (*ParseResult, error) {
	// ledongthuc/pdf requires a ReadSeeker, so write to temp file
	tmp, err := os.CreateTemp("", "yase-pdf-*.pdf")
	if err != nil {
		return nil, fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if _, err := io.Copy(tmp, bytes.NewReader(content)); err != nil {
		tmp.Close()
		return nil, fmt.Errorf("write temp: %w", err)
	}
	tmp.Close()

	f, reader, err := pdf.Open(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("pdf open: %w", err)
	}
	defer f.Close()

	var sb strings.Builder
	pageCount := reader.NumPage()

	for i := 1; i <= pageCount; i++ {
		page := reader.Page(i)
		if page.V.IsNull() {
			continue
		}
		text, err := page.GetPlainText(nil)
		if err != nil {
			continue // skip unreadable pages
		}
		trimmed := strings.TrimSpace(text)
		if trimmed != "" {
			if sb.Len() > 0 {
				sb.WriteString("\n\n")
			}
			sb.WriteString(trimmed)
		}
	}

	metadata := map[string]string{
		"page_count": fmt.Sprintf("%d", pageCount),
		"parser":     "native-go",
	}

	return &ParseResult{
		Markdown: sb.String(),
		Metadata: metadata,
	}, nil
}

func (p *NativePDFParser) SupportedTypes() []string {
	return []string{"application/pdf"}
}
