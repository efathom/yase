package parser

import (
	"context"
	"fmt"
	"strings"

	"github.com/efathom/yase/pkg/crawler"
)

// Router dispatches documents to the appropriate parser based on MIME type.
// HTML uses the built-in extractor. PDF uses a configurable parser (sidecar
// gRPC client or built-in Go extractor). Unknown types fall back to plain text.
type Router struct {
	pdfParser DocumentParser // optional: gRPC sidecar or Go-native PDF parser
}

// NewRouter creates a MIME-based document parser router.
// pdfParser can be nil if PDF support is not needed.
func NewRouter(pdfParser DocumentParser) *Router {
	return &Router{pdfParser: pdfParser}
}

// Parse routes the document to the appropriate parser based on MIME type.
func (r *Router) Parse(ctx context.Context, content []byte, mimeType string) (*ParseResult, error) {
	switch {
	case isHTML(mimeType):
		return r.parseHTML(content)
	case isPDF(mimeType):
		return r.parsePDF(ctx, content, mimeType)
	case isPlainText(mimeType):
		return &ParseResult{Markdown: string(content)}, nil
	default:
		// Best effort: treat as plain text
		return &ParseResult{Markdown: string(content)}, nil
	}
}

func (r *Router) parseHTML(content []byte) (*ParseResult, error) {
	doc, err := crawler.ExtractHTMLToMarkdown(content)
	if err != nil {
		return nil, fmt.Errorf("html extract: %w", err)
	}
	return &ParseResult{
		Markdown: doc.Markdown,
		Outlinks: doc.Outlinks,
	}, nil
}

func (r *Router) parsePDF(ctx context.Context, content []byte, mimeType string) (*ParseResult, error) {
	if r.pdfParser == nil {
		return nil, fmt.Errorf("pdf parsing not configured (no sidecar or built-in parser)")
	}
	return r.pdfParser.Parse(ctx, content, mimeType)
}

func isHTML(mime string) bool {
	return strings.Contains(mime, "html") || strings.Contains(mime, "xhtml")
}

func isPDF(mime string) bool {
	return strings.Contains(mime, "pdf")
}

func isPlainText(mime string) bool {
	return strings.Contains(mime, "text/plain") || mime == ""
}
