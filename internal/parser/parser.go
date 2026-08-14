package parser

import "context"

// ParseResult holds the output of document parsing.
type ParseResult struct {
	Markdown string            // extracted text as Markdown
	Outlinks []string          // discovered URLs (HTML only)
	Tables   []TableData       // extracted tables (PDF only)
	Metadata map[string]string // title, author, page_count, etc.
}

// TableData holds a single extracted table.
type TableData struct {
	MarkdownTable string
	PageNumber    int
}

// DocumentParser extracts structured content from raw bytes.
type DocumentParser interface {
	Parse(ctx context.Context, content []byte, mimeType string) (*ParseResult, error)
	// SupportedTypes returns the MIME types this parser handles.
	SupportedTypes() []string
}
