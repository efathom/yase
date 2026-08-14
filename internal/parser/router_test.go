package parser

import (
	"context"
	"testing"
)

func TestRouterHTML(t *testing.T) {
	router := NewRouter(nil)

	html := []byte(`<html><body>
		<h1>Hello World</h1>
		<p>This is a test page.</p>
		<a href="https://example.com/link1">Link 1</a>
		<script>alert('xss')</script>
		<nav>navigation</nav>
	</body></html>`)

	result, err := router.Parse(context.Background(), html, "text/html")
	if err != nil {
		t.Fatalf("Parse HTML: %v", err)
	}

	if result.Markdown == "" {
		t.Error("expected non-empty markdown")
	}
	if len(result.Outlinks) != 1 {
		t.Errorf("expected 1 outlink, got %d", len(result.Outlinks))
	}
	if len(result.Outlinks) > 0 && result.Outlinks[0] != "https://example.com/link1" {
		t.Errorf("outlink: got %q", result.Outlinks[0])
	}

	t.Logf("HTML markdown: %s", result.Markdown)
}

func TestRouterXHTML(t *testing.T) {
	router := NewRouter(nil)

	xhtml := []byte(`<html><body><p>XHTML content</p></body></html>`)
	result, err := router.Parse(context.Background(), xhtml, "application/xhtml+xml")
	if err != nil {
		t.Fatalf("Parse XHTML: %v", err)
	}
	if result.Markdown == "" {
		t.Error("expected non-empty markdown for XHTML")
	}
}

func TestRouterPlainText(t *testing.T) {
	router := NewRouter(nil)

	text := []byte("Just plain text content here.")
	result, err := router.Parse(context.Background(), text, "text/plain")
	if err != nil {
		t.Fatalf("Parse text: %v", err)
	}
	if result.Markdown != "Just plain text content here." {
		t.Errorf("expected verbatim text, got %q", result.Markdown)
	}
}

func TestRouterEmptyMIME(t *testing.T) {
	router := NewRouter(nil)

	result, err := router.Parse(context.Background(), []byte("data"), "")
	if err != nil {
		t.Fatalf("Parse empty MIME: %v", err)
	}
	if result.Markdown != "data" {
		t.Errorf("expected verbatim, got %q", result.Markdown)
	}
}

func TestRouterUnknownMIME(t *testing.T) {
	router := NewRouter(nil)

	result, err := router.Parse(context.Background(), []byte("binary stuff"), "application/octet-stream")
	if err != nil {
		t.Fatalf("Parse unknown: %v", err)
	}
	if result.Markdown != "binary stuff" {
		t.Errorf("fallback should return verbatim text")
	}
}

func TestRouterPDFWithoutParser(t *testing.T) {
	router := NewRouter(nil) // no PDF parser configured

	_, err := router.Parse(context.Background(), []byte("%PDF-1.4"), "application/pdf")
	if err == nil {
		t.Error("expected error when no PDF parser configured")
	}
}

func TestRouterPDFWithMockParser(t *testing.T) {
	mock := &mockPDFParser{
		result: &ParseResult{
			Markdown: "# Extracted PDF Content\n\nPage 1 text here.",
			Metadata: map[string]string{"page_count": "3"},
		},
	}
	router := NewRouter(mock)

	result, err := router.Parse(context.Background(), []byte("%PDF-1.4..."), "application/pdf")
	if err != nil {
		t.Fatalf("Parse PDF: %v", err)
	}
	if result.Markdown != "# Extracted PDF Content\n\nPage 1 text here." {
		t.Errorf("unexpected markdown: %q", result.Markdown)
	}
	if result.Metadata["page_count"] != "3" {
		t.Errorf("metadata page_count: got %q", result.Metadata["page_count"])
	}
}

func TestRouterPDFParserError(t *testing.T) {
	mock := &mockPDFParser{err: context.DeadlineExceeded}
	router := NewRouter(mock)

	_, err := router.Parse(context.Background(), []byte("%PDF"), "application/pdf")
	if err == nil {
		t.Error("expected error from failing PDF parser")
	}
}

// ── Mock ──

type mockPDFParser struct {
	result *ParseResult
	err    error
}

func (m *mockPDFParser) Parse(_ context.Context, _ []byte, _ string) (*ParseResult, error) {
	return m.result, m.err
}

func (m *mockPDFParser) SupportedTypes() []string {
	return []string{"application/pdf"}
}
