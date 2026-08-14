package crawler

import (
	"strings"
	"testing"
)

func TestExtractRemovesScriptsAndAds(t *testing.T) {
	html := []byte(`<html><body>
		<script>alert('xss')</script>
		<style>.hidden{display:none}</style>
		<nav><a href="/nav">Nav</a></nav>
		<div class="ad-banner">BUY NOW</div>
		<div class="tracker">tracking pixel</div>
		<iframe src="https://ads.example.com"></iframe>
		<footer>Copyright 2024</footer>
		<aside>Sidebar</aside>
		<p>Important content here.</p>
	</body></html>`)

	doc, err := ExtractHTMLToMarkdown(html)
	if err != nil {
		t.Fatalf("ExtractHTMLToMarkdown: %v", err)
	}

	// Content should be preserved
	if !strings.Contains(doc.Markdown, "Important content here") {
		t.Error("expected content to be preserved")
	}

	// Pruned elements should be absent
	for _, bad := range []string{"alert", "display:none", "BUY NOW", "tracking pixel", "ads.example.com", "Copyright", "Sidebar"} {
		if strings.Contains(doc.Markdown, bad) {
			t.Errorf("expected %q to be removed from markdown", bad)
		}
	}
}

func TestExtractOutlinks(t *testing.T) {
	html := []byte(`<html><body>
		<a href="https://example.com/page1">Link 1</a>
		<a href="https://example.com/page2">Link 2</a>
		<a href="http://other.com">Link 3</a>
		<a href="/relative">Relative Link</a>
		<a href="ftp://files.example.com">FTP</a>
		<a href="https://example.com/page3">Link 4</a>
		<a href="https://example.com/page4">Link 5</a>
	</body></html>`)

	doc, err := ExtractHTMLToMarkdown(html)
	if err != nil {
		t.Fatalf("ExtractHTMLToMarkdown: %v", err)
	}

	// Should extract only http/https links (5 out of 7)
	if len(doc.Outlinks) != 5 {
		t.Errorf("expected 5 outlinks, got %d: %v", len(doc.Outlinks), doc.Outlinks)
	}

	// Relative and ftp links should not be included
	for _, link := range doc.Outlinks {
		if !strings.HasPrefix(link, "http") {
			t.Errorf("unexpected non-http link: %s", link)
		}
	}
}

func TestExtractMalformedHTML(t *testing.T) {
	// Malformed HTML should not panic
	html := []byte(`<html><body><p>Unclosed paragraph<div>Nested <b>badly</p></div>
		<a href="https://ok.com">Still works</a></body>`)

	doc, err := ExtractHTMLToMarkdown(html)
	if err != nil {
		t.Fatalf("ExtractHTMLToMarkdown: %v", err)
	}

	if doc == nil {
		t.Fatal("expected non-nil document")
	}

	// Should still extract the valid link
	found := false
	for _, link := range doc.Outlinks {
		if link == "https://ok.com" {
			found = true
		}
	}
	if !found {
		t.Error("expected to find https://ok.com in outlinks")
	}
}

func TestExtractEmptyBody(t *testing.T) {
	html := []byte(`<html><body></body></html>`)
	doc, err := ExtractHTMLToMarkdown(html)
	if err != nil {
		t.Fatalf("ExtractHTMLToMarkdown: %v", err)
	}
	if len(doc.Outlinks) != 0 {
		t.Errorf("expected 0 outlinks, got %d", len(doc.Outlinks))
	}
}
