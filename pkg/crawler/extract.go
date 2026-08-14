package crawler

import (
	"bytes"
	"net/url"
	"strings"

	md "github.com/JohannesKaufmann/html-to-markdown"
	"github.com/PuerkitoBio/goquery"
)

// ExtractedDocument holds the Markdown text and outbound links extracted from HTML.
type ExtractedDocument struct {
	Markdown string
	Outlinks []string
}

// ExtractHTMLToMarkdown parses raw HTML and converts it to Markdown without
// resolving relative links (equivalent to an empty base URL).
func ExtractHTMLToMarkdown(htmlPayload []byte) (*ExtractedDocument, error) {
	return ExtractHTMLToMarkdownWithBase(htmlPayload, "")
}

// ExtractHTMLToMarkdownWithBase parses raw HTML, aggressively prunes non-content
// elements (scripts, styles, navigation, ads, iframes), extracts outbound links
// (resolving relative links against baseURL), and converts the cleaned HTML to
// Markdown.
func ExtractHTMLToMarkdownWithBase(htmlPayload []byte, baseURL string) (*ExtractedDocument, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(htmlPayload))
	if err != nil {
		return nil, err
	}

	// 1. Aggressive DOM pruning — strip visual clutter & ad tracking
	doc.Find("script, style, nav, footer, aside, .ad-banner, .tracker, iframe").Remove()

	// 2. Extract outbound links for the crawler frontier, resolving relative
	// links against the page URL.
	var base *url.URL
	if baseURL != "" {
		base, _ = url.Parse(baseURL)
	}

	var outlinks []string
	doc.Find("a[href]").Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if !exists || href == "" || strings.HasPrefix(href, "javascript:") ||
			strings.HasPrefix(href, "mailto:") || strings.HasPrefix(href, "#") {
			return
		}
		abs := href
		if !strings.HasPrefix(href, "http") {
			if base == nil {
				return // skip relative links when no base URL is available
			}
			if resolved, err := base.Parse(href); err == nil {
				abs = resolved.String()
			}
		}
		outlinks = append(outlinks, abs)
	})

	// 3. Convert cleaned HTML to Markdown
	htmlStr, err := doc.Find("body").Html()
	if err != nil {
		return nil, err
	}

	converter := md.NewConverter("", true, nil)
	markdownText, err := converter.ConvertString(htmlStr)
	if err != nil {
		return nil, err
	}

	return &ExtractedDocument{
		Markdown: strings.TrimSpace(markdownText),
		Outlinks: outlinks,
	}, nil
}
