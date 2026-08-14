package parser

import (
	"context"
	"os"
	"os/exec"
	"testing"
)

func TestNativePDFParserWithRealPDF(t *testing.T) {
	// Generate a minimal PDF using Python (if available) or skip
	pdfPath := generateTestPDF(t)
	if pdfPath == "" {
		t.Skip("could not generate test PDF (python3 not available)")
	}
	defer os.Remove(pdfPath)

	content, err := os.ReadFile(pdfPath)
	if err != nil {
		t.Fatalf("read PDF: %v", err)
	}

	parser := NewNativePDFParser()
	result, err := parser.Parse(context.Background(), content, "application/pdf")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if result.Markdown == "" {
		t.Error("expected non-empty text from PDF")
	}
	if result.Metadata["parser"] != "native-go" {
		t.Errorf("parser metadata: got %q", result.Metadata["parser"])
	}

	t.Logf("Extracted %d chars, page_count=%s", len(result.Markdown), result.Metadata["page_count"])
	t.Logf("Content preview: %.200s", result.Markdown)
}

func TestNativePDFParserInvalidPDF(t *testing.T) {
	parser := NewNativePDFParser()
	_, err := parser.Parse(context.Background(), []byte("not a pdf"), "application/pdf")
	if err == nil {
		t.Error("expected error for invalid PDF")
	}
}

func TestNativePDFParserSupportedTypes(t *testing.T) {
	parser := NewNativePDFParser()
	types := parser.SupportedTypes()
	if len(types) != 1 || types[0] != "application/pdf" {
		t.Errorf("supported types: got %v", types)
	}
}

// generateTestPDF creates a minimal PDF using Python's reportlab or fpdf.
// Returns the temp file path, or empty string if Python is unavailable.
func generateTestPDF(t *testing.T) string {
	t.Helper()

	tmp, err := os.CreateTemp("", "yase-test-*.pdf")
	if err != nil {
		return ""
	}
	tmp.Close()
	path := tmp.Name()

	// Try using Python to generate a simple PDF
	script := `
import sys
try:
    from fpdf import FPDF
except ImportError:
    try:
        from reportlab.pdfgen import canvas
        c = canvas.Canvas(sys.argv[1])
        c.drawString(100, 750, "YASE Test Document")
        c.drawString(100, 700, "This is a test PDF for the native Go parser.")
        c.drawString(100, 650, "It contains simple text content.")
        c.save()
        sys.exit(0)
    except ImportError:
        sys.exit(1)

pdf = FPDF()
pdf.add_page()
pdf.set_font("Helvetica", size=12)
pdf.cell(200, 10, text="YASE Test Document", new_x="LMARGIN", new_y="NEXT")
pdf.cell(200, 10, text="This is a test PDF for the native Go parser.", new_x="LMARGIN", new_y="NEXT")
pdf.cell(200, 10, text="It contains simple text content.", new_x="LMARGIN", new_y="NEXT")
pdf.output(sys.argv[1])
`

	cmd := exec.Command("python3", "-c", script, path)
	if err := cmd.Run(); err != nil {
		os.Remove(path)
		return ""
	}

	info, err := os.Stat(path)
	if err != nil || info.Size() == 0 {
		os.Remove(path)
		return ""
	}

	return path
}
