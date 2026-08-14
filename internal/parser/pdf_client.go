package parser

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PDFSidecarClient connects to a remote PDF parsing sidecar via gRPC.
// The sidecar runs Docling or LlamaParse for layout-aware PDF extraction.
type PDFSidecarClient struct {
	conn    *grpc.ClientConn
	addr    string
	timeout time.Duration
}

// NewPDFSidecarClient creates a gRPC client to the PDF parsing sidecar.
func NewPDFSidecarClient(addr string, timeout time.Duration) (*PDFSidecarClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", addr, err)
	}
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &PDFSidecarClient{conn: conn, addr: addr, timeout: timeout}, nil
}

// Parse sends raw PDF bytes to the sidecar and returns extracted markdown.
// This is a placeholder implementation — the actual proto-generated client
// will be wired in when parser.proto is compiled.
func (c *PDFSidecarClient) Parse(ctx context.Context, content []byte, mimeType string) (*ParseResult, error) {
	// TODO: Wire up generated ParserServiceClient from parser.proto
	// For now, return an error indicating the sidecar is not connected.
	return nil, fmt.Errorf("pdf sidecar at %s: proto client not yet generated (compile parser.proto)", c.addr)
}

// SupportedTypes returns the MIME types handled by the PDF sidecar.
func (c *PDFSidecarClient) SupportedTypes() []string {
	return []string{"application/pdf"}
}

// Close shuts down the gRPC connection.
func (c *PDFSidecarClient) Close() error {
	return c.conn.Close()
}
