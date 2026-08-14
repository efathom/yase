package integration

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/efathom/yase/pkg/connector"

	_ "github.com/efathom/yase/internal/connector/s3store"
)

func TestS3ConnectorE2E(t *testing.T) {
	endpoint, _, _ := startMinio(t)
	ctx := context.Background()

	bucket := "test-connector"
	createBucket(t, endpoint, bucket)

	// Seed test files
	seedS3Files(t, endpoint, bucket, map[string]string{
		"docs/readme.md":        "# YASE\nA hybrid search engine in Go.",
		"docs/architecture.md":  "# Architecture\nHNSW + BM25 with RRF fusion.",
		"docs/notes.txt":        "Some plain text notes.",
		"images/logo.png":       "fake-png-bytes",
		"docs/report.pdf":       "fake-pdf-bytes",
	})

	// Create S3 connector
	conn, err := connector.DefaultRegistry.Create(connector.ConnectorConfig{
		Type: "s3",
		Config: map[string]interface{}{
			"endpoint":        endpoint,
			"bucket":          bucket,
			"prefix":          "docs/",
			"region":          "us-east-1",
			"force_path_style": true,
			"access_key":      "minioadmin",
			"secret_key":      "minioadmin",
			"file_extensions":  []interface{}{".md", ".txt"},
		},
		Auth: &connector.AuthConfig{Method: ""},
	})
	if err != nil {
		t.Fatalf("Create s3 connector: %v", err)
	}
	defer conn.Close()

	// Validate
	if err := conn.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// Discover
	catalog, err := conn.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	t.Logf("Discovered %d streams", len(catalog.Streams))

	// Read files (only .md and .txt under docs/ prefix)
	state := connector.NewSyncState()
	records, errs := conn.Read(ctx, []connector.ConfiguredStream{
		{Name: "files", SyncMode: connector.FullRefresh},
	}, state)

	var collected []connector.Record
	for r := range records {
		collected = append(collected, r)
		t.Logf("File: key=%s mime=%s size=%s", r.Metadata["key"], r.MimeType, r.Metadata["size"])
	}
	for err := range errs {
		t.Errorf("Error: %v", err)
	}

	// Should get 3 files: readme.md, architecture.md, notes.txt
	// NOT logo.png (wrong extension) or report.pdf (wrong extension)
	if len(collected) != 3 {
		t.Fatalf("expected 3 files (.md + .txt), got %d", len(collected))
	}

	// Verify content
	for _, r := range collected {
		if r.Metadata["source"] != "s3" {
			t.Errorf("source: got %q", r.Metadata["source"])
		}
		if r.Metadata["bucket"] != bucket {
			t.Errorf("bucket: got %q", r.Metadata["bucket"])
		}
		if len(r.Content) == 0 {
			t.Errorf("empty content for %s", r.ID)
		}
	}
}

func TestS3ConnectorIncremental(t *testing.T) {
	endpoint, _, _ := startMinio(t)
	ctx := context.Background()

	bucket := "test-incremental"
	createBucket(t, endpoint, bucket)

	seedS3Files(t, endpoint, bucket, map[string]string{
		"file1.md": "content 1",
	})

	conn, err := connector.DefaultRegistry.Create(connector.ConnectorConfig{
		Type: "s3",
		Config: map[string]interface{}{
			"endpoint":        endpoint,
			"bucket":          bucket,
			"region":          "us-east-1",
			"force_path_style": true,
			"access_key":      "minioadmin",
			"secret_key":      "minioadmin",
		},
		Auth: &connector.AuthConfig{Method: ""},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer conn.Close()

	// First sync
	state := connector.NewSyncState()
	records, errs := conn.Read(ctx, []connector.ConfiguredStream{
		{Name: "files", SyncMode: connector.Incremental},
	}, state)

	count := 0
	for range records {
		count++
	}
	for err := range errs {
		t.Errorf("Error: %v", err)
	}
	t.Logf("First sync: %d files", count)

	// Verify state saved
	raw := state.GetStreamState("files")
	if raw == nil {
		t.Fatal("expected state saved")
	}
	t.Logf("State: %s", raw)
}

// seedS3Files uploads test files to MinIO.
func seedS3Files(t *testing.T, endpoint, bucket string, files map[string]string) {
	t.Helper()
	ctx := context.Background()

	cfg, _ := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""),
		),
	)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	for key, content := range files {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte(content)),
		})
		if err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
}
