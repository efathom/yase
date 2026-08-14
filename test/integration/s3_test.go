package integration

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	yaseStorage "github.com/efathom/yase/pkg/storage"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
)

func startMinio(t *testing.T) (endpoint, username, password string) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	ctx := context.Background()
	container, err := tcminio.Run(ctx, "minio/minio:latest",
		tcminio.WithUsername("minioadmin"),
		tcminio.WithPassword("minioadmin"),
	)
	if err != nil {
		t.Fatalf("start minio container: %v", err)
	}
	t.Cleanup(func() { container.Terminate(ctx) })

	ep, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("minio connection string: %v", err)
	}

	return "http://" + ep, container.Username, container.Password
}

func createBucket(t *testing.T, endpoint, bucket string) {
	t.Helper()
	ctx := context.Background()

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""),
		),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Logf("CreateBucket %s: %v (may already exist)", bucket, err)
	}
}

func TestS3StoreRoundTrip(t *testing.T) {
	endpoint, _, _ := startMinio(t)
	ctx := context.Background()

	createBucket(t, endpoint, "test-roundtrip")

	store, err := yaseStorage.NewS3StoreFromConfig(ctx, yaseStorage.S3Config{
		Endpoint:       endpoint,
		Bucket:         "test-roundtrip",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKey:      "minioadmin",
		SecretKey:      "minioadmin",
	})
	if err != nil {
		t.Fatalf("NewS3StoreFromConfig: %v", err)
	}

	// Upload
	data := []byte("hello from S3 integration test")
	if err := store.Upload(ctx, "indexes/v1/data.bin", bytes.NewReader(data)); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// Download
	rc, err := store.Download(ctx, "indexes/v1/data.bin")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data) {
		t.Errorf("roundtrip: got %q, want %q", got, data)
	}
}

func TestS3StoreListAndDelete(t *testing.T) {
	endpoint, _, _ := startMinio(t)
	ctx := context.Background()

	createBucket(t, endpoint, "test-list")

	store, err := yaseStorage.NewS3StoreFromConfig(ctx, yaseStorage.S3Config{
		Endpoint:       endpoint,
		Bucket:         "test-list",
		Region:         "us-east-1",
		Prefix:         "idx",
		ForcePathStyle: true,
		AccessKey:      "minioadmin",
		SecretKey:      "minioadmin",
	})
	if err != nil {
		t.Fatalf("NewS3StoreFromConfig: %v", err)
	}

	// Upload multiple files
	store.Upload(ctx, "v1/a.bin", bytes.NewReader([]byte("a")))
	store.Upload(ctx, "v1/b.bin", bytes.NewReader([]byte("b")))
	store.Upload(ctx, "v2/c.bin", bytes.NewReader([]byte("c")))

	// List under v1 prefix
	paths, err := store.List(ctx, "v1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(paths) != 2 {
		t.Errorf("expected 2 files under v1, got %d: %v", len(paths), paths)
	}

	// Delete one
	if err := store.Delete(ctx, "v1/a.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// List again
	paths, err = store.List(ctx, "v1")
	if err != nil {
		t.Fatalf("List after delete: %v", err)
	}
	if len(paths) != 1 {
		t.Errorf("expected 1 file after delete, got %d: %v", len(paths), paths)
	}
}

func TestS3StoreLargeFile(t *testing.T) {
	endpoint, _, _ := startMinio(t)
	ctx := context.Background()

	createBucket(t, endpoint, "test-large")

	store, err := yaseStorage.NewS3StoreFromConfig(ctx, yaseStorage.S3Config{
		Endpoint:       endpoint,
		Bucket:         "test-large",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKey:      "minioadmin",
		SecretKey:      "minioadmin",
	})
	if err != nil {
		t.Fatalf("NewS3StoreFromConfig: %v", err)
	}

	// Simulate a 1MB arena snapshot
	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = byte(i % 256)
	}

	if err := store.Upload(ctx, "snapshots/arena.bin", bytes.NewReader(data)); err != nil {
		t.Fatalf("Upload large: %v", err)
	}

	rc, err := store.Download(ctx, "snapshots/arena.bin")
	if err != nil {
		t.Fatalf("Download large: %v", err)
	}
	defer rc.Close()

	got, _ := io.ReadAll(rc)
	if len(got) != len(data) {
		t.Fatalf("size mismatch: got %d, want %d", len(got), len(data))
	}
	// Spot-check every 4096th byte
	for i := 0; i < len(data); i += 4096 {
		if got[i] != data[i] {
			t.Errorf("byte %d: got %d, want %d", i, got[i], data[i])
			break
		}
	}
	t.Logf("Large file round-trip OK: %d bytes", len(got))
}

func TestS3StoreWithCachedStore(t *testing.T) {
	endpoint, _, _ := startMinio(t)
	ctx := context.Background()

	createBucket(t, endpoint, "test-cached")

	s3Store, err := yaseStorage.NewS3StoreFromConfig(ctx, yaseStorage.S3Config{
		Endpoint:       endpoint,
		Bucket:         "test-cached",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKey:      "minioadmin",
		SecretKey:      "minioadmin",
	})
	if err != nil {
		t.Fatalf("NewS3StoreFromConfig: %v", err)
	}

	cacheDir := t.TempDir()
	cached, err := yaseStorage.NewCachedStore(s3Store, cacheDir)
	if err != nil {
		t.Fatalf("NewCachedStore: %v", err)
	}

	data := []byte("cached S3 data")
	if err := cached.Upload(ctx, "test.bin", bytes.NewReader(data)); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// First download — cache miss, fetches from S3
	rc, err := cached.Download(ctx, "test.bin")
	if err != nil {
		t.Fatalf("Download (miss): %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		t.Errorf("miss: got %q, want %q", got, data)
	}

	// Second download — should hit local cache
	rc2, err := cached.Download(ctx, "test.bin")
	if err != nil {
		t.Fatalf("Download (hit): %v", err)
	}
	got2, _ := io.ReadAll(rc2)
	rc2.Close()
	if !bytes.Equal(got2, data) {
		t.Errorf("hit: got %q, want %q", got2, data)
	}
}
