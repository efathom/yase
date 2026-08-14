// Package s3store implements a YASE connector for S3-compatible object storage.
// Supports AWS S3, MinIO, and any S3-compatible endpoint.
// Streams: files (filtered by prefix and extension). Incremental via LastModified.
package s3store

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/efathom/yase/pkg/connector"
)

const connectorType = "s3"

func init() {
	connector.DefaultRegistry.Register(connectorType, NewConnector)
}

// Config holds S3-specific configuration.
type Config struct {
	Endpoint       string   `json:"endpoint"` // custom endpoint for MinIO
	Bucket         string   `json:"bucket"`
	Prefix         string   `json:"prefix"`           // key prefix filter
	Region         string   `json:"region"`           // default "us-east-1"
	ForcePathStyle bool     `json:"force_path_style"` // required for MinIO
	AccessKey      string   `json:"access_key"`
	SecretKey      string   `json:"secret_key"`
	FileExtensions []string `json:"file_extensions"` // e.g., [".md", ".pdf", ".html"]
	MaxObjectSize  int64    `json:"max_object_size"` // max bytes per object (0 = unlimited)
}

// Connector implements the YASE Connector interface for S3.
type Connector struct {
	id     string
	config Config
	client *s3.Client
}

// NewConnector creates an S3 connector from configuration.
func NewConnector(cfg connector.ConnectorConfig) (connector.Connector, error) {
	var config Config
	configBytes, _ := json.Marshal(cfg.Config)
	if err := json.Unmarshal(configBytes, &config); err != nil {
		return nil, fmt.Errorf("parse s3 config: %w", err)
	}

	if config.Bucket == "" {
		return nil, fmt.Errorf("s3: bucket is required")
	}
	if config.Region == "" {
		config.Region = "us-east-1"
	}

	// Also check auth config for credentials
	if cfg.Auth != nil && cfg.Auth.Method == connector.AuthAPIKey {
		if config.AccessKey == "" {
			config.AccessKey = cfg.Auth.APIKey
		}
		if config.SecretKey == "" {
			config.SecretKey = cfg.Auth.Password
		}
	}

	ctx := context.Background()
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(config.Region),
	}
	if config.AccessKey != "" && config.SecretKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(config.AccessKey, config.SecretKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}

	s3Opts := []func(*s3.Options){}
	if config.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(config.Endpoint)
			o.UsePathStyle = config.ForcePathStyle
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	return &Connector{
		id:     cfg.Type,
		config: config,
		client: client,
	}, nil
}

func (c *Connector) ID() string          { return c.id }
func (c *Connector) DisplayName() string { return "S3" }

func (c *Connector) Spec() *connector.ConnectorSpec {
	return &connector.ConnectorSpec{
		AuthMethods: []connector.AuthMethod{connector.AuthAPIKey, connector.AuthSession},
		SyncModes:   []connector.SyncMode{connector.FullRefresh, connector.Incremental},
	}
}

func (c *Connector) Validate(ctx context.Context) error {
	_, err := c.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(c.config.Bucket),
	})
	return err
}

func (c *Connector) Discover(ctx context.Context) (*connector.Catalog, error) {
	return &connector.Catalog{
		Streams: []connector.Stream{
			{Name: "files", SupportedSyncModes: []connector.SyncMode{connector.FullRefresh, connector.Incremental}, DefaultCursorField: "LastModified"},
		},
	}, nil
}

func (c *Connector) Read(ctx context.Context, streams []connector.ConfiguredStream, state *connector.SyncState) (<-chan connector.Record, <-chan error) {
	records := make(chan connector.Record, 100)
	errs := make(chan error, 10)

	go func() {
		defer close(records)
		defer close(errs)

		for _, stream := range streams {
			if stream.Name == "files" {
				c.readFiles(ctx, stream, state, records, errs)
			} else {
				errs <- fmt.Errorf("unknown stream: %s", stream.Name)
			}
		}
	}()

	return records, errs
}

func (c *Connector) Close() error { return nil }

func (c *Connector) readFiles(ctx context.Context, stream connector.ConfiguredStream, state *connector.SyncState, records chan<- connector.Record, errs chan<- error) {
	// Get last modified timestamp for incremental
	var lastModified time.Time
	if stream.SyncMode == connector.Incremental {
		if raw := state.GetStreamState(stream.Name); raw != nil {
			var cursorState struct {
				LastModified string `json:"last_modified"`
			}
			json.Unmarshal(raw, &cursorState)
			if cursorState.LastModified != "" {
				lastModified, _ = time.Parse(time.RFC3339, cursorState.LastModified)
			}
		}
	}

	var latestModified time.Time

	// List objects with prefix
	paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.config.Bucket),
		Prefix: aws.String(c.config.Prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			errs <- fmt.Errorf("list objects: %w", err)
			return
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)

			// Filter by extension
			if !c.matchesExtension(key) {
				continue
			}

			// Skip older files for incremental sync
			if !lastModified.IsZero() && obj.LastModified != nil && !obj.LastModified.After(lastModified) {
				continue
			}

			// Download object
			content, err := c.downloadObject(ctx, key)
			if err != nil {
				errs <- fmt.Errorf("download %s: %w", key, err)
				continue
			}

			mimeType := detectMimeType(key)
			objURL := fmt.Sprintf("s3://%s/%s", c.config.Bucket, key)
			if c.config.Endpoint != "" {
				objURL = c.config.Endpoint + "/" + c.config.Bucket + "/" + key
			}

			// Guard against S3-compatible stores returning a nil LastModified.
			lastMod := ""
			if obj.LastModified != nil {
				lastMod = obj.LastModified.Format(time.RFC3339)
			}

			connector.SendRecord(ctx, records, connector.Record{
				StreamName: stream.Name,
				ID:         key,
				Content:    content,
				MimeType:   mimeType,
				URL:        objURL,
				Metadata: map[string]string{
					"bucket":        c.config.Bucket,
					"key":           key,
					"size":          fmt.Sprintf("%d", aws.ToInt64(obj.Size)),
					"last_modified": lastMod,
					"source":        "s3",
				},
				Action:    connector.Upsert,
				EmittedAt: time.Now(),
			})

			if obj.LastModified != nil && obj.LastModified.After(latestModified) {
				latestModified = *obj.LastModified
			}
		}
	}

	// Save cursor
	if !latestModified.IsZero() {
		cursorJSON, _ := json.Marshal(map[string]string{"last_modified": latestModified.Format(time.RFC3339)})
		state.SetStreamState(stream.Name, cursorJSON)
	}
}

func (c *Connector) downloadObject(ctx context.Context, key string) ([]byte, error) {
	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return connector.ReadLimited(out.Body, c.config.MaxObjectSize)
}

func (c *Connector) matchesExtension(key string) bool {
	if len(c.config.FileExtensions) == 0 {
		return true
	}
	ext := strings.ToLower(path.Ext(key))
	for _, allowed := range c.config.FileExtensions {
		if strings.ToLower(allowed) == ext {
			return true
		}
	}
	return false
}

func detectMimeType(key string) string {
	ext := strings.ToLower(path.Ext(key))
	switch ext {
	case ".html", ".htm":
		return "text/html"
	case ".md", ".markdown":
		return "text/markdown"
	case ".pdf":
		return "application/pdf"
	case ".txt":
		return "text/plain"
	case ".json":
		return "application/json"
	case ".docx":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case ".csv":
		return "text/csv"
	default:
		return "application/octet-stream"
	}
}
