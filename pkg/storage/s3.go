package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Config configures the S3-backed index store.
type S3Config struct {
	Endpoint       string // custom endpoint for MinIO/LocalStack (e.g., "http://localhost:9000")
	Bucket         string
	Region         string
	Prefix         string // key prefix for all objects (e.g., "indexes/")
	ForcePathStyle bool   // required for MinIO
	AccessKey      string // optional static credentials (for MinIO/testing)
	SecretKey      string // optional static credentials (for MinIO/testing)
}

// NewS3StoreFromConfig creates an S3-backed IndexStore.
func NewS3StoreFromConfig(ctx context.Context, cfg S3Config) (*S3StoreReal, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	s3Opts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = cfg.ForcePathStyle
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	return &S3StoreReal{
		client: client,
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
	}, nil
}

// S3StoreReal implements IndexStore backed by AWS S3 (or S3-compatible services).
type S3StoreReal struct {
	client *s3.Client
	bucket string
	prefix string
}

func (s *S3StoreReal) key(path string) string {
	if s.prefix != "" {
		return s.prefix + "/" + path
	}
	return path
}

func (s *S3StoreReal) Upload(ctx context.Context, path string, reader io.Reader) error {
	// Read all into memory for PutObject (S3 needs content length or chunked upload)
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(path)),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("PutObject: %w", err)
	}
	return nil
}

func (s *S3StoreReal) Download(ctx context.Context, path string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(path)),
	})
	if err != nil {
		return nil, fmt.Errorf("GetObject: %w", err)
	}
	return out.Body, nil
}

func (s *S3StoreReal) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := s.key(prefix)
	var paths []string

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("ListObjectsV2: %w", err)
		}
		for _, obj := range page.Contents {
			// Strip the store prefix to return relative paths
			key := aws.ToString(obj.Key)
			if len(key) > len(s.prefix)+1 {
				paths = append(paths, key[len(s.prefix)+1:])
			} else {
				paths = append(paths, key)
			}
		}
	}
	return paths, nil
}

func (s *S3StoreReal) Delete(ctx context.Context, path string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(path)),
	})
	if err != nil {
		return fmt.Errorf("DeleteObject: %w", err)
	}
	return nil
}
