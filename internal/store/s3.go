// Package store implements the S3-backed blob store used by hf-cache-d.
package store

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ErrNotFound is returned when the requested object does not exist.
// It is distinct from any backend transport or service error.
var ErrNotFound = errors.New("store: object not found")

// Store is a concrete S3 blob store (path-style addressing, custom endpoint).
// It is a struct, not an interface: there is exactly one implementation and
// consumers use it directly. Introduce an interface only when a second
// implementation or a test fake is actually needed.
type Store struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

// New builds a Store against the given S3 endpoint and bucket using static
// credentials. It performs no network I/O; connectivity problems surface on
// first use.
func New(endpoint, bucket, accessKey, secretKey string) (*Store, error) {
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("store: endpoint, bucket, accessKey and secretKey are all required")
	}
	client := s3.New(s3.Options{
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}, nil
		}),
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
		Region:       "us-east-1",
	})
	return &Store{
		client:   client,
		uploader: manager.NewUploader(client),
		bucket:   bucket,
	}, nil
}

// Get returns a streaming reader for the object at key, along with its size.
// The caller must Close the returned reader. A missing object yields
// ErrNotFound; anything else is a backend error.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if isNotFound(err) {
		return nil, 0, ErrNotFound
	}
	if err != nil {
		return nil, 0, fmt.Errorf("s3 get %q: %w", key, err)
	}
	return out.Body, aws.ToInt64(out.ContentLength), nil
}

// GetRange returns a streaming reader over the half-open byte range
// [start, end) of the object at key. A missing object yields ErrNotFound.
func (s *Store) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	if start < 0 || end <= start {
		return nil, fmt.Errorf("store: invalid range [%d,%d)", start, end)
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end-1)),
	})
	if isNotFound(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("s3 get range %q: %w", key, err)
	}
	return out.Body, nil
}

// Put streams the contents of r into the object at key. When size is known
// (>= 0) a single PutObject is issued; the body is passed straight through
// as a reader. When size is unknown the multipart uploader streams from r.
// Neither path buffers the whole body in memory.
func (s *Store) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   r,
	}
	var err error
	if size >= 0 {
		input.ContentLength = aws.Int64(size)
		_, err = s.client.PutObject(ctx, input)
	} else {
		_, err = s.uploader.Upload(ctx, input)
	}
	if err != nil {
		return fmt.Errorf("s3 put %q: %w", key, err)
	}
	return nil
}

// Head reports whether the object exists and its size. A missing object is
// (false, 0, nil), not an error.
func (s *Store) Head(ctx context.Context, key string) (bool, int64, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if isNotFound(err) {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, fmt.Errorf("s3 head %q: %w", key, err)
	}
	return true, aws.ToInt64(out.ContentLength), nil
}

// isNotFound maps missing-object responses onto the ErrNotFound sentinel.
// GetObject surfaces a typed NoSuchKey (or NoSuchBucket); HeadObject has no
// typed error for 404, so the HTTP status is inspected directly.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *s3types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var respErr interface{ HTTPStatusCode() int }
	if errors.As(err, &respErr) {
		return respErr.HTTPStatusCode() == 404
	}
	return false
}
