package s3

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dogmatiq/persistencekit/driver/aws/s3journal"
	"github.com/dogmatiq/persistencekit/driver/aws/s3kv"
	"github.com/dogmatiq/persistencekit/driver/aws/s3set"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
	"github.com/dogmatiq/runkit/internal/persistence/driver/internal/xaws"
)

// NewProvider returns a [Provider] configured from an s3:// URL. It returns
// an error if u.Scheme is not "s3".
//
// URL format:
//
//	s3:///<bucket>
//	s3://<endpoint>/<bucket>
//
// Supported query parameters:
//
//   - region: the AWS region (e.g. "us-east-1"). If omitted, the region is
//     resolved from the environment using the standard AWS SDK chain
//     (AWS_REGION, AWS_DEFAULT_REGION, EC2 instance metadata, etc.).
//
//   - role_arn: the ARN of an IAM role to assume before accessing S3.
//     The role is assumed using the resolved base credentials, which must have
//     sts:AssumeRole permission on the target role. If omitted, the base
//     credentials are used directly.
//
//   - insecure: when present, use plain HTTP instead of HTTPS when connecting
//     to a custom endpoint. It is an error to use this parameter without a host
//     in the URL. Intended for use with local emulators such as MinIO.
func NewProvider(u *url.URL) (*Provider, error) {
	if u.Scheme != "s3" {
		return nil, fmt.Errorf("invalid s3 URL: unexpected URL scheme %q", u.Scheme)
	}

	bucket := strings.TrimPrefix(u.Path, "/")
	if bucket == "" {
		return nil, errors.New("invalid s3 URL: bucket name is required in the path (e.g. s3:///<bucket>)")
	}

	loadConfig, err := xaws.ParseConfig(u)
	if err != nil {
		return nil, err
	}

	return &Provider{
		bucket:     bucket,
		loadConfig: loadConfig,
	}, nil
}

// Provider is a persistence.Provider backed by Amazon S3.
type Provider struct {
	bucket     string
	loadConfig xaws.ConfigLoader

	m      sync.Mutex
	client *s3.Client
}

// KVStore returns a key-value store backed by Amazon S3.
func (p *Provider) KVStore(ctx context.Context) (kv.BinaryStore, error) {
	client, err := p.open(ctx)
	if err != nil {
		return nil, err
	}
	return s3kv.NewBinaryStore(client, p.bucket), nil
}

// JournalStore returns a journal store backed by Amazon S3.
func (p *Provider) JournalStore(ctx context.Context) (journal.BinaryStore, error) {
	client, err := p.open(ctx)
	if err != nil {
		return nil, err
	}
	return s3journal.NewBinaryStore(client, p.bucket), nil
}

// SetStore returns a set store backed by Amazon S3.
func (p *Provider) SetStore(ctx context.Context) (set.BinaryStore, error) {
	client, err := p.open(ctx)
	if err != nil {
		return nil, err
	}
	return s3set.NewBinaryStore(client, p.bucket), nil
}

// Close is a no-op.
func (*Provider) Close() error {
	return nil
}

func (p *Provider) open(ctx context.Context) (*s3.Client, error) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.client != nil {
		return p.client, nil
	}

	cfg, err := p.loadConfig(ctx)
	if err != nil {
		return nil, err
	}

	p.client = s3.NewFromConfig(cfg)
	return p.client, nil
}
