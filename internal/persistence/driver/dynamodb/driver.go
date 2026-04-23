package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/dogmatiq/persistencekit/driver/aws/dynamojournal"
	"github.com/dogmatiq/persistencekit/driver/aws/dynamokv"
	"github.com/dogmatiq/persistencekit/driver/aws/dynamoset"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/set"
	"github.com/dogmatiq/runkit/internal/persistence/driver/internal/xaws"
)

// NewProvider returns a [Provider] configured from a dynamodb:// URL. It
// returns an error if u.Scheme is not "dynamodb".
//
// URL format:
//
//	dynamodb:///<table-prefix>
//	dynamodb://<host>:<port>/<table-prefix>
//
// Supported query parameters:
//
//   - region: the AWS region (e.g. "us-east-1"). If omitted, the region is
//     resolved from the environment using the standard AWS SDK chain
//     (AWS_REGION, AWS_DEFAULT_REGION, EC2 instance metadata, etc.).
//
//   - role_arn: the ARN of an IAM role to assume before accessing DynamoDB.
//     The role is assumed using the resolved base credentials, which must have
//     sts:AssumeRole permission on the target role. If omitted, the base
//     credentials are used directly.
//
//   - insecure: when present, use plain HTTP instead of HTTPS when connecting
//     to a custom endpoint. It is an error to use this parameter without a host
//     in the URL. Intended for use with local emulators such as DynamoDB Local.
func NewProvider(u *url.URL) (*Provider, error) {
	if u.Scheme != "dynamodb" {
		return nil, fmt.Errorf("invalid dynamodb URL: unexpected URL scheme %q", u.Scheme)
	}

	tablePrefix := strings.TrimPrefix(u.Path, "/")
	if tablePrefix == "" {
		return nil, errors.New("invalid dynamodb URL: table prefix is required in the path (e.g. dynamodb:///<table-prefix>)")
	}

	loadConfig, err := xaws.ParseConfig(u)
	if err != nil {
		return nil, err
	}

	return &Provider{
		tablePrefix: tablePrefix,
		loadConfig:  loadConfig,
	}, nil
}

// Provider is a persistence.Provider backed by Amazon DynamoDB.
type Provider struct {
	tablePrefix string
	loadConfig  xaws.ConfigLoader

	m      sync.Mutex
	client *dynamodb.Client
}

// KVStore returns a key-value store backed by DynamoDB.
func (p *Provider) KVStore(ctx context.Context) (kv.BinaryStore, error) {
	client, err := p.open(ctx)
	if err != nil {
		return nil, err
	}
	return dynamokv.NewBinaryStore(client, p.tablePrefix+"-kv"), nil
}

// JournalStore returns a journal store backed by DynamoDB.
func (p *Provider) JournalStore(ctx context.Context) (journal.BinaryStore, error) {
	client, err := p.open(ctx)
	if err != nil {
		return nil, err
	}
	return dynamojournal.NewBinaryStore(client, p.tablePrefix+"-journal"), nil
}

// SetStore returns a set store backed by DynamoDB.
func (p *Provider) SetStore(ctx context.Context) (set.BinaryStore, error) {
	client, err := p.open(ctx)
	if err != nil {
		return nil, err
	}
	return dynamoset.NewBinaryStore(client, p.tablePrefix+"-set"), nil
}

// Close is a no-op.
func (*Provider) Close() error {
	return nil
}

func (p *Provider) open(ctx context.Context) (*dynamodb.Client, error) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.client != nil {
		return p.client, nil
	}

	cfg, err := p.loadConfig(ctx)
	if err != nil {
		return nil, err
	}

	p.client = dynamodb.NewFromConfig(cfg)
	return p.client, nil
}
