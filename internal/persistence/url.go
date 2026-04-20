package persistence

import (
	"fmt"
	"net/url"

	"github.com/dogmatiq/runkit/internal/persistence/driver/dynamodb"
	"github.com/dogmatiq/runkit/internal/persistence/driver/memory"
	"github.com/dogmatiq/runkit/internal/persistence/driver/postgres"
	"github.com/dogmatiq/runkit/internal/persistence/driver/s3"
)

// ProviderFromURL returns a [Provider] configured from the given URL string.
//
// The URL scheme identifies the persistence driver. An error is returned if the
// URL is malformed, the scheme is unrecognized, or driver-specific validation
// fails.
//
// # PostgreSQL
//
// The "postgres" and "postgresql" drivers store data in PostgreSQL. The URL is
// passed through verbatim to [pgx](https://github.com/jackc/pgx).
//
//	postgres://<user>:<pass>@<host>/<dbname>
//	postgresql://<user>:<pass>@<host>/<dbname>
//
// # DynamoDB
//
// The "dynamodb" driver stores data in Amazon DynamoDB. The path component
// provides the base name used to derive table names. Supported query
// parameters: region, role_arn, insecure.
//
//	dynamodb:///<base>
//	dynamodb://<host>:<port>/<base>
//
// # S3
//
// The "s3" driver stores data in Amazon S3. The path component is the bucket
// name. Supported query parameters: region, role_arn, insecure.
//
//	s3:///<bucket>
//	s3://<endpoint>/<bucket>
//
// # Memory
//
// The "memory" driver stores data in-process and is intended for testing.
// Providers that share the same silo name operate on the same underlying
// stores. The host and query components must be empty.
//
//	memory:///<silo>
func ProviderFromURL(rawURL string) (Provider, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid persistence URL: %w", err)
	}

	switch u.Scheme {
	case "memory":
		return memory.NewProvider(u)
	case "postgres", "postgresql":
		return postgres.NewProvider(u)
	case "dynamodb":
		return dynamodb.NewProvider(u)
	case "s3":
		return s3.NewProvider(u)
	default:
		return nil, fmt.Errorf("unknown persistence driver: %q", u.Scheme)
	}
}
