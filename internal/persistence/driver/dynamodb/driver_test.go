package dynamodb_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/dogmatiq/runkit/internal/persistence/driver/dynamodb"
	tcdynamodb "github.com/testcontainers/testcontainers-go/modules/dynamodb"
)

func TestProvider(t *testing.T) {
	// DynamoDB Local does not validate credentials, but the AWS SDK requires
	// some credentials to be present. Set dummy values if not already set.
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}

	ctx := context.Background()

	ctr, err := tcdynamodb.Run(
		ctx,
		"amazon/dynamodb-local",
		tcdynamodb.WithDisableTelemetry(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ctr.Terminate(context.Background()); err != nil {
			t.Errorf("could not terminate DynamoDB container: %v", err)
		}
	})

	endpoint, err := ctr.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}

	u, err := url.Parse(fmt.Sprintf("dynamodb://%s/testbase?insecure=true&region=us-east-1", endpoint))
	if err != nil {
		t.Fatal(err)
	}

	p, err := dynamodb.NewProvider(u)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("could not close DynamoDB provider: %v", err)
		}
	})

	t.Run("KVStore", func(t *testing.T) {
		s, err := p.KVStore(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ks, err := s.Open(ctx, "test")
		if err != nil {
			t.Fatal(err)
		}
		if err := ks.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("JournalStore", func(t *testing.T) {
		s, err := p.JournalStore(ctx)
		if err != nil {
			t.Fatal(err)
		}
		j, err := s.Open(ctx, "test")
		if err != nil {
			t.Fatal(err)
		}
		if err := j.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("SetStore", func(t *testing.T) {
		s, err := p.SetStore(ctx)
		if err != nil {
			t.Fatal(err)
		}
		set, err := s.Open(ctx, "test")
		if err != nil {
			t.Fatal(err)
		}
		if err := set.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
