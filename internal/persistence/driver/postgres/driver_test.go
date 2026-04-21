package postgres_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/dogmatiq/runkit/internal/persistence/driver/postgres"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestProvider(t *testing.T) {
	ctx := context.Background()

	ctr, err := tcpostgres.Run(
		ctx,
		"postgres:18-alpine",
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ctr.Terminate(context.Background()); err != nil {
			t.Errorf("could not terminate postgres container: %v", err)
		}
	})

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}

	p, err := postgres.NewProvider(u)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("could not close postgres provider: %v", err)
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
