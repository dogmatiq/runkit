package database

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib" // register the "pgx" driver with database/sql
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// NewTestDB starts a fresh PostgreSQL container, opens a [*sql.DB] against it,
// and returns both the DB and the DSN.
//
// The container is terminated and the DB closed when the test ends.
func NewTestDB(t testing.TB, schema ...string) (*sql.DB, string) {
	t.Helper()

	username := "dogma"
	password := uuid.NewString()

	container, err := tcpostgres.Run(
		t.Context(),
		"postgres:18-alpine",
		tcpostgres.BasicWaitStrategies(),
		tcpostgres.WithUsername(username),
		tcpostgres.WithPassword(password),
	)
	if err != nil {
		t.Fatalf("unable to start postgres container: %s", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := container.Terminate(ctx); err != nil {
			t.Errorf("unable to terminate postgres container: %s", err)
		}
	})

	dsn, err := container.ConnectionString(t.Context())
	if err != nil {
		t.Fatalf("unable to read postgres connection string: %s", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("unable to open DB: %s", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("unable to close DB: %s", err)
		}
	})

	if err := ApplySchema(t.Context(), db); err != nil {
		t.Fatalf("unable to apply schema: %s", err)
	}

	for _, schema := range schema {
		if _, err := db.ExecContext(t.Context(), schema); err != nil {
			t.Fatalf("unable to apply custom schema: %s", err)
		}
	}

	return db, dsn
}
