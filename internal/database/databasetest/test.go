package databasetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib" // register the "pgx" driver with database/sql
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// New starts a fresh PostgreSQL container, opens a [*sql.DB] against it, and
// returns both the DB and the DSN.
//
// The container is terminated and the DB closed when the test ends.
func New(t testing.TB, schema ...string) (*sql.DB, string) {
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

	if err := database.ApplySchema(t.Context(), db); err != nil {
		t.Fatalf("unable to apply schema: %s", err)
	}

	for _, s := range schema {
		if _, err := db.ExecContext(t.Context(), s); err != nil {
			t.Fatalf("unable to apply custom schema: %s", err)
		}
	}

	return db, dsn
}

// Transact executes the given function within a transaction.
func Transact(
	t testing.TB,
	db *sql.DB,
	fn func(*sql.Tx),
) {
	t.Helper()

	if err := database.Transact(
		t.Context(),
		db,
		func(_ context.Context, tx *sql.Tx) error {
			t.Helper()

			fn(tx)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
}

// WaitUntil repeatedly executes a database query until it produces a row with a
// truthy value or [xtesting.WaitTimeout] elapses.
func WaitUntil(
	t testing.TB,
	q database.Querier,
	description, query string,
	args ...any,
) {
	t.Helper()

	xtesting.WaitUntil(
		t,
		description,
		func() bool {
			t.Helper()

			row := q.QueryRowContext(
				t.Context(),
				query,
				args...,
			)

			var result bool
			if err := row.Scan(&result); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return false
				}
				t.Fatal(err)
			}

			return result
		},
	)
}
