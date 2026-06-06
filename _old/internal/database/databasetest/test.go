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

// ExecOne executes the given query and asserts that it affected exactly one row.
func ExecOne(
	t testing.TB,
	x database.Executor,
	query string,
	args ...any,
) {
	t.Helper()

	res, err := x.ExecContext(t.Context(), query, args...)
	if err != nil {
		t.Fatal(err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}

	if n != 1 {
		t.Fatalf("unexpected number of rows affected: got %d, want 1", n)
	}
}

// Expect executes a database query and fails the test if it does not produce a
// row with a truth value.
func Expect(
	t testing.TB,
	q database.Executor,
	description, query string,
	args ...any,
) {
	t.Helper()

	row := q.QueryRowContext(
		t.Context(),
		query,
		args...,
	)

	var result bool
	if err := row.Scan(&result); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("%s: no rows returned", description)
		} else {
			t.Fatal(err)
		}
	}

	if !result {
		t.Fatalf("%s: unexpected result: got false, want true", description)
	}
}

// WaitUntil repeatedly executes a database query until it produces a row with a
// truthy value or [xtesting.WaitTimeout] elapses.
func WaitUntil(
	t testing.TB,
	q database.Executor,
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
