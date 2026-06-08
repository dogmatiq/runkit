package xtesting

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // register the "pgx" driver with database/sql

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// NewDatabase is a variant of [newDatabase] that applies the engine's schema to the
// database before returning it.
func NewDatabase(t testing.TB) *sql.DB {
	db := newDatabase(t)

	if err := database.CreateSchema(t.Context(), db); err != nil {
		t.Fatalf("unable to apply database schema: %s", err)
	}

	return db
}

// newDatabase returns a database pool that connects to an isolated test
// database.
//
// The database is removed when the test ends.
func newDatabase(t testing.TB) *sql.DB {
	t.Helper()

	container := getContainer(t)

	mainDSN, err := container.ConnectionString(t.Context())
	if err != nil {
		t.Fatalf("unable to read PostgreSQL DSN: %s", err)
	}

	dbName := createTestDatabase(t, mainDSN)
	testDSN := replaceDatabaseNameInDSN(t, mainDSN, dbName)

	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatalf("unable to open PostgreSQL connection pool: %s", err)
	}
	t.Cleanup(func() {
		db.Close()
	})

	return db
}

// replaceDatabaseNameInDSN replaces the database name in the given PostgreSQL
// connection string with the given name and returns the modified connection
// string.
func replaceDatabaseNameInDSN(
	t testing.TB,
	dsn string,
	dbName string,
) string {
	t.Helper()

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("unable to parse PostgreSQL connection string: %s", err)
	}
	u.Path = "/" + dbName

	return u.String()
}

// createTestDatabase creates a new database with a random name on the server
// specified by the given DSN. It returns the name.
func createTestDatabase(t testing.TB, dsn string) (dbName string) {
	t.Helper()

	dbName = "dogma-" + uuidpb.Generate().AsString()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("unable to open PostgreSQL connection pool: %s", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(
		t.Context(),
		`CREATE DATABASE "`+dbName+`"`,
	); err != nil {
		t.Fatalf("unable to create PostgreSQL database: %s", err)
	}

	return dbName
}

var containerState struct {
	mutex     sync.Mutex
	container *postgres.PostgresContainer
}

// getContainer returns a singleton PostgreSQL container for the test suite. It
// starts the container if it hasn't been started yet.
//
// By caching the container ourselves, we avoid the overhead of calling out to
// docker at the start of every test.
func getContainer(t testing.TB) *postgres.PostgresContainer {
	t.Helper()

	containerState.mutex.Lock()
	defer containerState.mutex.Unlock()

	if containerState.container != nil {
		return containerState.container
	}

	// We don't use "dogma" as the username because the default PostgreSQL
	// "search_path" would automatically find the "dogma" schema, but we want to
	// make sure that all queries name the schema explicitly.
	username := "dogmatiq-reference-engine-test"
	password := "password"

	container, err := postgres.Run(
		t.Context(),
		"postgres:18-alpine",

		// Allow container reuse, but key it based on the same session ID that
		// testcontainers does for starting the Ryuk reaper process; otherwise
		// the container will be shutdown by the first reaper process that
		// starts.
		testcontainers.WithReuseByName(
			fmt.Sprintf(
				"dogma-reference-engine-%s",
				testcontainers.SessionID(),
			),
		),

		postgres.BasicWaitStrategies(),
		postgres.WithUsername(username),
		postgres.WithPassword(password),
	)
	if err != nil {
		t.Fatalf("unable to start PostgreSQL container: %s", err)
	}

	containerState.container = container

	return container
}

// DatabaseExecutor is the common interface for running queries and executing
// statements, it is implemented by both [sql.DB] and [sql.Tx].
type DatabaseExecutor interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// ExpectQueryResult executes a database query and fails the test if it does not
// produce a row with the wanted value in the first column.
func ExpectQueryResult[T comparable](
	t testing.TB,
	description string,
	want T,
	x DatabaseExecutor,
	query string,
	args ...any,
) {
	t.Helper()

	row := x.QueryRowContext(
		t.Context(),
		query,
		args...,
	)

	var got T
	if err := row.Scan(&got); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expectation failed: %s: got no rows, want %v", description, want)
		}

		t.Fatalf("expectation failed: %s: unable to scan row: %s", description, err)
	}

	if got != want {
		t.Fatalf("expectation failed: %s: got %v, want %v", description, got, want)
	}
}

// ExpectQueryResultEventually executes a database query and fails the test if
// it does not produce a row with the wanted value in the first column within
// [WaitTimeout].
func ExpectQueryResultEventually[T comparable](
	t testing.TB,
	description string,
	want T,
	x DatabaseExecutor,
	query string,
	args ...any,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), WaitTimeout)
	defer cancel()

	for {
		row := x.QueryRowContext(
			ctx,
			query,
			args...,
		)

		var got T
		if err := row.Scan(&got); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				t.Logf("expectation failed: %s: got no rows, want %v", description, want)
			}

			t.Logf("expectation failed: %s: unable to scan row: %s", description, err)
		}

		if got == want {
			return
		}

		t.Logf("expectation failed: %s: got %v, want %v", description, got, want)

		select {
		case <-ctx.Done():
			t.Fatalf("expectation failed: %s: timed out waiting for value to become %v", description, want)
		case <-time.After(50 * time.Millisecond):
			continue
		}
	}
}
