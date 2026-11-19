package xtesting

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/schema"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// NewDatabase returns a database pool that connects to an isolated test
// database that has the engine's schema applied.
//
// The pool connects as a role that has only the privileges that the README's
// "Restricting the engine's privileges" section lists, so it can't change the
// schema. Use [NewDatabaseAsOwner] in a test that changes the schema.
func NewDatabase(t testing.TB) *sql.DB {
	t.Helper()

	config := createDatabase(t)
	owner := openPool(t, config, nil)
	ApplySchema(t, owner)

	return openPool(t, asEngineRole(t, config, owner), nil)
}

// NewDatabaseAsOwner returns a database pool that connects as the role that
// owns the engine's schema, to an isolated test database that has the schema
// applied.
func NewDatabaseAsOwner(t testing.TB) *sql.DB {
	t.Helper()

	db := openPool(t, createDatabase(t), nil)
	ApplySchema(t, db)

	return db
}

// ApplySchema brings the schema of the given database to the current schema
// version, as the engine does, and fails the test if it can't.
func ApplySchema(t testing.TB, db *sql.DB) {
	t.Helper()

	if err := schema.Apply(
		t.Context(),
		db,
		false,
		5*time.Second,
		30*time.Second,
	); err != nil {
		t.Fatalf("unable to apply database schema: %s", err)
	}
}

// DumpSchema returns the output of "pg_dump --schema-only" for the given
// database, and fails the test if it can't run it.
//
// The output contains lines with a randomly chosen key, so the output for two
// identical schemas is not necessarily identical.
func DumpSchema(t testing.TB, db *sql.DB) string {
	t.Helper()

	var name string
	if err := db.QueryRowContext(
		t.Context(),
		`SELECT current_database()`,
	).Scan(&name); err != nil {
		t.Fatalf("unable to read the database name: %s", err)
	}

	code, output, err := getContainer(t).Exec(
		t.Context(),
		[]string{
			"pg_dump",
			"--schema-only",
			"--username", containerUsername,
			"--dbname", name,
		},
		exec.Multiplexed(),
	)
	if err != nil {
		t.Fatalf("unable to run pg_dump: %s", err)
	}

	dump, err := io.ReadAll(output)
	if err != nil {
		t.Fatalf("unable to read the output of pg_dump: %s", err)
	}

	if code != 0 {
		t.Fatalf("pg_dump exited with code %d: %s", code, dump)
	}

	return string(dump)
}

// NewDatabaseWithFailingQueries returns a database pool like [NewDatabase],
// except that each query for which the given function returns true fails
// without being sent to the database.
//
// The function is called with the SQL and arguments of every query that the
// pool sends. The schema is applied by a different pool, so the function isn't
// called for those queries.
func NewDatabaseWithFailingQueries(
	t testing.TB,
	fail func(query string, args []any) bool,
) *sql.DB {
	t.Helper()

	config := createDatabase(t)
	owner := openPool(t, config, nil)
	ApplySchema(t, owner)

	return openPool(
		t,
		asEngineRole(t, config, owner),
		&queryFailer{
			fail: fail,
		},
	)
}

// NewDatabaseWithoutSchema returns a database pool that connects to an isolated
// test database that does not have the engine's schema applied.
//
// The pool connects as the database's owner, so it can create the schema.
func NewDatabaseWithoutSchema(t testing.TB) *sql.DB {
	t.Helper()
	return openPool(t, createDatabase(t), nil)
}

// createDatabase creates a new isolated test database, and returns the
// configuration for connecting to it as its owner.
func createDatabase(t testing.TB) *pgx.ConnConfig {
	t.Helper()

	container := getContainer(t)

	mainDSN, err := container.ConnectionString(t.Context())
	if err != nil {
		t.Fatalf("unable to read PostgreSQL DSN: %s", err)
	}

	dbName := createTestDatabase(t, mainDSN)
	testDSN := replaceDatabaseNameInDSN(t, mainDSN, dbName)

	config, err := pgx.ParseConfig(testDSN)
	if err != nil {
		t.Fatalf("unable to parse PostgreSQL connection string: %s", err)
	}

	return config
}

// asEngineRole creates a role that has only the privileges that the README's
// "Restricting the engine's privileges" section lists, and returns a copy of
// config that connects as that role. It uses owner, a pool that connects as the
// database's owner, to create the role and to drop it.
//
// Roles belong to the whole PostgreSQL server, not to one database, so each
// test gets a role with a name of its own. The role is dropped when the test
// ends, after the pools that connect as it are closed and before owner is
// closed.
func asEngineRole(
	t testing.TB,
	config *pgx.ConnConfig,
	owner *sql.DB,
) *pgx.ConnConfig {
	t.Helper()

	name := "engine-" + uuidpb.Generate().AsString()
	role := pgx.Identifier{name}.Sanitize()

	if _, err := owner.ExecContext(
		t.Context(),
		`CREATE ROLE `+role+` LOGIN PASSWORD '`+containerPassword+`';
		GRANT USAGE ON SCHEMA runkit TO `+role+`;
		GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA runkit TO `+role+`;
		GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA runkit TO `+role,
	); err != nil {
		t.Fatalf("unable to create the engine's role: %s", err)
	}

	// Cleanup functions run in the reverse of the order in which they were
	// registered. owner registered its cleanup before this one, and the pools
	// that connect as the role register theirs after it.
	t.Cleanup(func() {
		if _, err := owner.ExecContext(
			context.Background(),
			`DROP OWNED BY `+role+`;
			DROP ROLE `+role,
		); err != nil {
			t.Errorf("unable to drop the engine's role: %s", err)
		}
	})

	config = config.Copy()
	config.User = name
	config.Password = containerPassword

	return config
}

// openPool returns a database pool that connects with the given configuration,
// and reports each query to the given tracer, if it is not nil.
//
// The pool is closed when the test ends.
func openPool(
	t testing.TB,
	config *pgx.ConnConfig,
	tracer pgx.QueryTracer,
) *sql.DB {
	t.Helper()

	config = config.Copy()
	config.Tracer = tracer

	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(100)

	t.Cleanup(func() {
		db.Close()
	})

	return db
}

// queryFailer is a [pgx.QueryTracer] that fails each query for which its fail
// function returns true.
type queryFailer struct {
	fail func(query string, args []any) bool
}

func (f *queryFailer) TraceQueryStart(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceQueryStartData,
) context.Context {
	if !f.fail(data.SQL, data.Args) {
		return ctx
	}

	// pgx runs the query with the returned context, so a canceled context
	// fails it before it is sent.
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	return ctx
}

func (f *queryFailer) TraceQueryEnd(
	ctx context.Context,
	conn *pgx.Conn,
	data pgx.TraceQueryEndData,
) {
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

const (
	// containerUsername is the name of the PostgreSQL user that the tests use
	// to connect.
	//
	// It must not be "runkit", the name of the engine's schema. The default
	// PostgreSQL "search_path" includes a schema named after the connected
	// user, which would let a query that omits the schema name succeed. The
	// tests want every query to name its schema.
	containerUsername = "dogmatiq-runkit-test"

	// containerPassword is the password of the PostgreSQL user that the tests
	// connect as.
	containerPassword = "password"
)

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

	container, err := postgres.Run(
		t.Context(),
		"postgres:18-alpine",

		// Allow container reuse, but key it based on the same session ID that
		// testcontainers does for starting the Ryuk reaper process; otherwise
		// the container will be shutdown by the first reaper process that
		// starts.
		testcontainers.WithReuseByName(
			fmt.Sprintf(
				"dogmatiq-runkit-%s",
				testcontainers.SessionID(),
			),
		),

		postgres.BasicWaitStrategies(),
		postgres.WithUsername(containerUsername),
		postgres.WithPassword(containerPassword),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			Cmd: []string{"-c", "max_connections=1000"},
		}),
	)
	if err != nil {
		t.Fatalf("unable to start PostgreSQL container: %s", err)
	}

	containerState.container = container

	return container
}

// Transact executes the given function within a database transaction, and fails
// the test if it returns an error.
func Transact(t testing.TB, db *sql.DB, fn func(*sql.Tx)) {
	t.Helper()

	if err := xsql.Transact(
		t.Context(),
		db,
		func(_ context.Context, tx *sql.Tx) error {
			fn(tx)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
}

// ExecOne executes the given query and fails the test if it returns an error or
// does not affect exactly one row.
func ExecOne(
	t testing.TB,
	x xsql.Executor,
	query string,
	args ...any,
) {
	t.Helper()

	if err := xsql.ExecOne(
		t.Context(),
		x,
		query,
		args...,
	); err != nil {
		t.Fatal(err)
	}
}

// ExpectQueryResult executes a database query and fails the test if it does not
// produce a row with the wanted value in the first column.
func ExpectQueryResult[T comparable](
	t testing.TB,
	description string,
	want T,
	q xsql.Querier,
	query string,
	args ...any,
) {
	t.Helper()

	row := q.QueryRowContext(
		t.Context(),
		query,
		args...,
	)

	var got T
	if err := row.Scan(&got); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expectation failed: %s: unable to scan row: %s", description, err)
		}

		t.Fatalf("expectation failed: %s: got no rows, want %v", description, want)
	}

	if got != want {
		t.Fatalf("expectation failed: %s: got %v, want %v", description, got, want)
	}
}

// WaitForQueryResult executes a database query repeatedly until it produces a
// row with the wanted value in the first column. If this does not occur within
// [WaitTimeout], the test fails.
//
// An error from the query does not fail the test until the timeout, as it may
// be transient. If the timeout is reached, the most recent error is reported.
func WaitForQueryResult[T comparable](
	t testing.TB,
	description string,
	want T,
	q xsql.Querier,
	query string,
	args ...any,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), WaitTimeout)
	defer cancel()

	var lastErr error

	for {
		row := q.QueryRowContext(
			ctx,
			query,
			args...,
		)

		var got T
		if err := row.Scan(&got); err == nil {
			if got == want {
				return
			}

			lastErr = nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			if lastErr != nil {
				t.Fatalf("expectation failed: %s: %s", description, lastErr)
			}

			t.Fatalf("expectation failed: %s: timed out waiting for value to become %v", description, want)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// WaitForLockWaiters waits until at least n of the given database's connections
// are waiting for a lock. If this does not occur within [WaitTimeout], the test
// fails.
func WaitForLockWaiters(t testing.TB, db *sql.DB, n int) {
	t.Helper()

	WaitUntil(t, func() bool {
		return CountLockWaiters(t, db) >= n
	})
}

// CountLockWaiters returns the number of the given database's connections that
// are waiting for a lock.
func CountLockWaiters(t testing.TB, db *sql.DB) int {
	t.Helper()

	var waiting int
	if err := db.QueryRowContext(
		t.Context(),
		`SELECT COUNT(*)
		FROM pg_stat_activity
		WHERE datname = current_database()
		AND wait_event_type = 'Lock'`,
	).Scan(&waiting); err != nil {
		t.Fatal(err)
	}

	return waiting
}

// WaitUntil calls done repeatedly until it returns true. If this does not occur
// within [WaitTimeout], the test fails.
func WaitUntil(t testing.TB, done func() bool) {
	t.Helper()

	timeout := time.After(WaitTimeout)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for !done() {
		select {
		case <-timeout:
			t.Fatal("timed out waiting for the database")
		case <-ticker.C:
		}
	}
}
