package databasetest

import (
	"database/sql"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib" // register the "pgx" driver with database/sql
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// New returns a database pool that connects to an isolated test database.
//
// The database is removed when the test ends.
func New(t testing.TB) *sql.DB {
	t.Helper()

	username := "dogma"
	password := "dogma"

	container, err := postgres.Run(
		t.Context(),
		"postgres:18-alpine",
		testcontainers.WithReuseByName("dogmatiq-reference-engine-test"),
		postgres.BasicWaitStrategies(),
		postgres.WithUsername(username),
		postgres.WithPassword(password),
	)
	if err != nil {
		t.Fatalf("unable to start PostgreSQL container: %s", err)
	}

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

// NewWithSchema is a variant of [New] that applies the engine's schema to the
// database before returning it.
func NewWithSchema(t testing.TB) *sql.DB {
	db := New(t)

	if err := database.CreateSchema(t.Context(), db); err != nil {
		t.Fatalf("unable to apply database schema: %s", err)
	}

	return db
}

// replaceDatabaseNameInDSN replaces the database name in the given PostgreSQL
// connection string. with the given name and returns the modified connection
// string.
func replaceDatabaseNameInDSN(
	t testing.TB,
	dsn string,
	dbName string,
) string {
	t.Helper()

	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("unable to parse PostgreSQL connection string: %s", err)
	}
	config.Database = dbName

	return config.ConnString()
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
