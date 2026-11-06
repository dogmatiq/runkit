package schema_test

import (
	"testing"

	"github.com/dogmatiq/runkit/internal/schema"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

func TestCreateAndDrop(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseWithoutSchema(t)

	var baseline int
	if err := db.QueryRow(
		`SELECT COUNT(*)
		FROM information_schema.schemata`,
	).Scan(&baseline); err != nil {
		t.Fatal(err)
	}

	if err := schema.Create(t.Context(), db); err != nil {
		t.Fatal(err)
	}

	xtesting.ExpectQueryResult(
		t,
		"schema count increased after Create()",
		true,
		db,
		`SELECT COUNT(*) > $1
		FROM information_schema.schemata`,
		baseline,
	)

	xtesting.ExpectQueryResult(
		t,
		"no tables created in public schema",
		0,
		db,
		`SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = 'public'`,
	)

	xtesting.ExpectQueryResult(
		t,
		"no functions created in public schema",
		0,
		db,
		`SELECT COUNT(*)
		FROM information_schema.routines
		WHERE routine_schema = 'public'`,
	)

	// Create() again to verify idempotency.
	if err := schema.Create(t.Context(), db); err != nil {
		t.Fatalf("schema creation is not idempotent: %s", err)
	}

	if err := schema.Drop(t.Context(), db); err != nil {
		t.Fatal(err)
	}

	xtesting.ExpectQueryResult(
		t,
		"schema count restored after Drop()",
		baseline,
		db,
		`SELECT COUNT(*)
		FROM information_schema.schemata`,
	)
}

// TestCreate_migratesProcessCheckpoints verifies that an existing installation
// keeps its progress, including checkpoints for streams it cannot currently read.
func TestCreate_migratesProcessCheckpoints(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseWithoutSchema(t)
	if _, err := db.ExecContext(t.Context(), `
		CREATE SCHEMA eventstream;
		CREATE SCHEMA process;
		CREATE TABLE eventstream.handler_checkpoints (
			handler_key uuid NOT NULL,
			stream_id uuid NOT NULL,
			checkpoint_offset bigint NOT NULL,
			failures int NOT NULL,
			resume_at timestamptz NOT NULL,
			PRIMARY KEY (handler_key, stream_id)
		);
		CREATE TABLE process.handlers (
			handler_key uuid PRIMARY KEY,
			has_checkpoint_offsets boolean NOT NULL
		);
		INSERT INTO eventstream.handler_checkpoints VALUES
			('65d5a8ca-e2a5-4c07-aa17-c847c4b2b322',
			 '3fc2f883-6016-4d34-b6dd-ad8cc195d3e5', 42, 3, now());
	`); err != nil {
		t.Fatal(err)
	}

	for range 2 {
		if err := schema.Create(t.Context(), db); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectQueryResult(
			t,
			"existing checkpoint is preserved",
			uint64(42),
			db,
			`SELECT checkpoint_offset FROM process.checkpoints`,
		)
	}

	xtesting.ExpectQueryResult(
		t,
		"obsolete process event pump tables are removed",
		true,
		db,
		`SELECT to_regclass('eventstream.handler_checkpoints') IS NULL
			AND to_regclass('process.handlers') IS NULL`,
	)
}
