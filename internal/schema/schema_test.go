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

// TestCreate_removesLocalStreamConstraint verifies that schema upgrades retain
// existing process progress and allow checkpoint rows for remote streams.
func TestCreate_removesLocalStreamConstraint(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabase(t)
	if _, err := db.ExecContext(
		t.Context(),
		`ALTER TABLE eventstream.handler_checkpoints
		ADD CONSTRAINT handler_checkpoints_stream_id_fkey
		FOREIGN KEY (stream_id) REFERENCES eventstream.streams(id);

		INSERT INTO eventstream.streams (id)
		VALUES ('3fc2f883-6016-4d34-b6dd-ad8cc195d3e5');

		INSERT INTO eventstream.handler_checkpoints (
			handler_key,
			stream_id,
			checkpoint_offset,
			failures,
			resume_at
		) VALUES (
			'65d5a8ca-e2a5-4c07-aa17-c847c4b2b322',
			'3fc2f883-6016-4d34-b6dd-ad8cc195d3e5',
			42,
			3,
			'2100-01-01T00:00:00Z'
		);

		INSERT INTO process.handlers (
			handler_key,
			has_checkpoint_offsets
		) VALUES (
			'65d5a8ca-e2a5-4c07-aa17-c847c4b2b322',
			true
		)`,
	); err != nil {
		t.Fatal(err)
	}

	for range 2 {
		if err := schema.Create(t.Context(), db); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectQueryResult(
			t,
			"existing checkpoint and legacy retry state are preserved",
			true,
			db,
			`SELECT checkpoint_offset = 42
				AND failures = 3
				AND resume_at = '2100-01-01T00:00:00Z'::timestamptz
			FROM eventstream.handler_checkpoints
			WHERE stream_id = '3fc2f883-6016-4d34-b6dd-ad8cc195d3e5'`,
		)
	}

	xtesting.ExpectQueryResult(
		t,
		"legacy handler registration is preserved",
		true,
		db,
		`SELECT has_checkpoint_offsets
		FROM process.handlers
		WHERE handler_key = '65d5a8ca-e2a5-4c07-aa17-c847c4b2b322'`,
	)

	// This stream is not present in the local streams table.
	if _, err := db.ExecContext(
		t.Context(),
		`INSERT INTO eventstream.handler_checkpoints (
			handler_key,
			stream_id
		) VALUES (
			'65d5a8ca-e2a5-4c07-aa17-c847c4b2b322',
			'02b888ed-e546-4fba-b524-426dfbbefac0'
		)`,
	); err != nil {
		t.Fatalf("unable to checkpoint a remote stream: %v", err)
	}

	xtesting.ExpectQueryResult(
		t,
		"no replacement checkpoint table is created",
		true,
		db,
		`SELECT to_regclass('process.checkpoints') IS NULL`,
	)
}
