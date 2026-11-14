package schema_test

import (
	"context"
	"database/sql"
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

	xtesting.ExpectQueryResult(
		t,
		"the queue is created on a database that never had the old tables",
		true,
		db,
		`SELECT to_regclass('messagequeue.messages') IS NOT NULL`,
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

// TestCreate_doesNotRequireExclusiveCheckpointLock verifies that applying an
// already-upgraded schema does not wait for checkpoint writes to finish.
func TestCreate_doesNotRequireExclusiveCheckpointLock(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabase(t)

	xtesting.Transact(t, db, func(tx *sql.Tx) {
		// Keep the checkpoint write uncommitted while applying the schema
		// through another database connection.
		if _, err := tx.ExecContext(
			t.Context(),
			`INSERT INTO eventstream.handler_checkpoints (
                handler_key,
                stream_id
            ) VALUES (
                '65d5a8ca-e2a5-4c07-aa17-c847c4b2b322',
                '02b888ed-e546-4fba-b524-426dfbbefac0'
            )`,
		); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
		defer cancel()

		if err := schema.Create(ctx, db); err != nil {
			t.Fatalf("unable to apply schema during a checkpoint write: %v", err)
		}
	})
}

// TestCreate_migratesQueues verifies that schema upgrades move pending commands
// and deadlines from the tables of earlier versions into the queue.
func TestCreate_migratesQueues(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseWithoutSchema(t)
	if _, err := db.ExecContext(
		t.Context(),
		`CREATE SCHEMA commandqueue;

		CREATE TABLE commandqueue.commands (
			message_id      uuid        PRIMARY KEY,
			message_type_id uuid        NOT NULL,
			envelope        bytea       NOT NULL,
			failures        int         NOT NULL DEFAULT 0,
			enqueued_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
			deliver_at      timestamptz NOT NULL DEFAULT clock_timestamp()
		);

		CREATE TABLE commandqueue.idempotency_keys (
			idempotency_key text PRIMARY KEY,
			message_id      uuid NOT NULL UNIQUE
		);

		CREATE SCHEMA process;

		CREATE TABLE process.deadlines (
			message_id      uuid        PRIMARY KEY,
			message_type_id uuid        NOT NULL,
			handler_key     uuid        NOT NULL,
			instance_id     text        NOT NULL,
			envelope        bytea       NOT NULL,
			failures        int         NOT NULL DEFAULT 0,
			deliver_at      timestamptz NOT NULL
		);

		INSERT INTO commandqueue.commands (
			message_id,
			message_type_id,
			envelope,
			failures,
			enqueued_at,
			deliver_at
		) VALUES (
			'0d3b3f4a-6c1e-4f4e-9d6b-2b4f0f1a6e01',
			'7c1b1c2d-3e4f-4a5b-8c6d-9e0f1a2b3c01',
			'\x01',
			0,
			'2100-01-01T00:00:00Z',
			'2100-01-01T00:00:00Z'
		), (
			'0d3b3f4a-6c1e-4f4e-9d6b-2b4f0f1a6e02',
			'7c1b1c2d-3e4f-4a5b-8c6d-9e0f1a2b3c01',
			'\x02',
			3,
			'2100-01-01T00:00:00Z',
			'2100-01-01T00:05:00Z'
		);

		INSERT INTO commandqueue.idempotency_keys (
			idempotency_key,
			message_id
		) VALUES (
			'<key>',
			'0d3b3f4a-6c1e-4f4e-9d6b-2b4f0f1a6e01'
		);

		INSERT INTO process.deadlines (
			message_id,
			message_type_id,
			handler_key,
			instance_id,
			envelope,
			failures,
			deliver_at
		) VALUES (
			'0d3b3f4a-6c1e-4f4e-9d6b-2b4f0f1a6e03',
			'7c1b1c2d-3e4f-4a5b-8c6d-9e0f1a2b3c02',
			'65d5a8ca-e2a5-4c07-aa17-c847c4b2b322',
			'<instance>',
			'\x03',
			0,
			'2100-01-01T01:00:00Z'
		), (
			'0d3b3f4a-6c1e-4f4e-9d6b-2b4f0f1a6e04',
			'7c1b1c2d-3e4f-4a5b-8c6d-9e0f1a2b3c02',
			'65d5a8ca-e2a5-4c07-aa17-c847c4b2b322',
			'<instance>',
			'\x04',
			2,
			'2100-01-01T01:05:00Z'
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
			"every pending message is in the queue once",
			4,
			db,
			`SELECT COUNT(*)
			FROM messagequeue.messages`,
		)
	}

	xtesting.ExpectQueryResult(
		t,
		"a command is queued under its message type",
		true,
		db,
		`SELECT queue_id = message_type_id
			AND handler_key IS NULL
			AND instance_id IS NULL
			AND envelope = '\x01'
			AND failures = 0
			AND first_failed_at IS NULL
			AND NOT failure_logged_as_error
			AND scheduled_for = '2100-01-01T00:00:00Z'::timestamptz
			AND next_attempt_at = '2100-01-01T00:00:00Z'::timestamptz
		FROM messagequeue.messages
		WHERE message_id = '0d3b3f4a-6c1e-4f4e-9d6b-2b4f0f1a6e01'`,
	)

	xtesting.ExpectQueryResult(
		t,
		"a failing command keeps its failures, and its streak starts at its next attempt",
		true,
		db,
		`SELECT failures = 3
			AND first_failed_at = '2100-01-01T00:05:00Z'::timestamptz
			AND NOT failure_logged_as_error
			AND scheduled_for = '2100-01-01T00:00:00Z'::timestamptz
			AND next_attempt_at = '2100-01-01T00:05:00Z'::timestamptz
		FROM messagequeue.messages
		WHERE message_id = '0d3b3f4a-6c1e-4f4e-9d6b-2b4f0f1a6e02'`,
	)

	xtesting.ExpectQueryResult(
		t,
		"a deadline is queued under its handler",
		true,
		db,
		`SELECT queue_id = '65d5a8ca-e2a5-4c07-aa17-c847c4b2b322'
			AND handler_key = '65d5a8ca-e2a5-4c07-aa17-c847c4b2b322'
			AND instance_id = '<instance>'
			AND envelope = '\x03'
			AND failures = 0
			AND first_failed_at IS NULL
			AND NOT failure_logged_as_error
			AND scheduled_for = '2100-01-01T01:00:00Z'::timestamptz
			AND next_attempt_at = '2100-01-01T01:00:00Z'::timestamptz
		FROM messagequeue.messages
		WHERE message_id = '0d3b3f4a-6c1e-4f4e-9d6b-2b4f0f1a6e03'`,
	)

	xtesting.ExpectQueryResult(
		t,
		"a failing deadline keeps its failures, and its streak starts at its next attempt",
		true,
		db,
		`SELECT failures = 2
			AND first_failed_at = '2100-01-01T01:05:00Z'::timestamptz
			AND NOT failure_logged_as_error
			AND scheduled_for = '2100-01-01T01:05:00Z'::timestamptz
			AND next_attempt_at = '2100-01-01T01:05:00Z'::timestamptz
		FROM messagequeue.messages
		WHERE message_id = '0d3b3f4a-6c1e-4f4e-9d6b-2b4f0f1a6e04'`,
	)

	xtesting.ExpectQueryResult(
		t,
		"the idempotency key still refers to its command",
		true,
		db,
		`SELECT message_id = '0d3b3f4a-6c1e-4f4e-9d6b-2b4f0f1a6e01'
		FROM messagequeue.idempotency_keys
		WHERE idempotency_key = '<key>'`,
	)

	xtesting.ExpectQueryResult(
		t,
		"the old tables are gone",
		true,
		db,
		`SELECT to_regclass('commandqueue.commands') IS NULL
			AND to_regclass('commandqueue.idempotency_keys') IS NULL
			AND to_regclass('process.deadlines') IS NULL`,
	)

	xtesting.ExpectQueryResult(
		t,
		"the commandqueue schema is gone",
		0,
		db,
		`SELECT COUNT(*)
		FROM information_schema.schemata
		WHERE schema_name = 'commandqueue'`,
	)
}
