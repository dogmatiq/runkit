package schema_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dogmatiq/runkit/internal/schema"
	"github.com/dogmatiq/runkit/internal/schema/internal/schematest"
	"github.com/dogmatiq/runkit/internal/schema/internal/version"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

const (
	testLockTimeout      = 5 * time.Second
	testStatementTimeout = 30 * time.Second
)

func TestApplyAndDrop(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseWithoutSchema(t)

	var baseline int
	if err := db.QueryRow(
		`SELECT COUNT(*)
		FROM information_schema.schemata`,
	).Scan(&baseline); err != nil {
		t.Fatal(err)
	}

	if err := schema.Apply(t.Context(), db, false, testLockTimeout, testStatementTimeout); err != nil {
		t.Fatal(err)
	}

	xtesting.ExpectQueryResult(
		t,
		"schema count increased after Apply()",
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
		`SELECT to_regclass('runkit.queued_messages') IS NOT NULL`,
	)

	// A database at the target schema version needs no change.
	if err := schema.Apply(t.Context(), db, false, testLockTimeout, testStatementTimeout); err != nil {
		t.Fatalf("unable to apply the schema again: %s", err)
	}

	xtesting.ExpectQueryResult(
		t,
		"the database is at the current schema version",
		schema.TargetVersion,
		db,
		`SELECT version
		FROM runkit.schema_version`,
	)

	xtesting.ExpectQueryResult(
		t,
		"no migration is in progress",
		true,
		db,
		`SELECT migrating_to IS NULL
			AND migration_steps_completed IS NULL
		FROM runkit.schema_version`,
	)

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

// TestApply_reappliesCurrent verifies that "current" can be applied to a
// database that already has it. Every upgrade applies "current", including one
// with no migrations to run, such as an upgrade that raises only the patch
// number.
//
// The test changes [schema.TargetVersion], so it must not call t.Parallel().
// Go runs the parallel tests only after every test that doesn't call it.
func TestApply_reappliesCurrent(t *testing.T) {
	db := xtesting.NewDatabaseAsOwner(t)

	target := schema.TargetVersion
	t.Cleanup(func() { schema.TargetVersion = target })

	schema.TargetVersion = version.Version{
		target.Major(),
		target.Minor(),
		target.Patch() + 1,
	}

	if err := schema.Apply(t.Context(), db, false, testLockTimeout, testStatementTimeout); err != nil {
		t.Fatal(err)
	}

	xtesting.ExpectQueryResult(
		t,
		"the database is at the new target schema version",
		schema.TargetVersion,
		db,
		`SELECT version
		FROM runkit.schema_version`,
	)
}

// TestApply_doesNotDeadlockWithEngineTransactions verifies that an upgrade and a
// transaction of a running engine both finish when they run at the same time.
//
// Each case is a transaction that locks some tables, then others, as the engine
// does when it appends events and when it delivers a message. The test starts
// the upgrade between the two groups of locks. The upgrade must lock the tables
// in the same order as the transaction. Otherwise, each of them can wait for a
// lock that the other holds, and PostgreSQL ends one of them with a deadlock
// error.
//
// The test changes [schema.TargetVersion], so it must not call t.Parallel().
// Go runs the parallel tests only after every test that doesn't call it.
func TestApply_doesNotDeadlockWithEngineTransactions(t *testing.T) {
	cases := []struct {
		Name string

		// First is the statement that takes the transaction's first locks.
		First string

		// Second is the statement that takes the transaction's other locks. It
		// runs after the upgrade is waiting for a lock.
		Second string
	}{
		{
			Name: "an event append",

			// Creating the event stream locks the "event_streams" table.
			First: `INSERT INTO runkit.event_streams (id)
				VALUES ('3c8f6f0e-6a0b-4f43-9c53-6d2a6c1b7e01')`,

			// Appending the event locks the "events" table.
			Second: `SELECT runkit.append_events_to_specific_stream(
				'3c8f6f0e-6a0b-4f43-9c53-6d2a6c1b7e01',
				'5b0d9a52-1f5e-4c0b-8a43-0e6f3d2c9a02',
				NULL,
				NULL,
				ARRAY[
					ROW(
						'a4e1c7d3-9b2f-4e68-b0c5-7f1d8e3a6b03',
						'd2f7b8a1-4c3e-4d59-a6e0-9c8b7a5f4e04',
						'\x01'
					)::runkit.event
				]
			)`,
		},
		{
			Name: "a delivery",

			// Adding a message to a queue locks the "queued_messages" table.
			First: `INSERT INTO runkit.queued_messages (
					message_id,
					message_type_id,
					queue_id,
					envelope,
					scheduled_for,
					next_attempt_at
				) VALUES (
					'0d3b3f4a-6c1e-4f4e-9d6b-2b4f0f1a6e01',
					'7c1b1c2d-3e4f-4a5b-8c6d-9e0f1a2b3c01',
					'9e8d7c6b-5a49-4382-b1a0-0f9e8d7c6b02',
					'\x01',
					now(),
					now()
				)`,

			// Recording an event locks the "event_streams" and "events" tables.
			Second: `SELECT runkit.append_events_to_any_stream(
				'5b0d9a52-1f5e-4c0b-8a43-0e6f3d2c9a02',
				NULL,
				NULL,
				ARRAY[
					ROW(
						'a4e1c7d3-9b2f-4e68-b0c5-7f1d8e3a6b03',
						'd2f7b8a1-4c3e-4d59-a6e0-9c8b7a5f4e04',
						'\x01'
					)::runkit.event
				]
			)`,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db := xtesting.NewDatabaseAsOwner(t)

			target := schema.TargetVersion
			t.Cleanup(func() { schema.TargetVersion = target })

			schema.TargetVersion = version.Version{
				target.Major(),
				target.Minor(),
				target.Patch() + 1,
			}

			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()

			if _, err := tx.ExecContext(t.Context(), c.First); err != nil {
				t.Fatal(err)
			}

			upgraded := make(chan error, 1)
			go func() {
				upgraded <- schema.Apply(t.Context(), db, false, testLockTimeout, testStatementTimeout)
			}()

			// The upgrade is now waiting for a lock that the transaction holds.
			xtesting.WaitForLockWaiters(t, db, 1)

			if _, err := tx.ExecContext(t.Context(), c.Second); err != nil {
				t.Fatalf("unable to run the transaction during an upgrade: %v", err)
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			if err := <-upgraded; err != nil {
				t.Fatalf("unable to upgrade during the transaction: %v", err)
			}
		})
	}
}

// TestApply_newerCompatibleDatabase verifies that a database whose schema
// version is above the current one, with the same major number, is left alone.
func TestApply_newerCompatibleDatabase(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseAsOwner(t)

	newer := fmt.Sprintf(
		"v%d.%d.0",
		schema.TargetVersion.Major(),
		schema.TargetVersion.Minor()+1,
	)

	if _, err := db.ExecContext(
		t.Context(),
		`UPDATE runkit.schema_version
		SET version = $1,
			applied_at = '2100-01-01T00:00:00Z'`,
		newer,
	); err != nil {
		t.Fatal(err)
	}

	for _, offline := range []bool{false, true} {
		if err := schema.Apply(t.Context(), db, offline, testLockTimeout, testStatementTimeout); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectQueryResult(
			t,
			"the schema version is unchanged",
			true,
			db,
			`SELECT version = $1
				AND applied_at = '2100-01-01T00:00:00Z'::timestamptz
			FROM runkit.schema_version`,
			newer,
		)
	}
}

// TestApply_databaseWithNewerMajorNumber verifies that a database whose
// schema version has a higher major number than the current one is refused.
func TestApply_databaseWithNewerMajorNumber(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseAsOwner(t)

	newer := fmt.Sprintf("v%d.0.0", schema.TargetVersion.Major()+1)

	if _, err := db.ExecContext(
		t.Context(),
		`UPDATE runkit.schema_version
		SET version = $1`,
		newer,
	); err != nil {
		t.Fatal(err)
	}

	for _, offline := range []bool{false, true} {
		err := schema.Apply(t.Context(), db, offline, testLockTimeout, testStatementTimeout)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}

		if _, ok := errors.AsType[schema.IncompatibleSchemaError](err); !ok {
			t.Fatalf("unexpected error: %s", err)
		}
	}

	xtesting.ExpectQueryResult(
		t,
		"the schema version is unchanged",
		newer,
		db,
		`SELECT version
		FROM runkit.schema_version`,
	)
}

// TestApply_migrationInProgressWithNewerMajorNumber verifies that a database
// is refused while a migration that raises the major number above the current
// one is in progress, even though its recorded schema version is the current
// one.
func TestApply_migrationInProgressWithNewerMajorNumber(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseAsOwner(t)

	newer := fmt.Sprintf("v%d.0.0", schema.TargetVersion.Major()+1)

	if _, err := db.ExecContext(
		t.Context(),
		`UPDATE runkit.schema_version
		SET migrating_to = $1,
			migration_steps_completed = 1`,
		newer,
	); err != nil {
		t.Fatal(err)
	}

	for _, offline := range []bool{false, true} {
		err := schema.Apply(t.Context(), db, offline, testLockTimeout, testStatementTimeout)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}

		if _, ok := errors.AsType[schema.IncompatibleMigrationError](err); !ok {
			t.Fatalf("unexpected error: %s", err)
		}
	}

	xtesting.ExpectQueryResult(
		t,
		"the migration is still in progress",
		true,
		db,
		`SELECT version = $1
			AND migrating_to = $2
			AND migration_steps_completed = 1
		FROM runkit.schema_version`,
		schema.TargetVersion,
		newer,
	)
}

// TestApply_migrationInProgressWithNewerMinorNumber verifies that a database
// is accepted while a migration that raises only the minor number is in
// progress, because such a change is safe while engines are running.
func TestApply_migrationInProgressWithNewerMinorNumber(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseAsOwner(t)

	newer := fmt.Sprintf(
		"v%d.%d.0",
		schema.TargetVersion.Major(),
		schema.TargetVersion.Minor()+1,
	)

	if _, err := db.ExecContext(
		t.Context(),
		`UPDATE runkit.schema_version
		SET migrating_to = $1,
			migration_steps_completed = 1`,
		newer,
	); err != nil {
		t.Fatal(err)
	}

	for _, offline := range []bool{false, true} {
		if err := schema.Apply(t.Context(), db, offline, testLockTimeout, testStatementTimeout); err != nil {
			t.Fatal(err)
		}
	}

	xtesting.ExpectQueryResult(
		t,
		"the migration is still in progress",
		true,
		db,
		`SELECT version = $1
			AND migrating_to = $2
			AND migration_steps_completed = 1
		FROM runkit.schema_version`,
		schema.TargetVersion,
		newer,
	)
}

// TestApply_migrationInProgressOnDatabaseThatNeedsUpgrade verifies that a
// database that needs an upgrade is refused while a migration to a schema
// version above the current one is in progress, because this runkit version
// has no such migration and so can't finish the upgrade.
func TestApply_migrationInProgressOnDatabaseThatNeedsUpgrade(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseAsOwner(t)

	// An upgrade by a later runkit version, from a schema version with a lower
	// major number, leaves the database like this if it stops after the
	// migration to the current schema version.
	older := fmt.Sprintf("v%d.0.0", schema.TargetVersion.Major()-1)
	newer := fmt.Sprintf(
		"v%d.%d.0",
		schema.TargetVersion.Major(),
		schema.TargetVersion.Minor()+1,
	)

	if _, err := db.ExecContext(
		t.Context(),
		`UPDATE runkit.schema_version
		SET version = $1,
			migrating_to = $2,
			migration_steps_completed = 1`,
		older,
		newer,
	); err != nil {
		t.Fatal(err)
	}

	for _, offline := range []bool{false, true} {
		err := schema.Apply(t.Context(), db, offline, testLockTimeout, testStatementTimeout)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}

		if _, ok := errors.AsType[schema.IncompatibleMigrationError](err); !ok {
			t.Fatalf("unexpected error: %s", err)
		}
	}

	xtesting.ExpectQueryResult(
		t,
		"the schema version and the migration in progress are unchanged",
		true,
		db,
		`SELECT version = $1
			AND migrating_to = $2
			AND migration_steps_completed = 1
		FROM runkit.schema_version`,
		older,
		newer,
	)
}

// TestApply_invalidIndex verifies that a database that needs an upgrade is
// refused while it has an invalid index in the engine's schema, and that the
// error names the index.
//
// The database also needs an upgrade that raises the major number. An online
// call must still report the index, because fixing it doesn't need the
// application stopped, and the major upgrade does.
func TestApply_invalidIndex(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseAsOwner(t)

	older := fmt.Sprintf("v%d.0.0", schema.TargetVersion.Major()-1)

	if _, err := db.ExecContext(
		t.Context(),
		`UPDATE runkit.schema_version
		SET version = $1`,
		older,
	); err != nil {
		t.Fatal(err)
	}

	// A unique index built concurrently over duplicate values fails, and
	// leaves the index invalid, as an interrupted build does.
	if _, err := db.ExecContext(
		t.Context(),
		`CREATE TABLE runkit.duplicates (value int);
		INSERT INTO runkit.duplicates VALUES (1), (1)`,
	); err != nil {
		t.Fatal(err)
	}

	if _, err := db.ExecContext(
		t.Context(),
		`CREATE UNIQUE INDEX CONCURRENTLY duplicates_by_value
		ON runkit.duplicates (value)`,
	); err == nil {
		t.Fatal("expected the index build to fail")
	}

	for _, offline := range []bool{false, true} {
		err := schema.Apply(t.Context(), db, offline, testLockTimeout, testStatementTimeout)
		if err == nil {
			t.Fatalf("offline = %t: expected an error, got nil", offline)
		}

		invalid, ok := errors.AsType[schema.InvalidIndexesError](err)
		if !ok {
			t.Fatalf("offline = %t: unexpected error: %s", offline, err)
		}

		if !slices.Contains(invalid.Indexes, "runkit.duplicates_by_value") {
			t.Fatalf("offline = %t: the error doesn't name the invalid index: %s", offline, err)
		}
	}

	xtesting.ExpectQueryResult(
		t,
		"the schema version is unchanged",
		older,
		db,
		`SELECT version
		FROM runkit.schema_version`,
	)
}

// TestApply_raisingMajorNumberNeedsOffline verifies that migrating while
// engines may be running refuses to raise the major number of the schema
// version, and changes nothing.
func TestApply_raisingMajorNumberNeedsOffline(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseWithoutSchema(t)
	createSchemaOf(t, db, "v0.2.0")

	err := schema.Apply(t.Context(), db, false, testLockTimeout, testStatementTimeout)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}

	if _, ok := errors.AsType[schema.OfflineUpgradeRequiredError](err); !ok {
		t.Fatalf("unexpected error: %s", err)
	}

	xtesting.ExpectQueryResult(
		t,
		"the old tables are still there",
		true,
		db,
		`SELECT to_regclass('commandqueue.commands') IS NOT NULL
			AND to_regclass('process.deadlines') IS NOT NULL
			AND to_regclass('runkit.queued_messages') IS NULL`,
	)

	xtesting.ExpectQueryResult(
		t,
		"the database has no schema of its own",
		false,
		db,
		`SELECT EXISTS (
			SELECT
			FROM pg_namespace
			WHERE nspname = 'runkit'
		)`,
	)
}

// TestApply_concurrentCalls verifies that calls to migrate the same database
// at the same time all succeed.
func TestApply_concurrentCalls(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseWithoutSchema(t)

	var (
		wg   sync.WaitGroup
		errs = make([]error, 8)
	)

	for i := range errs {
		wg.Go(func() {
			errs[i] = schema.Apply(t.Context(), db, false, testLockTimeout, testStatementTimeout)
		})
	}

	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}

	xtesting.ExpectQueryResult(
		t,
		"the database is at the current schema version",
		schema.TargetVersion,
		db,
		`SELECT version
		FROM runkit.schema_version`,
	)
}

// TestApply_doesNotWaitForOtherCallWhenSatisfied verifies that a call returns
// without waiting for another call's migration to finish when the database is
// already at a schema version that satisfies the target.
func TestApply_doesNotWaitForOtherCallWhenSatisfied(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseWithoutSchema(t)
	createSchemaOf(t, db, "v0.2.0")

	// Lock a table that the migration needs to lock. The call to Apply that
	// migrates the database then waits for the table, and holds the advisory
	// lock while it waits.
	blocker, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()

	if _, err := blocker.ExecContext(
		t.Context(),
		`SELECT COUNT(*) FROM commandqueue.idempotency_keys`,
	); err != nil {
		t.Fatal(err)
	}

	migrated := make(chan error, 1)
	go func() {
		migrated <- schema.Apply(t.Context(), db, true, testLockTimeout, testStatementTimeout)
	}()

	xtesting.WaitForLockWaiters(t, db, 1)

	// Record the target schema version. A database has this version recorded
	// while a newer runkit version migrates it to a later one.
	if _, err := db.ExecContext(
		t.Context(),
		`UPDATE runkit.schema_version
		SET version = $1`,
		schema.TargetVersion,
	); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
	defer cancel()

	if err := schema.Apply(ctx, db, false, testLockTimeout, testStatementTimeout); err != nil {
		t.Fatalf("unable to return while another call is migrating: %v", err)
	}

	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := <-migrated; err != nil {
		t.Fatal(err)
	}
}

// TestApply_doesNotWaitForCheckpointWrites verifies that migrating a database
// that is at the current schema version does not wait for checkpoint writes to
// finish.
func TestApply_doesNotWaitForCheckpointWrites(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseAsOwner(t)

	xtesting.Transact(t, db, func(tx *sql.Tx) {
		// Keep the checkpoint write uncommitted while migrating through
		// another database connection.
		if _, err := tx.ExecContext(
			t.Context(),
			`INSERT INTO runkit.process_handler_checkpoints (
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

		if err := schema.Apply(ctx, db, false, testLockTimeout, testStatementTimeout); err != nil {
			t.Fatalf("unable to migrate during a checkpoint write: %v", err)
		}
	})
}

// TestApply_doesNotWaitForProcessInstanceWrites verifies that an upgrade that
// applies "current" again does not wait for writes to process instances to
// finish.
//
// The engine's transactions lock the "process_instances" and "queued_messages"
// tables in both orders, so an upgrade that locked "process_instances" could
// deadlock with them.
//
// The test changes [schema.TargetVersion], so it must not call t.Parallel().
// Go runs the parallel tests only after every test that doesn't call it.
func TestApply_doesNotWaitForProcessInstanceWrites(t *testing.T) {
	db := xtesting.NewDatabaseAsOwner(t)

	target := schema.TargetVersion
	t.Cleanup(func() { schema.TargetVersion = target })

	schema.TargetVersion = version.Version{
		target.Major(),
		target.Minor(),
		target.Patch() + 1,
	}

	xtesting.Transact(t, db, func(tx *sql.Tx) {
		// Keep the write uncommitted while upgrading through another database
		// connection.
		if _, err := tx.ExecContext(
			t.Context(),
			`INSERT INTO runkit.process_instances (
				handler_key,
				instance_id
			) VALUES (
				'65d5a8ca-e2a5-4c07-aa17-c847c4b2b322',
				'<instance>'
			)`,
		); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
		defer cancel()

		if err := schema.Apply(ctx, db, false, testLockTimeout, testStatementTimeout); err != nil {
			t.Fatalf("unable to upgrade during a process instance write: %v", err)
		}
	})
}

// TestEnqueueCommand_usesQueuedAt verifies that a command is first due at the
// time that the caller gives, not at the time of the database's clock.
func TestEnqueueCommand_usesQueuedAt(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseAsOwner(t)

	if _, err := db.ExecContext(
		t.Context(),
		`SELECT runkit.enqueue_command(
			'0d3b3f4a-6c1e-4f4e-9d6b-2b4f0f1a6e01',
			'7a1c2e3f-4b5d-4c6e-8f70-91a2b3c4d5e6',
			'5e4d3c2b-1a09-4f8e-9d7c-6b5a49382716',
			'\x01',
			'',
			'2100-01-01T00:00:00Z'
		)`,
	); err != nil {
		t.Fatal(err)
	}

	xtesting.ExpectQueryResult(
		t,
		"the command is first due at the given time",
		true,
		db,
		`SELECT scheduled_for = '2100-01-01T00:00:00Z'::timestamptz
			AND next_attempt_at = '2100-01-01T00:00:00Z'::timestamptz
		FROM runkit.queued_messages
		WHERE message_id = '0d3b3f4a-6c1e-4f4e-9d6b-2b4f0f1a6e01'`,
	)
}

// TestSaveAggregateInstance_countsBindings verifies that each instance that
// binds to an event stream is counted once, for its handler and the stream.
func TestSaveAggregateInstance_countsBindings(t *testing.T) {
	t.Parallel()

	const (
		handlerA = "65d5a8ca-e2a5-4c07-aa17-c847c4b2b322"
		handlerB = "9b1d7f3e-2c4a-4e58-8a6d-0f3b5c7e9a11"
	)

	// save records one event for an instance. It first creates the instance,
	// unless the stream is given, because an instance that the engine saves
	// has always been created.
	save := func(t testing.TB, tx *sql.Tx, handlerKey, instanceID string, streamID any) {
		t.Helper()

		if streamID == nil {
			if _, err := tx.ExecContext(
				t.Context(),
				`INSERT INTO runkit.aggregate_instances (handler_key, instance_id)
				VALUES ($1, $2)`,
				handlerKey,
				instanceID,
			); err != nil {
				t.Fatal(err)
			}
		}

		if _, err := tx.ExecContext(
			t.Context(),
			`SELECT runkit.save_aggregate_instance(
				$1,
				$2,
				$3,
				'5b0d9a52-1f5e-4c0b-8a43-0e6f3d2c9a02',
				NULL,
				ARRAY[
					ROW(
						gen_random_uuid(),
						'd2f7b8a1-4c3e-4d59-a6e0-9c8b7a5f4e04',
						'\x01'
					)::runkit.event
				]
			)`,
			handlerKey,
			instanceID,
			streamID,
		); err != nil {
			t.Fatal(err)
		}
	}

	// expectCountsMatchInstances checks that the counts are those that counting
	// the bound instances gives, and that there is at least one count.
	expectCountsMatchInstances := func(t *testing.T, db *sql.DB, want int) {
		t.Helper()

		xtesting.ExpectQueryResult(
			t,
			"the counts match the bound instances",
			0,
			db,
			`SELECT COUNT(*)
			FROM (
				(
					SELECT handler_key, stream_id, COUNT(*) AS instances
					FROM runkit.aggregate_instances
					WHERE stream_id IS NOT NULL
					GROUP BY handler_key, stream_id
					EXCEPT
					SELECT handler_key, stream_id, instances
					FROM runkit.aggregate_stream_binding_counts
				)
				UNION ALL
				(
					SELECT handler_key, stream_id, instances
					FROM runkit.aggregate_stream_binding_counts
					EXCEPT
					SELECT handler_key, stream_id, COUNT(*)
					FROM runkit.aggregate_instances
					WHERE stream_id IS NOT NULL
					GROUP BY handler_key, stream_id
				)
			) AS differences`,
		)

		xtesting.ExpectQueryResult(
			t,
			"the number of bound instances",
			want,
			db,
			`SELECT COALESCE(SUM(instances), 0)::int
			FROM runkit.aggregate_stream_binding_counts`,
		)
	}

	t.Run("it counts each instance for its handler and stream", func(t *testing.T) {
		t.Parallel()

		db := xtesting.NewDatabase(t)

		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		for _, id := range []string{"<1>", "<2>", "<3>"} {
			save(t, tx, handlerA, id, nil)
		}
		save(t, tx, handlerB, "<1>", nil)

		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		expectCountsMatchInstances(t, db, 4)
	})

	t.Run("it doesn't count an instance again when it records more events", func(t *testing.T) {
		t.Parallel()

		db := xtesting.NewDatabase(t)

		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		save(t, tx, handlerA, "<1>", nil)

		var streamID string
		if err := tx.QueryRowContext(
			t.Context(),
			`SELECT stream_id
			FROM runkit.aggregate_instances
			WHERE handler_key = $1`,
			handlerA,
		).Scan(&streamID); err != nil {
			t.Fatal(err)
		}

		save(t, tx, handlerA, "<1>", streamID)
		save(t, tx, handlerA, "<1>", streamID)

		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		expectCountsMatchInstances(t, db, 1)
	})

	t.Run("it leaves the counts unchanged when the binding is rolled back", func(t *testing.T) {
		t.Parallel()

		db := xtesting.NewDatabase(t)

		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		save(t, tx, handlerA, "<1>", nil)

		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}

		expectCountsMatchInstances(t, db, 0)
	})
}

// TestMigrate_fromReleased verifies that migrating a database created by each
// runkit version that introduced a schema version produces the same schema as a
// new database, and keeps the data that the migrations move.
func TestMigrate_fromReleased(t *testing.T) {
	t.Parallel()

	for _, r := range readChangelogReleases(t) {
		// A runkit version that isn't new has the schema version of an earlier
		// one, and so has the same schema.
		if !r.IsNew {
			continue
		}

		t.Run(r.RunkitVersion, func(t *testing.T) {
			t.Parallel()
			testMigrateFrom(t, r.RunkitVersion)
		})
	}
}

// testMigrateFrom is the test of TestMigrate_fromReleased for one runkit
// version.
func testMigrateFrom(t *testing.T, runkitVersion string) {
	db := xtesting.NewDatabaseWithoutSchema(t)
	createSchemaOf(t, db, runkitVersion)

	// The rows that the test inserts, and the checks of what the migration
	// does with them, are for the migration to schema version v2.0.0. They use
	// tables that only a runkit version with a lower schema version has.
	beforeV2_0_0 := schemaVersionOf(t, runkitVersion).Compare(mustParseVersion(t, "v2.0.0")) < 0

	// Schema version v1.1.0 is the first to have the table that records the
	// database's application.
	hasIdentity := schemaVersionOf(t, runkitVersion).Compare(mustParseVersion(t, "v1.1.0")) >= 0

	if beforeV2_0_0 {
		insertRowsBeforeV2_0_0(t, db, hasIdentity)
	}

	for range 2 {
		if err := schema.Apply(t.Context(), db, true, testLockTimeout, testStatementTimeout); err != nil {
			t.Fatal(err)
		}

		if beforeV2_0_0 {
			xtesting.ExpectQueryResult(
				t,
				"every pending message is in the queue once",
				4,
				db,
				`SELECT COUNT(*)
				FROM runkit.queued_messages`,
			)
		}
	}

	xtesting.ExpectQueryResult(
		t,
		"the database is at the current schema version",
		true,
		db,
		`SELECT version = $1
			AND migrating_to IS NULL
			AND migration_steps_completed IS NULL
		FROM runkit.schema_version`,
		schema.TargetVersion,
	)

	if beforeV2_0_0 {
		expectRowsMigratedToV2_0_0(t, db, hasIdentity)
	}

	got := dumpSchemaObjects(t, db)
	want := dumpSchemaObjects(t, xtesting.NewDatabaseAsOwner(t))

	for _, object := range difference(want, got) {
		t.Errorf("the migrated database is missing:\n%s", object)
	}

	for _, object := range difference(got, want) {
		t.Errorf("the migrated database has more than a new database:\n%s", object)
	}
}

// insertRowsBeforeV2_0_0 inserts rows into the tables of a database at a schema
// version below v2.0.0, for [expectRowsMigratedToV2_0_0] to find after the
// migration to v2.0.0. The database has the "identity" table only if
// hasIdentity is true.
func insertRowsBeforeV2_0_0(t testing.TB, db *sql.DB, hasIdentity bool) {
	t.Helper()

	if hasIdentity {
		if _, err := db.ExecContext(
			t.Context(),
			`INSERT INTO application.identity (
				key,
				name
			) VALUES (
				'2b0c6f0e-5f3a-4a4f-9c1e-6a8d4d1b7e20',
				'<app>'
			)`,
		); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := db.ExecContext(
		t.Context(),
		`INSERT INTO eventstream.streams (id)
		VALUES ('3fc2f883-6016-4d34-b6dd-ad8cc195d3e5');

		INSERT INTO aggregate.instances (
			handler_key,
			instance_id,
			stream_id
		) VALUES (
			'65d5a8ca-e2a5-4c07-aa17-c847c4b2b322',
			'<bound-1>',
			'3fc2f883-6016-4d34-b6dd-ad8cc195d3e5'
		), (
			'65d5a8ca-e2a5-4c07-aa17-c847c4b2b322',
			'<bound-2>',
			'3fc2f883-6016-4d34-b6dd-ad8cc195d3e5'
		), (
			'65d5a8ca-e2a5-4c07-aa17-c847c4b2b322',
			'<unbound>',
			NULL
		);

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
}

// expectRowsMigratedToV2_0_0 checks that the migration to schema version v2.0.0
// moved the rows that [insertRowsBeforeV2_0_0] inserted.
func expectRowsMigratedToV2_0_0(t testing.TB, db *sql.DB, hadIdentity bool) {
	t.Helper()

	if hadIdentity {
		xtesting.ExpectQueryResult(
			t,
			"the application of the existing cluster is preserved",
			true,
			db,
			`SELECT application_key = '2b0c6f0e-5f3a-4a4f-9c1e-6a8d4d1b7e20'
				AND application_name = '<app>'
			FROM runkit.cluster`,
		)

		xtesting.ExpectQueryResult(
			t,
			"the existing cluster has an ID",
			1,
			db,
			`SELECT COUNT(id)
			FROM runkit.cluster`,
		)
	} else {
		xtesting.ExpectQueryResult(
			t,
			"a database that was never bound has no cluster",
			0,
			db,
			`SELECT COUNT(*)
			FROM runkit.cluster`,
		)
	}

	xtesting.ExpectQueryResult(
		t,
		"the instances that were bound to a stream are counted",
		true,
		db,
		`SELECT instances = 2
		FROM runkit.aggregate_stream_binding_counts
		WHERE handler_key = '65d5a8ca-e2a5-4c07-aa17-c847c4b2b322'
		AND stream_id = '3fc2f883-6016-4d34-b6dd-ad8cc195d3e5'`,
	)

	xtesting.ExpectQueryResult(
		t,
		"the instance that was bound to no stream is not counted",
		1,
		db,
		`SELECT COUNT(*)
		FROM runkit.aggregate_stream_binding_counts`,
	)

	xtesting.ExpectQueryResult(
		t,
		"existing checkpoint is preserved",
		true,
		db,
		`SELECT checkpoint_offset = 42
		FROM runkit.process_handler_checkpoints
		WHERE stream_id = '3fc2f883-6016-4d34-b6dd-ad8cc195d3e5'`,
	)

	xtesting.ExpectQueryResult(
		t,
		"legacy handler registration is preserved",
		true,
		db,
		`SELECT has_checkpoint_offsets
		FROM runkit.process_handlers
		WHERE handler_key = '65d5a8ca-e2a5-4c07-aa17-c847c4b2b322'`,
	)

	// This stream is not present in the local streams table.
	if _, err := db.ExecContext(
		t.Context(),
		`INSERT INTO runkit.process_handler_checkpoints (
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
		FROM runkit.queued_messages
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
		FROM runkit.queued_messages
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
		FROM runkit.queued_messages
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
		FROM runkit.queued_messages
		WHERE message_id = '0d3b3f4a-6c1e-4f4e-9d6b-2b4f0f1a6e04'`,
	)

	xtesting.ExpectQueryResult(
		t,
		"the idempotency key still refers to its command",
		true,
		db,
		`SELECT message_id = '0d3b3f4a-6c1e-4f4e-9d6b-2b4f0f1a6e01'
		FROM runkit.command_idempotency_keys
		WHERE idempotency_key = '<key>'`,
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

// createSchemaOf creates the schema of the given released runkit version in the
// given database, in the way that it created the schema of a new database.
//
// It fails the test if it can't fetch that runkit version, or if the changelog
// doesn't state its schema version.
func createSchemaOf(t testing.TB, db *sql.DB, runkitVersion string) {
	t.Helper()
	schematest.CreateSchemaOf(t, db, runkitVersion, schemaVersionOf(t, runkitVersion))
}

// dumpObjectHeader matches the comment that pg_dump writes above each schema
// object.
var dumpObjectHeader = regexp.MustCompile(`(?m)^--\n-- Name: `)

// dumpRestrictLine matches the lines that pg_dump writes with a randomly chosen
// key, which differs between two dumps of the same schema.
var dumpRestrictLine = regexp.MustCompile(`(?m)^\\(un)?restrict .*\n`)

// dumpFooter is the comment that pg_dump writes at the end of its output.
const dumpFooter = "--\n-- PostgreSQL database dump complete\n--\n"

// dumpSchemaObjects returns the output of "pg_dump --schema-only" for the given
// database, split into one string for each schema object, sorted.
//
// The first string is the text that precedes the first object.
func dumpSchemaObjects(t testing.TB, db *sql.DB) []string {
	t.Helper()

	dump := dumpRestrictLine.ReplaceAllString(xtesting.DumpSchema(t, db), "")

	if i := strings.LastIndex(dump, dumpFooter); i != -1 {
		dump = dump[:i]
	}

	var objects []string

	previous := 0
	for _, loc := range dumpObjectHeader.FindAllStringIndex(dump, -1) {
		objects = append(objects, strings.TrimSpace(dump[previous:loc[0]]))
		previous = loc[0]
	}

	objects = append(objects, strings.TrimSpace(dump[previous:]))

	slices.Sort(objects)

	return objects
}

// difference returns the elements of a that are not in b.
func difference(a, b []string) []string {
	var result []string

	for _, element := range a {
		if !slices.Contains(b, element) {
			result = append(result, element)
		}
	}

	return result
}
