// Package v2_0_0_test tests the migration to schema version v2.0.0, which is
// in this directory.
package v2_0_0_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/runkit/internal/schema"
	"github.com/dogmatiq/runkit/internal/schema/internal/schematest"
	"github.com/dogmatiq/runkit/internal/schema/internal/version"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	testLockTimeout      = 5 * time.Second
	testStatementTimeout = 30 * time.Second
)

// createSchemaOfV0_2_0 creates the schema of runkit v0.2.0 in the given
// database. That runkit version creates schema version v1.1.0, which the
// migration to v2.0.0 upgrades.
func createSchemaOfV0_2_0(t testing.TB, db *sql.DB) {
	t.Helper()
	schematest.CreateSchemaOf(t, db, "v0.2.0", version.Version{1, 1, 0})
}

// TestMigrate_toV2_0_0_waitsForEngineWrites verifies that the migration to
// schema version v2.0.0 doesn't lose a command that an engine built for the
// earlier schema adds while the migration runs.
func TestMigrate_toV2_0_0_waitsForEngineWrites(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseWithoutSchema(t)
	createSchemaOfV0_2_0(t, db)

	// Hold a lock on a table that the migration locks, so that the migration
	// waits.
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

	inserted := make(chan error, 1)
	go func() {
		_, err := db.ExecContext(
			t.Context(),
			`INSERT INTO commandqueue.commands (
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
			)`,
		)
		inserted <- err
	}()

	// Wait until the engine's insert has either finished or is waiting for a
	// lock that the migration holds.
	var (
		insertErr      error
		insertFinished bool
	)
	xtesting.WaitUntil(t, func() bool {
		select {
		case insertErr = <-inserted:
			insertFinished = true
			return true
		default:
			return xtesting.CountLockWaiters(t, db) >= 2
		}
	})

	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := <-migrated; err != nil {
		t.Fatal(err)
	}

	if !insertFinished {
		insertErr = <-inserted
	}

	if insertErr != nil {
		// The engine's write failed because the migration dropped the table,
		// so the engine's caller knows that the command wasn't accepted. Any
		// other error means that the insert never reached the table.
		pgErr, ok := errors.AsType[*pgconn.PgError](insertErr)
		if !ok || pgErr.Code != pgerrcode.UndefinedTable {
			t.Fatalf("unexpected error from the engine's insert: %v", insertErr)
		}

		xtesting.ExpectQueryResult(
			t,
			"the command that the engine failed to add isn't in the queue",
			0,
			db,
			`SELECT COUNT(*)
			FROM runkit.queued_messages`,
		)

		return
	}

	xtesting.ExpectQueryResult(
		t,
		"the command that the engine added is in the queue",
		1,
		db,
		`SELECT COUNT(*)
		FROM runkit.queued_messages`,
	)
}

// TestMigrate_toV2_0_0_waitsForEngineDeliveries verifies that the migration to
// schema version v2.0.0 doesn't copy a command that an engine built for the
// earlier schema delivers while the migration runs.
func TestMigrate_toV2_0_0_waitsForEngineDeliveries(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseWithoutSchema(t)
	createSchemaOfV0_2_0(t, db)

	if _, err := db.ExecContext(
		t.Context(),
		`INSERT INTO commandqueue.commands (
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
		)`,
	); err != nil {
		t.Fatal(err)
	}

	// The engine deletes the command when it has delivered it, in a
	// transaction that commits while the migration runs.
	delivery, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer delivery.Rollback()

	if _, err := delivery.ExecContext(
		t.Context(),
		`DELETE FROM commandqueue.commands`,
	); err != nil {
		t.Fatal(err)
	}

	migrated := make(chan error, 1)
	go func() {
		migrated <- schema.Apply(t.Context(), db, true, testLockTimeout, testStatementTimeout)
	}()

	xtesting.WaitForLockWaiters(t, db, 1)

	if err := delivery.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := <-migrated; err != nil {
		t.Fatal(err)
	}

	xtesting.ExpectQueryResult(
		t,
		"the delivered command isn't in the queue",
		0,
		db,
		`SELECT COUNT(*)
		FROM runkit.queued_messages`,
	)
}

// TestMigrate_toV2_0_0_keepsApplicationSchemas verifies that the migration to
// schema version v2.0.0 keeps an old schema that holds the application's own
// tables. For example, projectionkit keeps its checkpoints in the "projection"
// schema.
func TestMigrate_toV2_0_0_keepsApplicationSchemas(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabaseWithoutSchema(t)
	createSchemaOfV0_2_0(t, db)

	if _, err := db.ExecContext(
		t.Context(),
		`CREATE TABLE projection.checkpoint (
			id text PRIMARY KEY
		)`,
	); err != nil {
		t.Fatal(err)
	}

	if err := schema.Apply(t.Context(), db, true, testLockTimeout, testStatementTimeout); err != nil {
		t.Fatal(err)
	}

	xtesting.ExpectQueryResult(
		t,
		"the schema holds only the application's table",
		"checkpoint",
		db,
		`SELECT string_agg(table_name, ', ')
		FROM information_schema.tables
		WHERE table_schema = 'projection'`,
	)
}
