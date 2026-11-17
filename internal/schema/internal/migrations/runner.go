package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/dogmatiq/runkit/internal/schema/internal/version"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Runner applies migrations to a database.
//
// The database must have the "runkit.schema_version" table, with a row in it.
// The runner records its progress in that row.
type Runner struct {
	// Conn is the connection that runs every step. The caller is responsible
	// for preventing other runners from using the database at the same time.
	Conn *sql.Conn

	// Migrations are the migrations to apply, in the order in which they
	// apply.
	Migrations []Migration

	// Offline is true if the caller guarantees that no engine is using the
	// database, which removes the timeouts.
	Offline bool

	// LockTimeout is the longest that a [Transaction] step waits for a lock
	// when Offline is false.
	LockTimeout time.Duration

	// StatementTimeout is the longest that a statement of a [Transaction] step
	// runs when Offline is false.
	StatementTimeout time.Duration
}

// Run applies the migrations that haven't been applied, and stops at the first
// step that fails.
//
// If a migration is already in progress, it skips the migrations before that
// one, and the steps of that one that have completed.
//
// A [Statement] step runs without a lock timeout or a statement timeout. A
// [Transaction] step runs with the timeouts unless Offline is true.
func (r *Runner) Run(ctx context.Context) error {
	r.validate()

	migratingTo, stepsCompleted, err := r.progress(ctx)
	if err != nil {
		return err
	}

	// pendingMigrations holds the migrations that have not yet been completed,
	// starting with the migration that is currently in progress, if there is
	// one.
	pendingMigrations := r.Migrations

	if !migratingTo.IsZero() {
		i := slices.IndexFunc(
			r.Migrations,
			func(m Migration) bool {
				return m.Version == migratingTo
			},
		)

		if i == -1 {
			return fmt.Errorf(
				"a migration to schema version %s is in progress, and this runkit version doesn't know it",
				migratingTo,
			)
		}

		if m := r.Migrations[i]; stepsCompleted > len(m.Steps) {
			return fmt.Errorf(
				"the database records %d completed steps of the migration to schema version %s, which only has %d",
				stepsCompleted,
				m.Version,
				len(m.Steps),
			)
		}

		pendingMigrations = r.Migrations[i:]
	}

	for _, m := range pendingMigrations {
		for i, s := range m.Steps[stepsCompleted:] {
			completed := stepsCompleted + i + 1

			if err := r.apply(ctx, m, s, completed); err != nil {
				return fmt.Errorf(
					"unable to apply step %s of the migration to schema version %s: %w",
					s.Name,
					m.Version,
					err,
				)
			}
		}

		// Only the first pending migration can have completed steps.
		stepsCompleted = 0
	}

	return nil
}

// Transact calls fn within a transaction on the runner's connection, and
// commits it if fn returns nil.
//
// Unless Offline is true, the transaction has a lock timeout and a statement
// timeout.
func (r *Runner) Transact(
	ctx context.Context,
	fn func(context.Context, *sql.Tx) error,
) error {
	r.validate()

	lockTimeout, statementTimeout := "0", "0"
	if !r.Offline {
		lockTimeout = milliseconds(r.LockTimeout)
		statementTimeout = milliseconds(r.StatementTimeout)
	}

	return xsql.Transact(
		ctx,
		r.Conn,
		func(ctx context.Context, tx *sql.Tx) error {
			// Set the timeouts explicitly even if they are none, because the
			// database or the role may set defaults for them.
			if _, err := tx.ExecContext(
				ctx,
				`SELECT
					set_config('lock_timeout', $1, true),
					set_config('statement_timeout', $2, true)`,
				lockTimeout,
				statementTimeout,
			); err != nil {
				return fmt.Errorf("unable to set the timeouts: %w", err)
			}

			return fn(ctx, tx)
		},
	)
}

// progress returns the schema version of the migration in progress, and how
// many of its steps have completed. The schema version is zero if no migration
// is in progress.
func (r *Runner) progress(ctx context.Context) (version.Version, int, error) {
	var (
		migration sql.Null[version.Version]
		steps     sql.Null[int]
	)

	if err := r.Conn.QueryRowContext(
		ctx,
		`SELECT migrating_to, migration_steps_completed
		FROM runkit.schema_version`,
	).Scan(&migration, &steps); err != nil {
		return version.Version{}, 0, fmt.Errorf("unable to read the progress of the migration: %w", err)
	}

	return migration.V, steps.V, nil
}

// apply applies one step and records that the given number of the migration's
// steps have completed.
func (r *Runner) apply(ctx context.Context, m Migration, s Step, completed int) error {
	if s.Kind == Statement {
		return r.applyStatement(ctx, m, s, completed)
	}

	return r.Transact(
		ctx,
		func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, s.SQL); err != nil {
				return err
			}

			return record(ctx, tx, m, completed)
		},
	)
}

// applyStatement applies a [Statement] step and records that the given number
// of the migration's steps have completed.
func (r *Runner) applyStatement(ctx context.Context, m Migration, s Step, completed int) (err error) {
	// A statement step has no timeouts because it is long by nature, such as
	// building an index, and it doesn't block reads or writes.
	//
	// The timeouts last for the connection's session, so they must be reset
	// before the connection is used for anything else, even if the step fails
	// or the context is canceled.
	defer func() {
		_, resetErr := r.Conn.ExecContext(
			context.WithoutCancel(ctx),
			`RESET lock_timeout; RESET statement_timeout`,
		)
		err = errors.Join(err, resetErr)
	}()

	if _, err := r.Conn.ExecContext(
		ctx,
		`SELECT
			set_config('lock_timeout', '0', false),
			set_config('statement_timeout', '0', false)`,
	); err != nil {
		return fmt.Errorf("unable to remove the timeouts: %w", err)
	}

	// PostgreSQL rejects a prepared statement that has more than one command,
	// which is how it checks that the step holds only one.
	stmt, err := r.Conn.PrepareContext(ctx, s.SQL)
	if err != nil {
		return err
	}
	defer stmt.Close()

	if _, err := stmt.ExecContext(ctx); err != nil {
		return err
	}

	return record(ctx, r.Conn, m, completed)
}

// record records that the given number of the migration's steps have
// completed.
func record(ctx context.Context, x xsql.Executor, m Migration, completed int) error {
	return xsql.ExecOne(
		ctx,
		x,
		`UPDATE runkit.schema_version
		SET migrating_to = $1,
			migration_steps_completed = $2`,
		m.Version,
		completed,
	)
}

// validate panics if the runner's required fields are not set.
func (r *Runner) validate() {
	if r.Conn == nil {
		panic("migrations: Runner.Conn must not be nil")
	}

	if r.LockTimeout <= 0 {
		panic("migrations: Runner.LockTimeout must be positive")
	}

	if r.StatementTimeout <= 0 {
		panic("migrations: Runner.StatementTimeout must be positive")
	}
}

// milliseconds formats d as a PostgreSQL duration setting.
func milliseconds(d time.Duration) string {
	return fmt.Sprintf("%dms", d.Milliseconds())
}
