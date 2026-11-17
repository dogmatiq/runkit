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
				"incompatible database schema: the database has an in-progress migration to %s, which this runkit version doesn't include; it may come from a hotfix to a prior runkit version",
				migratingTo,
			)
		}

		if m := r.Migrations[i]; stepsCompleted > len(m.Steps) {
			return fmt.Errorf(
				"invalid migration progress in the database: %d steps are recorded as completed for the migration to %s, which only has %d steps",
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
				if xsql.IsStatementTimeoutError(ctx, err) && !r.Offline {
					return fmt.Errorf(
						"database schema migration timed out while applying step %s of the migration to %s: the step takes longer than is allowed while the application is running, so stop every instance of the application, then apply the upgrade offline: %w",
						s.Name,
						m.Version,
						err,
					)
				}

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
// A call to fn's ExecContext fails if its SQL ends the transaction, such as
// with COMMIT or ROLLBACK. Its QueryContext and QueryRowContext don't check.
//
// Unless Offline is true, the transaction has a lock timeout and a statement
// timeout. If Offline is true, it has no timeout at all, including any
// transaction timeout that the database or the role sets.
func (r *Runner) Transact(
	ctx context.Context,
	fn func(context.Context, xsql.Executor) error,
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

			// PostgreSQL ends a session whose transaction runs for longer than
			// transaction_timeout. Nothing limits how long an offline step
			// runs, so remove that limit too.
			if r.Offline {
				if _, err := tx.ExecContext(
					ctx,
					`SELECT set_config('transaction_timeout', '0', true)`,
				); err != nil {
					return fmt.Errorf("unable to remove the transaction timeout: %w", err)
				}
			}

			g, err := newGuardedTx(ctx, tx)
			if err != nil {
				return err
			}

			return fn(ctx, g)
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
		func(ctx context.Context, tx xsql.Executor) error {
			if _, err := tx.ExecContext(ctx, s.SQL); err != nil {
				return err
			}

			return record(ctx, tx, m, completed)
		},
	)
}

// guardedTx is an [xsql.Executor] that runs statements in a transaction, and
// fails each call to ExecContext whose SQL ends that transaction.
//
// A COMMIT or ROLLBACK in the SQL ends the transaction early, and PostgreSQL
// only warns about it. The statements after it would then run outside the
// transaction, each committing by itself.
type guardedTx struct {
	tx *sql.Tx
	id string
}

// newGuardedTx returns a guardedTx for statements in tx.
func newGuardedTx(ctx context.Context, tx *sql.Tx) (*guardedTx, error) {
	id, err := transactionID(ctx, tx)
	if err != nil {
		return nil, err
	}

	return &guardedTx{tx, id}, nil
}

func (g *guardedTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	res, err := g.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	id, err := transactionID(ctx, g.tx)
	if err != nil {
		return nil, err
	}

	if id != g.id {
		return nil, errors.New("the SQL ended the transaction that it runs in, such as with COMMIT or ROLLBACK")
	}

	return res, nil
}

func (g *guardedTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return g.tx.QueryContext(ctx, query, args...)
}

func (g *guardedTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return g.tx.QueryRowContext(ctx, query, args...)
}

// transactionID returns the ID of the transaction that the connection is in.
// Outside a transaction, each call returns a different ID.
func transactionID(ctx context.Context, tx *sql.Tx) (string, error) {
	var id string
	if err := tx.QueryRowContext(
		ctx,
		`SELECT pg_current_xact_id()::text`,
	).Scan(&id); err != nil {
		return "", fmt.Errorf("unable to read the transaction ID: %w", err)
	}

	return id, nil
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
			`RESET lock_timeout;
			RESET statement_timeout;
			RESET transaction_timeout`,
		)
		err = errors.Join(err, resetErr)
	}()

	if _, err := r.Conn.ExecContext(
		ctx,
		`SELECT
			set_config('lock_timeout', '0', false),
			set_config('statement_timeout', '0', false),
			set_config('transaction_timeout', '0', false)`,
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
