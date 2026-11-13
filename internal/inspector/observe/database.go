// Package observe reads what an engine cluster is doing from its database,
// without changing anything.
//
// Every query runs in a read-only transaction, so it needs only the SELECT
// privilege on the engine's tables. The only other statement is LISTEN, which
// needs no privilege. Nothing in this package sends a notification, takes a
// lock or calls a function in the engine's schemas.
package observe

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Database reads the engine's tables.
type Database struct {
	// DB is the engine's database.
	DB *sql.DB

	// StatementTimeout is the longest time that one statement may run.
	StatementTimeout time.Duration
}

// read calls fn with a read-only transaction. PostgreSQL rejects any change
// to a table in such a transaction.
func (d *Database) read(ctx context.Context, fn func(tx *sql.Tx) error) error {
	if d.StatementTimeout <= 0 {
		panic("statement timeout must be positive")
	}

	tx, err := d.DB.BeginTx(
		ctx,
		&sql.TxOptions{
			ReadOnly: true,
		},
	)
	if err != nil {
		return fmt.Errorf("unable to begin read-only transaction: %w", err)
	}

	// Nothing is changed, so rolling back is the same as committing.
	defer tx.Rollback()

	timeout := fmt.Sprintf("%dms", d.StatementTimeout.Milliseconds())

	if _, err := tx.ExecContext(
		ctx,
		`SELECT set_config('statement_timeout', $1, true)`,
		timeout,
	); err != nil {
		return fmt.Errorf("unable to set statement timeout: %w", err)
	}

	return fn(tx)
}
