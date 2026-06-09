package xsql

import (
	"context"
	"database/sql"
	"fmt"
)

// Transact executes the given function within a transaction.
//
// If fn returns an error the transaction is rolled back, otherwise it is
// committed.
func Transact(
	ctx context.Context,
	db *sql.DB,
	fn func(context.Context, *sql.Tx) error,
) (err error) {
	defer func() {
		// If the context was canceled, return that error instead of any other
		// error from fn or database operations.
		//
		// This ensures that if the transaction is rolled back due to context
		// cancellation, the caller receives the context cancellation error
		// instead of an error from fn that may be a consequence of the
		// rollback, which in practice can include [sql.ErrTxDone],
		// [driver.ErrBadConn] as well as internal PostgreSQL errors.
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := fn(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}
