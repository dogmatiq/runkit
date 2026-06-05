package database

import (
	"context"
	"database/sql"
	"fmt"
)

// Transact executes the given function within a transaction.
func Transact(
	ctx context.Context,
	db *sql.DB,
	fn func(context.Context, *sql.Tx) error,
) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := fn(ctx, tx); err != nil {
		return fmt.Errorf("transaction function produced an error: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}
