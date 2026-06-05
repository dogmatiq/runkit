package database

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
)

//go:embed schema.sql
var schemaSQL string

// ApplySchema applies the PostgreSQL schema to the given database.
func ApplySchema(ctx context.Context, db *sql.DB) error {
	return Transact(
		ctx,
		db,
		func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, schemaSQL); err != nil {
				return fmt.Errorf("unable to apply schema: %w", err)
			}
			return nil
		},
	)
}
