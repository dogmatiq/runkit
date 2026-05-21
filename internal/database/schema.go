package database

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
)

//go:embed schema.sql
var schemaSQL string

// ApplySchema creates the engine's tables and indexes. It is safe to call
// on every startup.
func ApplySchema(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("unable to apply schema: %w", err)
	}
	return nil
}
