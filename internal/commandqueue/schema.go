package commandqueue

import (
	"context"
	"database/sql"
	_ "embed"
)

//go:embed schema.sql
var schemaSQL string

// ApplySchema creates the commandqueue package's tables and indexes. It is
// safe to call on every startup.
func ApplySchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, schemaSQL)
	return err
}
