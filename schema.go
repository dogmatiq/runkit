package runkit

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/runkit/internal/schema"
)

// CreateSchema creates the database schema used by the engine.
func CreateSchema(ctx context.Context, db *sql.DB) error {
	return schema.Create(ctx, db)
}

// DropSchema drops the database schema used by the engine.
func DropSchema(ctx context.Context, db *sql.DB) error {
	return schema.Drop(ctx, db)
}
