package database

import (
	"context"
	"database/sql"
)

// Querier is the subset of *sql.DB that is used by the engine to query the
// database. It is implemented by both *sql.DB and *sql.Tx, so it can be used
// in both contexts.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}
