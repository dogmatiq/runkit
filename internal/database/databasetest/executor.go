package databasetest

import (
	"context"
	"database/sql"
)

// Executor is the common interface for running queries and executing
// statements, it is implemented by both [sql.DB] and [sql.Tx].
type Executor interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}
