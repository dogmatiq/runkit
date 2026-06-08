package xsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

// Querier is the common interface for running queriesshared by both [sql.DB]
// and [sql.Tx].
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Executor is the common interface for running queries and executing statements,
// it is implemented by both [sql.DB] and [sql.Tx].
type Executor interface {
	Querier
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Value is an interface for values that can be stored in the database.
type Value interface {
	driver.Value
	sql.Scanner
}
