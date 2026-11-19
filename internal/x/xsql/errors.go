package xsql

import (
	"context"
	"errors"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

// IsLockNotAvailableError returns true if err is a PostgreSQL
// lock_not_available error (SQLSTATE 55P03).
//
// PostgreSQL returns it when a statement with NOWAIT can't lock a row straight
// away, or when waiting for a lock takes longer than lock_timeout.
//
// It returns false if ctx is done, so that a caller that is stopping doesn't
// treat the error as contention.
func IsLockNotAvailableError(ctx context.Context, err error) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	return ok && ctx.Err() == nil && pgErr.Code == pgerrcode.LockNotAvailable
}

// IsStatementCanceledError returns true if err is PostgreSQL canceling a
// statement (SQLSTATE 57014, query_canceled).
//
// PostgreSQL returns it when a statement runs for longer than the statement
// timeout, and also when someone cancels the statement, such as with
// pg_cancel_backend(). The error doesn't say which.
//
// It returns false if ctx is done, because then the client canceled the
// statement.
func IsStatementCanceledError(ctx context.Context, err error) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	return ok && ctx.Err() == nil && pgErr.Code == pgerrcode.QueryCanceled
}
