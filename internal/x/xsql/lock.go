package xsql

import (
	"errors"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

// IsLockNotAvailableError returns true if err is a PostgreSQL
// lock_not_available error (SQLSTATE 55P03).
//
// PostgreSQL returns it when a statement with NOWAIT can't lock a row straight
// away, or when waiting for a lock takes longer than lock_timeout.
func IsLockNotAvailableError(err error) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	return ok && pgErr.Code == pgerrcode.LockNotAvailable
}
