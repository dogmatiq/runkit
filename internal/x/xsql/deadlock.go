package xsql

import (
	"errors"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

// PanicOnDeadlock panics if err is a PostgreSQL deadlock_detected error
// (SQLSTATE 40P01) and the program is running under go test.
//
// Deadlocks indicate a lock-ordering bug in the engine and should never occur.
//
// In production the function is a no-op because the transaction is already
// aborted and the retry path recovers gracefully - crashing the process would
// cause more harm than the wasted round-trip.
func PanicOnDeadlock(err error) {
	if testing.Testing() {
		if err, ok := errors.AsType[*pgconn.PgError](err); ok {
			if err.Code == pgerrcode.DeadlockDetected {
				panic(err)
			}
		}
	}
}
