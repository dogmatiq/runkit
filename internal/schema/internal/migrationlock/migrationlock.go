// Package migrationlock provides the advisory lock that only one call that
// changes the engine's schema holds at a time.
package migrationlock

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/dogmatiq/runkit/internal/x/xsql"
)

const (
	// key is the key of the advisory lock.
	//
	// Its high bit is clear because PostgreSQL's key is a signed 64-bit
	// integer.
	key uint64 = 0x0_DA7ABA5E_BA221E2

	// pollInterval is how long [Acquire] waits before it tries again to take
	// the lock, when another connection holds it.
	pollInterval = 100 * time.Millisecond
)

// Acquire takes the lock on the given connection, and returns a function that
// releases it.
//
// If another connection holds the lock, Acquire waits until it can take the
// lock or ctx is canceled. While it waits, it has no statement open on the
// connection, so a concurrent index build on the connection that holds the
// lock doesn't have to wait for it.
//
// If it can't tell whether it took the lock, or the release function can't
// release it, it closes the connection, which releases the lock.
func Acquire(
	ctx context.Context,
	conn *sql.Conn,
) (release func(), err error) {
	// Each attempt is a statement that returns at once. An open statement is
	// an open transaction, and a concurrent index build waits for every older
	// transaction to end. The connection that holds the lock may be building
	// an index that way, and it must not have to wait for this one.
	for {
		var locked bool

		if err := conn.QueryRowContext(
			ctx,
			`SELECT pg_try_advisory_lock($1)`,
			key,
		).Scan(&locked); err != nil {
			// The lock might have been taken even though the call failed, such
			// as when the context is canceled.
			xsql.Discard(conn)
			return nil, fmt.Errorf("unable to lock the database for migration: %w", err)
		}

		if locked {
			break
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("unable to lock the database for migration: %w", ctx.Err())
		case <-time.After(pollInterval):
		}
	}

	return func() {
		if _, err := conn.ExecContext(
			context.WithoutCancel(ctx),
			`SELECT pg_advisory_unlock($1)`,
			key,
		); err != nil {
			xsql.Discard(conn)
		}
	}, nil
}
