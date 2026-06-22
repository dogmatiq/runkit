package concurrency

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// EnforceConcurrencyPreference calls fn, optionally serializing execution
// using a PostgreSQL session-level advisory lock when the concurrency preference
// is [dogma.MinimizeConcurrency]. If the preference is any other value, fn is
// called directly without acquiring a lock.
//
// Session-level advisory locks are used (rather than transaction-level) so that
// the lock can be released before the transaction commits, minimizing the
// duration of serialization.
func EnforceConcurrencyPreference(
	ctx context.Context,
	tx *sql.Tx,
	key *uuidpb.UUID,
	pref dogma.ConcurrencyPreference,
	fn func() error,
) (err error) {
	if pref != dogma.MinimizeConcurrency {
		return fn()
	}

	lockKey := advisoryLockKey(key)

	if _, err := tx.ExecContext(
		ctx,
		`SELECT pg_advisory_lock($1)`,
		lockKey,
	); err != nil {
		return fmt.Errorf("unable to acquire advisory lock: %w", err)
	}

	defer func() {
		if _, unlockErr := tx.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, lockKey); unlockErr != nil {
			err = fmt.Errorf("unable to release advisory lock: %w", unlockErr)
		}
	}()

	return fn()
}

// advisoryLockKey derives a lock key by XOR-ing the two 64-bit halves of the
// UUID. This reduces a 128-bit UUID to a 64-bit lock key, which introduces a
// theoretical collision risk. In practice the number of distinct handlers in a
// single database is small enough that collisions are astronomically unlikely.
func advisoryLockKey(id *uuidpb.UUID) int64 {
	return int64(id.GetUpper() ^ id.GetLower())
}
