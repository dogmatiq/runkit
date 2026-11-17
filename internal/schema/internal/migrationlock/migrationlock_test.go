package migrationlock_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	. "github.com/dogmatiq/runkit/internal/schema/internal/migrationlock"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

func TestAcquire(t *testing.T) {
	t.Run("it waits until the connection that holds the lock releases it", func(t *testing.T) {
		db := xtesting.NewDatabaseWithoutSchema(t)
		holder := newConn(t, db)
		waiter := newConn(t, db)

		release := acquire(t, holder)

		acquired := make(chan error, 1)
		go func() {
			release, err := Acquire(t.Context(), waiter)
			if err == nil {
				release()
			}
			acquired <- err
		}()

		select {
		case err := <-acquired:
			t.Fatalf("took the lock while another connection held it: %v", err)
		case <-time.After(500 * time.Millisecond):
		}

		release()

		select {
		case err := <-acquired:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(xtesting.WaitTimeout):
			t.Fatal("timed out waiting for the lock")
		}
	})

	t.Run("it waits without deadlocking a concurrent index build on the connection that holds the lock", func(t *testing.T) {
		db := xtesting.NewDatabaseWithoutSchema(t)
		holder := newConn(t, db)
		waiter := newConn(t, db)

		if _, err := db.ExecContext(
			t.Context(),
			`CREATE TABLE public.rows (value int);
			INSERT INTO public.rows SELECT generate_series(1, 1000)`,
		); err != nil {
			t.Fatal(err)
		}

		release := acquire(t, holder)

		var waiterPID int
		if err := waiter.QueryRowContext(
			t.Context(),
			`SELECT pg_backend_pid()`,
		).Scan(&waiterPID); err != nil {
			t.Fatal(err)
		}

		acquired := make(chan error, 1)
		go func() {
			release, err := Acquire(t.Context(), waiter)
			if err == nil {
				release()
			}
			acquired <- err
		}()

		xtesting.WaitForQueryResult(
			t,
			"the other connection has tried to take the lock",
			true,
			db,
			`SELECT query LIKE '%advisory_lock%'
			FROM pg_stat_activity
			WHERE pid = $1`,
			waiterPID,
		)

		// If the two connections waited for each other, PostgreSQL would
		// cancel one of them once it had waited for deadlock_timeout, which is
		// one second by default. Either the index build or the call to
		// Acquire would then fail.
		buildCtx, cancelBuild := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
		defer cancelBuild()

		if _, err := holder.ExecContext(
			buildCtx,
			`CREATE INDEX CONCURRENTLY rows_by_value ON public.rows (value)`,
		); err != nil {
			t.Fatalf("unable to build the index while another connection waits for the lock: %v", err)
		}

		xtesting.ExpectQueryResult(
			t,
			"the index is valid",
			true,
			db,
			`SELECT indisvalid
			FROM pg_index
			WHERE indexrelid = 'public.rows_by_value'::regclass`,
		)

		release()

		select {
		case err := <-acquired:
			if err != nil {
				t.Fatalf("unable to take the lock after the index build: %v", err)
			}
		case <-time.After(xtesting.WaitTimeout):
			t.Fatal("timed out waiting for the lock")
		}
	})

	t.Run("it returns an error if the context is canceled while it waits", func(t *testing.T) {
		db := xtesting.NewDatabaseWithoutSchema(t)
		holder := newConn(t, db)
		waiter := newConn(t, db)

		release := acquire(t, holder)
		defer release()

		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()

		_, err := Acquire(ctx, waiter)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("unexpected error: want %v, got %v", context.DeadlineExceeded, err)
		}
	})
}

// newConn returns a connection from the given pool, which is closed when the
// test ends.
func newConn(t *testing.T, db *sql.DB) *sql.Conn {
	t.Helper()

	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn
}

// acquire takes the lock on the given connection, and fails the test if it
// can't.
func acquire(t *testing.T, conn *sql.Conn) (release func()) {
	t.Helper()

	release, err := Acquire(t.Context(), conn)
	if err != nil {
		t.Fatal(err)
	}

	return release
}
