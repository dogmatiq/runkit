package concurrency_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/concurrency"
	"github.com/dogmatiq/reference-engine/internal/database"
)

var handlerKey = uuidpb.Generate()

func TestAcquire(t *testing.T) {
	db, _ := database.NewTestDB(t)

	t.Run("it returns true when the lock is not held", func(t *testing.T) {
		database.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				acquired, err := concurrency.Acquire(ctx, tx, handlerKey)
				if err != nil {
					return err
				}
				if !acquired {
					t.Fatal("expected lock to be acquired")
				}
				return nil
			},
		)
	})

	t.Run("it returns true when reacquiring after the prior transaction committed", func(t *testing.T) {
		database.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				acquired, err := concurrency.Acquire(ctx, tx, handlerKey)
				if err != nil {
					return err
				}
				if !acquired {
					t.Fatal("expected lock to be acquired")
				}
				return nil
			},
		)
	})

	t.Run("it returns false when another transaction holds the lock", func(t *testing.T) {
		// Begin a transaction that holds the lock for the duration of the test.
		holding, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer holding.Rollback()

		acquired, err := concurrency.Acquire(t.Context(), holding, handlerKey)
		if err != nil {
			t.Fatal(err)
		}
		if !acquired {
			t.Fatal("expected holding transaction to acquire lock")
		}

		// A second transaction should fail to acquire the same lock.
		database.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				acquired, err := concurrency.Acquire(ctx, tx, handlerKey)
				if err != nil {
					return err
				}
				if acquired {
					t.Fatal("expected lock to NOT be acquired")
				}
				return nil
			},
		)
	})

	t.Run("it allows independent handlers to acquire locks concurrently", func(t *testing.T) {
		otherKey := uuidpb.Generate()

		holding, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer holding.Rollback()

		acquired, err := concurrency.Acquire(t.Context(), holding, handlerKey)
		if err != nil {
			t.Fatal(err)
		}
		if !acquired {
			t.Fatal("expected holding transaction to acquire lock")
		}

		// A different handler key should still be acquirable.
		database.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				acquired, err := concurrency.Acquire(ctx, tx, otherKey)
				if err != nil {
					return err
				}
				if !acquired {
					t.Fatal("expected lock for other handler to be acquired")
				}
				return nil
			},
		)
	})
}
