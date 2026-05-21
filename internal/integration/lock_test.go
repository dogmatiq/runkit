package integration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
)

func TestAcquireLock(t *testing.T) {
	db, _ := database.NewTestDB(t)
	handlerKey := uuidpb.Generate()

	if err := ensureLockRowExists(t.Context(), db, handlerKey); err != nil {
		t.Fatal(err)
	}

	t.Run("it returns true when the lock is not held", func(t *testing.T) {
		database.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				acquired, err := acquireLock(ctx, tx, handlerKey)
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
				acquired, err := acquireLock(ctx, tx, handlerKey)
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
		holding, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer holding.Rollback()

		acquired, err := acquireLock(t.Context(), holding, handlerKey)
		if err != nil {
			t.Fatal(err)
		}
		if !acquired {
			t.Fatal("expected holding transaction to acquire lock")
		}

		database.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				acquired, err := acquireLock(ctx, tx, handlerKey)
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

	t.Run("it allows different handlers to acquire locks concurrently", func(t *testing.T) {
		otherKey := uuidpb.Generate()

		if err := ensureLockRowExists(t.Context(), db, otherKey); err != nil {
			t.Fatal(err)
		}

		holding, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer holding.Rollback()

		acquired, err := acquireLock(t.Context(), holding, handlerKey)
		if err != nil {
			t.Fatal(err)
		}
		if !acquired {
			t.Fatal("expected holding transaction to acquire lock")
		}

		database.Transact(
			t,
			db,
			func(ctx context.Context, tx *sql.Tx) error {
				acquired, err := acquireLock(ctx, tx, otherKey)
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
