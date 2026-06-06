package integration

import (
	"database/sql"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database/databasetest"
)

func TestAcquireLock(t *testing.T) {
	db, _ := databasetest.New(t)

	t.Run("it returns true when the lock is not held", func(t *testing.T) {
		handlerKey := uuidpb.Generate()

		if err := ensureLockRowExists(t.Context(), db, handlerKey); err != nil {
			t.Fatal(err)
		}

		databasetest.Transact(t, db, func(tx *sql.Tx) {
			acquired, err := acquireLock(t.Context(), tx, handlerKey)
			if err != nil {
				t.Fatal(err)
			}
			if !acquired {
				t.Fatal("expected lock to be acquired")
			}
		})
	})

	t.Run("it returns true when reacquiring after the prior transaction committed", func(t *testing.T) {
		handlerKey := uuidpb.Generate()

		if err := ensureLockRowExists(t.Context(), db, handlerKey); err != nil {
			t.Fatal(err)
		}

		databasetest.Transact(t, db, func(tx *sql.Tx) {
			acquired, err := acquireLock(t.Context(), tx, handlerKey)
			if err != nil {
				t.Fatal(err)
			}
			if !acquired {
				t.Fatal("expected lock to be acquired")
			}
		})

		databasetest.Transact(t, db, func(tx *sql.Tx) {
			acquired, err := acquireLock(t.Context(), tx, handlerKey)
			if err != nil {
				t.Fatal(err)
			}
			if !acquired {
				t.Fatal("expected lock to be acquired")
			}
		})
	})

	t.Run("it returns false when another transaction holds the lock", func(t *testing.T) {
		handlerKey := uuidpb.Generate()

		if err := ensureLockRowExists(t.Context(), db, handlerKey); err != nil {
			t.Fatal(err)
		}

		databasetest.Transact(t, db, func(tx *sql.Tx) {
			acquired, err := acquireLock(t.Context(), tx, handlerKey)
			if err != nil {
				t.Fatal(err)
			}
			if !acquired {
				t.Fatal("expected lock to be acquired")
			}

			databasetest.Transact(t, db, func(tx *sql.Tx) {
				acquired, err := acquireLock(t.Context(), tx, handlerKey)
				if err != nil {
					t.Fatal(err)
				}
				if acquired {
					t.Fatal("did not expect lock to be acquired while it is held by another transaction")
				}
			})
		})

	})

	t.Run("it allows different handlers to acquire locks concurrently", func(t *testing.T) {
		handlerKey1 := uuidpb.Generate()
		handlerKey2 := uuidpb.Generate()

		if err := ensureLockRowExists(t.Context(), db, handlerKey1); err != nil {
			t.Fatal(err)
		}

		if err := ensureLockRowExists(t.Context(), db, handlerKey2); err != nil {
			t.Fatal(err)
		}

		databasetest.Transact(t, db, func(tx *sql.Tx) {
			acquired, err := acquireLock(t.Context(), tx, handlerKey1)
			if err != nil {
				t.Fatal(err)
			}
			if !acquired {
				t.Fatal("expected lock for handler 1 to be acquired")
			}

			databasetest.Transact(t, db, func(tx *sql.Tx) {
				acquired, err := acquireLock(t.Context(), tx, handlerKey2)
				if err != nil {
					t.Fatal(err)
				}
				if !acquired {
					t.Fatal("expected lock for handler 2 to be acquired")
				}
			})
		})
	})
}
