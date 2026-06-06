package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
)

// ensureLockRowExists ensures the lock row exists for the given handler.
func ensureLockRowExists(ctx context.Context, db *sql.DB, handlerKey *uuidpb.UUID) error {
	_, err := db.ExecContext(
		ctx,
		`INSERT INTO integration_locks (
			handler_key
		) VALUES ($1)
		ON CONFLICT (handler_key) DO NOTHING`,
		database.MarshalUUID(handlerKey),
	)
	if err != nil {
		return fmt.Errorf("unable to seed lock row for handler %s: %w", handlerKey, err)
	}
	return nil
}

// acquireLock attempts to acquire the integration handler lock within the
// provided transaction.
//
// It returns true if the lock was acquired, or false if another transaction
// already holds it.
func acquireLock(
	ctx context.Context,
	tx *sql.Tx,
	handlerKey *uuidpb.UUID,
) (bool, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT 1
		FROM integration_locks
		WHERE handler_key = $1
		FOR UPDATE
		SKIP LOCKED`,
		database.MarshalUUID(handlerKey),
	)

	var ignored int
	if err := row.Scan(&ignored); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("unable to acquire lock for handler %s: %w", handlerKey, err)
	}

	return true, nil
}
