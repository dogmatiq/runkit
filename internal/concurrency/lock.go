package concurrency

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
)

// Acquire attempts to acquire the handler lock within the provided transaction.
//
// It returns true if the lock was acquired, or false if another transaction
// already holds it.
func Acquire(
	ctx context.Context,
	tx *sql.Tx,
	handlerKey *uuidpb.UUID,
) (bool, error) {
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO handler_locks (handler_key)
		VALUES ($1)
		ON CONFLICT (handler_key) DO NOTHING`,
		database.MarshalUUID(handlerKey),
	); err != nil {
		return false, fmt.Errorf("unable to ensure lock row for handler %s: %w", handlerKey, err)
	}

	row := tx.QueryRowContext(
		ctx,
		`SELECT 1
		FROM handler_locks
		WHERE handler_key = $1
		FOR UPDATE SKIP LOCKED`,
		database.MarshalUUID(handlerKey),
	)

	var one int
	if err := row.Scan(&one); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("unable to acquire lock for handler %s: %w", handlerKey, err)
	}

	return true, nil
}
