package process

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// InitializeHandler initializes the state of a process message handler.
//
// It is called by the engine on startup, before any components are started.
func InitializeHandler(
	ctx context.Context,
	db *sql.DB,
	handlerConfig *config.Process,
) error {
	return xsql.Transact(
		ctx,
		db,
		func(ctx context.Context, tx *sql.Tx) error {
			handlerKey := handlerConfig.Identity().GetKey()

			var hasCheckpoints bool
			if err := tx.QueryRowContext(
				ctx,
				`INSERT INTO process.handlers (handler_key)
				VALUES ($1)
				ON CONFLICT (handler_key) DO UPDATE SET
					handler_key = EXCLUDED.handler_key
				RETURNING has_checkpoint_offsets`,
				xsql.UUID(handlerKey),
			).Scan(&hasCheckpoints); err != nil {
				return fmt.Errorf("unable to insert handler: %w", err)
			}

			if hasCheckpoints {
				return nil
			}

			if _, err := tx.ExecContext(
				ctx,
				`INSERT INTO eventstream.handler_checkpoints (
					handler_key,
					stream_id,
					checkpoint_offset
				)
				SELECT $1, s.id, s.next_offset
				FROM eventstream.streams AS s
				ON CONFLICT DO NOTHING`,
				xsql.UUID(handlerKey),
			); err != nil {
				return fmt.Errorf("unable to initialize checkpoint offsets: %w", err)
			}

			if _, err := tx.ExecContext(
				ctx,
				`UPDATE process.handlers
				SET has_checkpoint_offsets = true
				WHERE handler_key = $1`,
				xsql.UUID(handlerKey),
			); err != nil {
				return fmt.Errorf("unable to update handler checkpoint flag: %w", err)
			}

			return nil
		},
	)
}
