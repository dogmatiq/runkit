package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// runSingleton implements the singleton-mode loop. It seeds a row in
// singleton_handlers, then in each cycle takes FOR UPDATE SKIP LOCKED on
// that row before claiming and handling one command.
func (c *Controller) runSingleton(ctx context.Context) error {
	handlerKey := c.Config.Identity().GetKey().AsString()

	if _, err := c.DB.ExecContext(
		ctx,
		`INSERT INTO singleton_handlers (handler_key) VALUES ($1)
		 ON CONFLICT DO NOTHING`,
		handlerKey,
	); err != nil {
		return fmt.Errorf("seed singleton_handlers: %w", err)
	}

	for {
		dispatched, err := c.singletonCycle(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.Logger.Error(
				"integration dispatch error",
				slog.String("handler", c.Config.Identity().GetName()),
				slog.String("error", err.Error()),
			)
		}
		if !dispatched {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollInterval):
			}
		}
	}
}

// singletonCycle attempts one (acquire-singleton-lock, claim, handle) cycle.
// Returns dispatched=true if a command was handled (success or failure
// recorded); dispatched=false if either the singleton lock was unavailable
// or the queue was empty.
func (c *Controller) singletonCycle(ctx context.Context) (bool, error) {
	handlerKey := c.Config.Identity().GetKey().AsString()

	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	// Acquire the singleton lock; another replica wins → sleep and retry.
	var dummy int
	err = tx.QueryRowContext(
		ctx,
		`SELECT 1 FROM singleton_handlers
		 WHERE handler_key = $1
		 FOR UPDATE SKIP LOCKED`,
		handlerKey,
	).Scan(&dummy)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	cmd, ok, err := c.claimCommand(ctx, tx)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	if err := c.handleClaimed(ctx, tx, cmd); err != nil {
		return true, err
	}
	return true, nil
}
