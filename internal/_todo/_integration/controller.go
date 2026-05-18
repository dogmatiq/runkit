package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
	"github.com/dogmatiq/reference-engine/internal/routes"
	"google.golang.org/protobuf/proto"
)

// pollInterval is the sleep between empty polling cycles.
const pollInterval = 100 * time.Millisecond

// Controller processes commands routed to one integration handler.
//
// Unlike aggregates, integrations perform external side-effects and have no
// instance state to replay. The handler is responsible for ensuring that its
// external side-effects are idempotent.
type Controller struct {
	Config *config.Integration
	DB     *sql.DB
	Packer *envelopepb.Packer
	Logger *slog.Logger
}

// Run drives the controller until ctx is cancelled. The loop shape is selected
// from the handler's [dogma.ConcurrencyPreference].
func (c *Controller) Run(ctx context.Context) error {
	if c.Config.ConcurrencyPreference() == dogma.MinimizeConcurrency {
		return c.runSingleton(ctx)
	}
	return c.runMaxParallel(ctx)
}

// claimedCommand is one row claimed via FOR UPDATE SKIP LOCKED, ready to
// hand to the handler.
type claimedCommand struct {
	id           string
	attemptCount int
	envBytes     []byte
}

// claimCommand SELECTs the next due command for this handler's subscribed
// types under FOR UPDATE SKIP LOCKED. Returns ok=false on sql.ErrNoRows.
func (c *Controller) claimCommand(ctx context.Context, tx *sql.Tx) (claimedCommand, bool, error) {
	var cmd claimedCommand
	err := tx.QueryRowContext(
		ctx,
		`SELECT message_id::text, attempt_count, envelope FROM commandqueue.commands
		 WHERE message_type_id = ANY($1::uuid[]) AND next_attempt_at <= now()
		 ORDER BY next_attempt_at
		 LIMIT 1
		 FOR UPDATE SKIP LOCKED`,
		routes.MessageTypes(c.Config, config.HandlesCommandRouteType),
	).Scan(&cmd.id, &cmd.attemptCount, &cmd.envBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return claimedCommand{}, false, nil
	}
	if err != nil {
		return claimedCommand{}, false, err
	}
	return cmd, true, nil
}

// handleClaimed runs the handler against cmd. On success, it persists any
// recorded events and DELETEs the commandqueue.commands row in tx,
// then commits.
// On failure (unpack error or handler error), tx is rolled back here and a
// fresh transaction is opened to call MarkCommandFailed.
func (c *Controller) handleClaimed(ctx context.Context, tx *sql.Tx, cmd claimedCommand) error {
	id := c.Config.Identity()
	handler := c.Config.Interface()

	cmdEnv := &envelopepb.Envelope{}
	if err := proto.Unmarshal(cmd.envBytes, cmdEnv); err != nil {
		_ = tx.Rollback()
		return c.markFailed(ctx, cmd.id, err)
	}

	rawMsg, err := envelopepb.Unpack(cmdEnv)
	if err != nil {
		_ = tx.Rollback()
		return c.markFailed(ctx, cmd.id, err)
	}
	msg, ok := rawMsg.(dogma.Command)
	if !ok {
		_ = tx.Rollback()
		return c.markFailed(
			ctx, cmd.id,
			fmt.Errorf("expected command, got %T", rawMsg),
		)
	}

	packer := c.Packer.PackEffects(cmdEnv, id)
	scope := &commandScope{
		packer: packer,
		logger: c.Logger,
	}

	if handlerErr := handler.HandleCommand(ctx, scope, msg); handlerErr != nil {
		_ = tx.Rollback()
		return c.markFailed(ctx, cmd.id, handlerErr)
	}

	envelopes, hasEffects := packer.Seal()
	if hasEffects {
		if err := eventstream.Append(ctx, tx, envelopes); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM commandqueue.commands WHERE message_id = $1`,
		cmd.id,
	); err != nil {
		return err
	}

	return tx.Commit()
}

// markFailed records err on the command row in a fresh transaction and
// schedules a retry via [commandqueue.Nack]. Used after the handling tx
// has already been rolled back.
func (c *Controller) markFailed(
	ctx context.Context,
	cmdID string,
	handlerErr error,
) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := commandqueue.Nack(ctx, tx, cmdID, handlerErr); err != nil {
		return err
	}
	return tx.Commit()
}
