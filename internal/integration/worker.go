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
	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
)

// worker handles commands for an integration handler.
type worker struct {
	Config                *config.Integration
	DB                    *sql.DB
	Packer                *envelopepb.Packer
	Logger                *slog.Logger
	ConcurrencyPreference dogma.ConcurrencyPreference
}

// Run runs the worker until ctx is canceled.
func (w *worker) Run(ctx context.Context) error {
	for {
		if err := w.tick(ctx); err != nil {
			if errors.Is(err, ctx.Err()) {
				return ctx.Err()
			}

			w.Logger.Error(
				"worker produced an error",
				slog.String("error", err.Error()),
			)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
			continue
		}
	}
}

func (w *worker) tick(ctx context.Context) error {
	tx, err := w.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if w.ConcurrencyPreference == dogma.MinimizeConcurrency {
		ok, err := acquireLock(ctx, tx, w.Config.Identity().GetKey())
		if !ok || err != nil {
			return err
		}
	}

	commandEnvelope, ok, err := w.acquireCommand(ctx, tx)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	if err := w.handleCommand(ctx, tx, commandEnvelope); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}

func (w *worker) acquireCommand(
	ctx context.Context,
	tx *sql.Tx,
) (*envelopepb.Envelope, bool, error) {
	var messageTypeIDs []string
	for route := range w.Config.
		RouteSet().
		Filter(config.FilterByRouteType(config.HandlesCommandRouteType)).
		Routes() {
		messageTypeIDs = append(messageTypeIDs, route.MessageTypeID.Get())
	}

	row := tx.QueryRowContext(
		ctx,
		`SELECT c.envelope
		FROM pending_commands AS c
		WHERE c.message_type_id = ANY($1::uuid[])
			AND c.next_attempt_at <= now()
		ORDER BY c.next_attempt_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED`,
		messageTypeIDs,
	)

	env := &envelopepb.Envelope{}
	if err := row.Scan(database.UnmarshalEnvelope(env)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("unable to query for pending command: %w", err)
	}

	return env, true, nil
}

func (w *worker) handleCommand(
	ctx context.Context,
	tx *sql.Tx,
	commandEnvelope *envelopepb.Envelope,
) error {
	commandMessageID := commandEnvelope.GetBody().GetMessageId()
	command, ok, err := w.unpackCommand(ctx, tx, commandEnvelope)
	if !ok || err != nil {
		return err
	}

	packer := w.Packer.PackEffects(
		commandEnvelope,
		w.Config.Identity(),
	)

	err = w.Config.Interface().HandleCommand(
		ctx,
		&scope{
			Packer: packer,
			Logger: w.Logger,
		},
		command,
	)
	if err != nil {
		w.Logger.Error(
			"handler returned an error",
			slog.String("message_id", commandMessageID.AsString()),
			slog.String("error", err.Error()),
		)
		return commandqueue.Nack(ctx, tx, commandMessageID)
	}

	if envelopes, ok := packer.Seal(); ok {
		eventStreamID, err := eventstream.Acquire(ctx, tx)
		if err != nil {
			return err
		}

		if _, err := eventstream.Append(
			ctx,
			tx,
			eventStreamID,
			envelopes,
		); err != nil {
			return err
		}
	}

	return commandqueue.Ack(ctx, tx, commandMessageID)
}

// unpackCommand attempts to unpack the given envelope into a command.
//
// If the envelope cannot be unpacked then the command is Nack'd and ok is false.
func (w *worker) unpackCommand(
	ctx context.Context,
	tx *sql.Tx,
	commandEnvelope *envelopepb.Envelope,
) (dogma.Command, bool, error) {
	command, err := envelopepb.Unpack[dogma.Command](commandEnvelope)
	if err != nil {
		commandMessageID := commandEnvelope.GetBody().GetMessageId()

		w.Logger.Error(
			"unable to unpack command",
			slog.String("message_id", commandMessageID.AsString()),
			slog.String("error", err.Error()),
		)

		return nil, false, commandqueue.Nack(ctx, tx, commandMessageID)
	}

	return command, true, nil
}
