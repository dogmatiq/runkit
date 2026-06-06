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
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
)

// Controller manages a single integration handler.
type Controller struct {
	// Config is the integration message handler's configuration.
	Config *config.Integration

	// DB is the database connection that the controller and its workers use.
	DB *sql.DB

	// Packer is used for packing the events that the handler records into
	// envelopes.
	Packer *envelopepb.Packer

	// Logger is the target for log messages from both the engine and the
	// application.
	Logger *slog.Logger

	// PollInterval is the frequency at which the controller polls for new work.
	PollInterval time.Duration

	messageTypeIDs []string
}

// Run runs the controller until ctx is canceled or an unrecoverable error
// occurs.
func (c *Controller) Run(ctx context.Context) error {
	if c.Config.ConcurrencyPreference() == dogma.MinimizeConcurrency {
		if err := ensureLockRowExists(ctx, c.DB, c.Config.Identity().GetKey()); err != nil {
			return err
		}
	}

	commandRoutes := c.Config.
		RouteSet().
		Filter(
			config.FilterByMessageDirection(config.InboundDirection),
			config.FilterByMessageKind(message.CommandKind),
		).
		Routes()

	for route := range commandRoutes {
		c.messageTypeIDs = append(c.messageTypeIDs, route.MessageTypeID.Get())
	}

	poll := time.NewTicker(max(1, c.PollInterval))
	defer poll.Stop()

	for {
		ok, err := c.poll(ctx)
		if err != nil {
			if errors.Is(err, ctx.Err()) {
				return ctx.Err()
			}

			c.Logger.Error(
				"integration controller tick failed",
				slog.String("error", err.Error()),
			)
		}

		if !ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-poll.C:
				continue
			}
		}
	}
}

func (c *Controller) poll(ctx context.Context) (ok bool, err error) {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("unable to begin transaction: %w", err)
	}
	defer func() {
		if !ok {
			tx.Rollback()
		}
	}()

	if c.Config.ConcurrencyPreference() == dogma.MinimizeConcurrency {
		acquired, err := acquireLock(ctx, tx, c.Config.Identity().GetKey())
		if !acquired || err != nil {
			return false, err
		}
	}

	row := tx.QueryRowContext(
		ctx,
		`SELECT
			c.envelope
		FROM pending_commands AS c
		WHERE c.message_type_id = ANY($1::uuid[])
			AND c.next_attempt_at <= clock_timestamp()
		ORDER BY c.next_attempt_at
		LIMIT 1
		FOR UPDATE
		SKIP LOCKED`,
		c.messageTypeIDs,
	)

	commandEnvelope := &envelopepb.Envelope{}
	if err := row.Scan(database.UnmarshalEnvelope(commandEnvelope)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("unable to query pending command: %w", err)
	}

	if c.Config.ConcurrencyPreference() == dogma.MinimizeConcurrency {
		c.handleCommandTransaction(ctx, tx, commandEnvelope)
	} else {
		go c.handleCommandTransaction(ctx, tx, commandEnvelope)
	}

	return true, nil
}

func (c *Controller) handleCommandTransaction(
	ctx context.Context,
	tx *sql.Tx,
	commandEnvelope *envelopepb.Envelope,
) {
	defer tx.Rollback()

	commandMessageID := commandEnvelope.GetBody().GetMessageId()

	logger := c.Logger.With(
		slog.String("message_id", commandMessageID.AsString()),
		slog.String("message_description", commandEnvelope.GetBody().GetMessage().String()),
		slog.String("message_type_id", commandEnvelope.GetBody().GetMessage().GetTypeId().AsString()),
	)

	if err := c.handleCommand(ctx, tx, logger, commandEnvelope); err != nil {
		if errors.Is(err, ctx.Err()) {
			return
		}

		c.Logger.Error(
			"unable to handle command",
			slog.String("error", err.Error()),
		)
	}

	if err := tx.Commit(); err != nil {
		c.Logger.Error(
			"unable to commit transaction",
			slog.String("error", err.Error()),
		)
	}
}

func (c *Controller) handleCommand(
	ctx context.Context,
	tx *sql.Tx,
	logger *slog.Logger,
	commandEnvelope *envelopepb.Envelope,
) error {
	commandMessageID := commandEnvelope.GetBody().GetMessageId()

	command, err := envelopepb.Unpack[dogma.Command](commandEnvelope)
	if err != nil {
		logger.Error(
			"command cannot be unpacked",
			slog.String("error", err.Error()),
		)

		return commandqueue.Backoff(ctx, tx, commandMessageID)
	}

	if err := command.Validate(
		dogma.CommandValidationScope(nil), // TODO
	); err != nil {
		logger.Error(
			"command is invalid",
			slog.String("error", err.Error()),
		)

		return commandqueue.Dequeue(ctx, tx, commandMessageID)
	}

	eventPacker := c.Packer.PackEffects(
		commandEnvelope,
		c.Config.Identity(),
	)

	if err := c.Config.Interface().HandleCommand(
		ctx,
		&scope{
			Packer: eventPacker,
			Logger: c.Logger,
		},
		command,
	); err != nil {
		logger.Error(
			"handler produced an error",
			slog.String("error", err.Error()),
		)

		return commandqueue.Backoff(ctx, tx, commandMessageID)
	}

	if eventEnvelopes, ok := eventPacker.Seal(); ok {
		eventStreamID, err := eventstream.Acquire(ctx, tx)
		if err != nil {
			return err
		}

		if _, err := eventstream.Append(
			ctx,
			tx,
			eventStreamID,
			eventEnvelopes,
		); err != nil {
			return err
		}
	}

	return commandqueue.Dequeue(ctx, tx, commandMessageID)
}
