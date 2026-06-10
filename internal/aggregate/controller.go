package aggregate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
	"github.com/dogmatiq/reference-engine/internal/x/xmessage"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// Controller manages the state of aggregate instances for a single aggregate
// message handler type.
type Controller struct {
	DB              *sql.DB
	Handler         dogma.AggregateMessageHandler[dogma.AggregateRoot]
	HandlerIdentity *identitypb.Identity
	EnvelopePacker  *envelopepb.Packer
	CommandTypeIDs  []string
	Logger          *slog.Logger

	commandLogger *slog.Logger
}

var errNoPendingCommands = errors.New("no pending commands")

// Run handles messages for the controller's handler until ctx is canceled.
func (c *Controller) Run(ctx context.Context) (err error) {
	for {
		if err := xsql.Transact(
			ctx,
			c.DB,
			c.processNextCommand,
		); err != nil {
			if !errors.Is(err, errNoPendingCommands) {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(25 * time.Millisecond):
				continue
			}
		}
	}
}

// processNextCommand attempts to process the next pending command for the
// handler.
//
// If there are no pending commands, it returns [errNoPendingCommands].
func (c *Controller) processNextCommand(ctx context.Context, tx *sql.Tx) error {
	row := tx.QueryRowContext(
		ctx,
		`SELECT
			c.message_id,
			c.envelope
		FROM dogma.pending_commands AS c
		WHERE message_type_id = ANY($1)
		AND attempt_at <= clock_timestamp()
		ORDER BY is_deprioritized
		LIMIT 1
		FOR UPDATE SKIP LOCKED`,
		c.CommandTypeIDs,
	)

	var (
		commandMessageID     = &uuidpb.UUID{}
		commandEnvelopeBytes []byte
	)

	if err := row.Scan(
		xsql.UUID(commandMessageID),
		&commandEnvelopeBytes,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errNoPendingCommands
		}
		return fmt.Errorf("unable to scan pending command: %w", err)
	}

	var (
		commandEnvelope                       = &envelopepb.Envelope{}
		commandForRouting, commandForHandling dogma.Command
	)

	if err := xmessage.Unpack(
		commandEnvelopeBytes,
		commandEnvelope,
		&commandForRouting,
		&commandForHandling,
	); err != nil {
		c.Logger.ErrorContext(
			ctx,
			"cannot unmarshal command",
			slog.Group(
				"command",
				xslog.UUID("message_id", commandMessageID),
			),
			xslog.Error(err),
		)

		return commandqueue.Defer(ctx, tx, commandMessageID)
	}

	c.commandLogger = c.Logger.With(
		xslog.Envelope("command", commandEnvelope),
	)

	instanceID, ok := c.routeCommandToInstance(ctx, commandForRouting)
	if !ok {
		return commandqueue.Defer(ctx, tx, commandMessageID)
	}

	c.commandLogger = c.Logger.With(
		xslog.Envelope("command", commandEnvelope),
		slog.Group(
			"aggregate_instance",
			slog.String("id", instanceID),
		),
	)

	eventStreamID, ok, err := c.lockInstance(ctx, tx, instanceID)
	if err != nil {
		return err
	}
	if !ok {
		return commandqueue.Deprioritize(ctx, tx, commandMessageID)
	}

	c.commandLogger = c.Logger.With(
		xslog.Envelope("command", commandEnvelope),
		slog.Group(
			"aggregate_instance",
			slog.String("id", instanceID),
			xslog.UUID("event_stream_id", eventStreamID),
		),
	)

	aggregateRoot, ok, err := c.loadRoot(ctx, tx, eventStreamID, instanceID)
	if err != nil {
		return err
	}
	if !ok {
		return commandqueue.Defer(ctx, tx, commandMessageID)
	}

	c.commandLogger = c.Logger.With(
		xslog.Envelope("command", commandEnvelope),
		slog.Group(
			"aggregate_instance",
			slog.String("id", instanceID),
			slog.String("description", aggregateRoot.AggregateInstanceDescription()),
			xslog.UUID("event_stream_id", eventStreamID),
		),
	)

	envelopePacker := c.EnvelopePacker.PackEffects(
		commandEnvelope,
		c.HandlerIdentity,
		envelopepb.WithInstanceID(instanceID),
	)

	c.Handler.HandleCommand(
		aggregateRoot,
		&scope{
			instanceID:     instanceID,
			aggregateRoot:  aggregateRoot,
			envelopePacker: envelopePacker,
			logger:         c.commandLogger,
		},
		commandForHandling,
	)

	if eventEnvelopes, ok := envelopePacker.Seal(); ok {
		if _, err := eventstream.Append(
			ctx,
			tx,
			eventStreamID,
			eventEnvelopes,
		); err != nil {
			return err
		}
	}

	return commandqueue.Remove(
		ctx,
		tx,
		commandEnvelope.GetBody().GetMessageId(),
	)
}

func (c *Controller) routeCommandToInstance(
	ctx context.Context,
	commandForRouting dogma.Command,
) (string, bool) {
	instanceID := c.Handler.RouteCommandToInstance(commandForRouting)
	if instanceID != "" {
		return instanceID, true
	}

	c.commandLogger.ErrorContext(
		ctx,
		"handler routed command to an empty instance ID",
	)

	return "", false
}

func (c *Controller) lockInstance(
	ctx context.Context,
	tx *sql.Tx,
	instanceID string,
) (*uuidpb.UUID, bool, error) {
	// Check if the instance already exists, and attempt to lock it in a single
	// database round-trip.
	row := tx.QueryRowContext(
		ctx,
		`WITH instance AS (
			SELECT event_stream_id
			FROM dogma.aggregate_instances
			WHERE handler_key = $1
			AND instance_id = $2
		), lock AS (
			SELECT EXISTS (
				SELECT true
				FROM dogma.aggregate_instances
				WHERE handler_key = $1
				AND instance_id = $2
				FOR UPDATE SKIP LOCKED
			) AS acquired
		)
		SELECT
			instance.event_stream_id,
			lock.acquired
		FROM instance, lock`,
		xsql.UUID(c.HandlerIdentity.GetKey()),
		instanceID,
	)

	var (
		eventStreamID = &uuidpb.UUID{}
		lockAcquired  bool
	)

	err := row.Scan(
		xsql.UUID(eventStreamID),
		&lockAcquired,
	)

	if err == nil {
		if !lockAcquired {
			c.commandLogger.DebugContext(
				ctx,
				"instance is currently locked by another transaction",
			)
		}

		return eventStreamID, lockAcquired, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, fmt.Errorf("unable to query aggregate instance: %w", err)
	}

	// We didn't find any rows, so we need to insert a new instance.

	// First we acquire a new event stream ID, which we will attempt to bind
	// to the new instance.
	candidateEventStreamID, err := eventstream.Acquire(ctx, tx)
	if err != nil {
		return nil, false, err
	}

	// If another transaction is racing to create the same instance we may lose
	// the race and block until it completes, in which case we must honor the
	// event stream binding that the other transaction established, rather than
	// using the candidate we chose.
	row = tx.QueryRowContext(
		ctx,
		`INSERT INTO dogma.aggregate_instances (
			handler_key,
			instance_id,
			event_stream_id
		) VALUES ($1, $2, $3)
		ON CONFLICT (handler_key, instance_id) DO UPDATE SET
			instance_id = EXCLUDED.instance_id
		RETURNING event_stream_id`,
		xsql.UUID(c.HandlerIdentity.GetKey()),
		instanceID,
		xsql.UUID(candidateEventStreamID),
	)

	if err := row.Scan(
		xsql.UUID(eventStreamID),
	); err != nil {
		return nil, false, fmt.Errorf("unable to insert aggregate instance: %w", err)
	}

	// At this point we've successfully inserted the instance OR done a dummy
	// update of the instance_id to its existing value; either way we now hold
	// the lock and may proceed to handle the command.
	return eventStreamID, true, nil
}

// loadRoot loads the aggregate instance with the given ID, and returns the
// root and event stream ID for the instance.
func (c *Controller) loadRoot(
	ctx context.Context,
	tx *sql.Tx,
	eventStreamID *uuidpb.UUID,
	instanceID string,
) (dogma.AggregateRoot, bool, error) {
	aggregateRoot := c.Handler.New()

	// Load all historical events for the instance.
	rows, err := tx.QueryContext(
		ctx,
		`SELECT
			message_id,
			envelope
		FROM dogma.events
		WHERE event_stream_id = $1
		AND aggregate_handler_key = $2
		AND aggregate_instance_id = $3
		ORDER BY event_stream_offset`,
		xsql.UUID(eventStreamID),
		xsql.UUID(c.HandlerIdentity.GetKey()),
		instanceID,
	)
	if err != nil {
		return nil, false, fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	// Apply the historical events to the root in order.
	for rows.Next() {
		var (
			eventMessageID     = &uuidpb.UUID{}
			eventEnvelopeBytes []byte
		)

		if err := rows.Scan(
			xsql.UUID(eventMessageID),
			&eventEnvelopeBytes,
		); err != nil {
			return nil, false, fmt.Errorf("unable to scan event: %w", err)
		}

		var (
			eventEnvelope = &envelopepb.Envelope{}
			eventForApply dogma.Event
		)

		if err := xmessage.Unpack(
			eventEnvelopeBytes,
			eventEnvelope,
			&eventForApply,
		); err != nil {
			c.commandLogger.ErrorContext(
				ctx,
				"cannot unmarshal event",
				slog.Group(
					"event",
					xslog.UUID("message_id", eventMessageID),
				),
				xslog.Error(err),
			)

			return nil, false, nil
		}

		aggregateRoot.ApplyEvent(eventForApply)
	}

	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("unable to iterate historical events: %w", err)
	}

	return aggregateRoot, true, nil
}
