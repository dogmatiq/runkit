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
		`SELECT envelope
		FROM dogma.pending_commands
		WHERE message_type_id = ANY($1)
		AND next_attempt_at <= clock_timestamp()
		LIMIT 1
		FOR UPDATE SKIP LOCKED`,
		c.CommandTypeIDs,
	)

	var (
		commandEnvelope                       = &envelopepb.Envelope{}
		commandForRouting, commandForHandling dogma.Command
	)

	if err := row.Scan(
		xsql.UnpackEnvelope(
			commandEnvelope,
			&commandForRouting,
			&commandForHandling,
		),
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errNoPendingCommands
		}

		if err, ok := errors.AsType[xsql.UnpackEnvelopeError](err); ok {
			c.Logger.ErrorContext(
				ctx,
				"cannot unpack command envelope",
				xslog.Envelope(commandEnvelope),
				xslog.Error(err),
			)

			return commandqueue.Defer(
				ctx,
				tx,
				commandEnvelope.GetBody().GetMessageId(),
			)
		}

		return fmt.Errorf("unable to scan pending command: %w", err)
	}

	instanceID := c.Handler.RouteCommandToInstance(commandForRouting)
	if instanceID == "" {
		return errors.New("handler returned an empty instance ID")
	}

	root, eventStreamID, ok, err := c.loadInstance(ctx, tx, instanceID, commandEnvelope)
	if !ok || err != nil {
		return err
	}

	envelopePacker := c.EnvelopePacker.PackEffects(
		commandEnvelope,
		c.HandlerIdentity,
		envelopepb.WithInstanceID(instanceID),
	)

	c.Handler.HandleCommand(
		root,
		&scope{
			instanceID:     instanceID,
			envelopePacker: envelopePacker,
		},
		commandForHandling,
	)

	if eventEnvelopes, ok := envelopePacker.Seal(); ok {
		if err := eventstream.Append(
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

// loadInstance loads the aggregate instance with the given ID, and returns the
// root and event stream ID for the instance.
func (c *Controller) loadInstance(
	ctx context.Context,
	tx *sql.Tx,
	instanceID string,
	commandEnvelope *envelopepb.Envelope,
) (root dogma.AggregateRoot, eventStreamID *uuidpb.UUID, ok bool, err error) {
	eventStreamID, err = c.ensureInstanceLocked(ctx, tx, instanceID)
	if err != nil {
		return nil, nil, false, err
	}

	rows, err := tx.QueryContext(
		ctx,
		`SELECT envelope
		FROM dogma.events
		WHERE event_stream_id = $1
		AND aggregate_handler_key = $2
		AND aggregate_instance_id = $3
		ORDER BY event_offset`,
		xsql.UUID(eventStreamID),
		xsql.UUID(c.HandlerIdentity.GetKey()),
		instanceID,
	)
	if err != nil {
		return nil, nil, false, fmt.Errorf("unable to query historical events: %w", err)
	}
	defer rows.Close()

	root = c.Handler.New()

	var (
		// Reuse the same envelope for each event to avoid unnecessary
		// allocations.
		eventEnvelope = &envelopepb.Envelope{}
		event         dogma.Event
	)

	for rows.Next() {
		if err := rows.Scan(
			xsql.UnpackEnvelope(
				eventEnvelope,
				&event,
			),
		); err != nil {
			if err, ok := errors.AsType[xsql.UnpackEnvelopeError](err); ok {
				c.Logger.ErrorContext(
					ctx,
					"cannot unpack historical event envelope",
					xslog.Envelope(eventEnvelope),
					xslog.Error(err),
				)

				if err := rows.Close(); err != nil {
					return nil, nil, false, fmt.Errorf("unable to close rows after envelope unpack error: %w", err)
				}

				return nil, nil, false, commandqueue.Defer(
					ctx,
					tx,
					commandEnvelope.GetBody().GetMessageId(),
				)
			}

			return nil, nil, false, fmt.Errorf("unable to scan historical event: %w", err)
		}

		root.ApplyEvent(event)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, false, fmt.Errorf("unable to iterate historical events: %w", err)
	}

	return root, eventStreamID, true, nil
}

// ensureInstanceLocked attempts to acquire a lock on the aggregate instance
// with the given ID, and returns the event stream ID for the instance.
//
// If the instance does not exist, it is created and the event stream ID for the
// new instance is returned.
func (c *Controller) ensureInstanceLocked(
	ctx context.Context,
	tx *sql.Tx,
	instanceID string,
) (eventStreamID *uuidpb.UUID, err error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT event_stream_id
		FROM dogma.aggregate_instances
		WHERE handler_key = $1
		AND instance_id = $2
		FOR UPDATE`,
		xsql.UUID(c.HandlerIdentity.GetKey()),
		instanceID,
	)

	eventStreamID = &uuidpb.UUID{}

	err = row.Scan(
		xsql.UUID(eventStreamID),
	)

	// The instance exists, return its event stream.
	if err == nil {
		return eventStreamID, nil
	}

	// Some legitimate error occurred, return it.
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("unable to scan event stream ID: %w", err)
	}

	// The instance does not exist, acquire an event stream for this instance.
	eventStreamID, err = eventstream.Acquire(ctx, tx)
	if err != nil {
		return nil, err
	}

	// Attempt to insert the new instance, or if it was inserted after our
	// initial select, return the existing instance's event stream ID.
	//
	// Either way, the row is locked by this transaction after this query.
	row = tx.QueryRowContext(
		ctx,
		`INSERT INTO dogma.aggregate_instances (
			handler_key,
			instance_id,
			event_stream_id
		) VALUES ($1, $2, $3)
		ON CONFLICT (handler_key, instance_id)
		DO UPDATE SET
			instance_id = EXCLUDED.instance_id
		RETURNING event_stream_id`,
		xsql.UUID(c.HandlerIdentity.GetKey()),
		instanceID,
		xsql.UUID(eventStreamID),
	)

	if err := row.Scan(
		xsql.UUID(eventStreamID),
	); err != nil {
		return nil, fmt.Errorf("unable to upsert aggregate instance: %w", err)
	}

	return eventStreamID, nil
}
