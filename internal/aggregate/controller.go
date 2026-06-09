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
	Packer          *envelopepb.Packer
	CommandTypeIDs  []string
	Logger          *slog.Logger
}

// Run handles messages for the controller's handler until ctx is canceled.
func (c *Controller) Run(ctx context.Context) (err error) {
	c.Logger.DebugContext(
		ctx,
		"aggregate controller started",
	)

	defer func() {
		c.Logger.DebugContext(
			ctx,
			"aggregate controller stopped",
			xslog.Error(err),
		)
	}()

	for {
		pollImmediately := false

		if err := xsql.Transact(
			ctx,
			c.DB,
			func(ctx context.Context, tx *sql.Tx) error {
				row := tx.QueryRowContext(
					ctx,
					`SELECT envelope
					FROM dogma.pending_commands
					WHERE message_type_id = ANY($1)
					LIMIT 1
					FOR UPDATE SKIP LOCKED`,
					c.CommandTypeIDs,
				)

				commandEnvelope := &envelopepb.Envelope{}

				if err := row.Scan(
					xsql.Envelope(commandEnvelope),
				); err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						return nil
					}

					return fmt.Errorf("unable to scan pending command: %w", err)
				}

				pollImmediately = true

				commandForRouting, ok, err := c.unpackCommand(ctx, tx, commandEnvelope)
				if !ok || err != nil {
					return err
				}

				instanceID := c.Handler.RouteCommandToInstance(commandForRouting)
				if instanceID == "" {
					return errors.New("handler returned an empty instance ID")
				}

				root, ok, err := c.loadInstance(ctx, tx, instanceID, commandEnvelope)
				if !ok || err != nil {
					return err
				}

				// Unpack the command a second time, as the Dogma API requires that each
				// time a message is passed to the application, the application assumes
				// ownership of the message and the values within it.
				commandForHandling, ok, err := c.unpackCommand(ctx, tx, commandEnvelope)
				if !ok || err != nil {
					return err
				}

				packer := c.Packer.PackEffects(
					commandEnvelope,
					c.HandlerIdentity,
					envelopepb.WithInstanceID(instanceID),
				)

				c.Handler.HandleCommand(
					root,
					&scope{
						instanceID: instanceID,
						packer:     packer,
					},
					commandForHandling,
				)

				if eventEnvelopes, ok := packer.Seal(); ok {
					eventStreamID, err := c.findEventStreamID(ctx, tx, instanceID)
					if err != nil {
						return err
					}

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
			},
		); err != nil {
			return err
		}

		if !pollImmediately {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(25 * time.Millisecond):
				continue
			}
		}
	}
}

func (c *Controller) findEventStreamID(ctx context.Context, tx *sql.Tx, instanceID string) (*uuidpb.UUID, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT event_stream_id
		FROM dogma.events
		WHERE aggregate_handler_key = $1
			AND aggregate_instance_id = $2
		ORDER BY event_offset
		LIMIT 1`,
		xsql.UUID(c.HandlerIdentity.GetKey()),
		instanceID,
	)

	eventStreamID := &uuidpb.UUID{}

	if err := row.Scan(
		xsql.UUID(eventStreamID),
	); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("unable to scan event stream ID: %w", err)
		}

		return eventstream.Acquire(ctx, tx)
	}

	return eventStreamID, nil
}

func (c *Controller) loadInstance(
	ctx context.Context,
	tx *sql.Tx,
	instanceID string,
	commandEnvelope *envelopepb.Envelope,
) (dogma.AggregateRoot, bool, error) {

	rows, err := tx.QueryContext(
		ctx,
		`SELECT envelope
		FROM dogma.events
		WHERE aggregate_handler_key = $1
			AND aggregate_instance_id = $2
		ORDER BY event_offset`,
		xsql.UUID(c.HandlerIdentity.GetKey()),
		instanceID,
	)
	if err != nil {
		return nil, false, fmt.Errorf("unable to query historical events: %w", err)
	}
	defer rows.Close()

	root := c.Handler.New()

	for rows.Next() {
		eventEnvelope := &envelopepb.Envelope{}

		if err := rows.Scan(
			xsql.Envelope(eventEnvelope),
		); err != nil {
			return nil, false, fmt.Errorf("unable to scan historical event: %w", err)
		}

		event, err := envelopepb.Unpack[dogma.Event](eventEnvelope)
		if err != nil {
			c.Logger.ErrorContext(
				ctx,
				"event cannot be unpacked from envelope",
				xslog.Envelope(eventEnvelope),
			)

			return nil, false, commandqueue.Defer(
				ctx,
				tx,
				commandEnvelope.GetBody().GetMessageId(),
			)
		}

		root.ApplyEvent(event)

		c.Logger.DebugContext(
			ctx,
			"event applied to aggregate root",
			xslog.Envelope(eventEnvelope),
		)
	}

	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("unable to iterate historical events: %w", err)
	}

	return root, true, nil
}

func (c *Controller) unpackCommand(
	ctx context.Context,
	tx *sql.Tx,
	commandEnvelope *envelopepb.Envelope,
) (dogma.Command, bool, error) {
	command, err := envelopepb.Unpack[dogma.Command](commandEnvelope)
	if err != nil {
		c.Logger.ErrorContext(
			ctx,
			"command cannot be unpacked from envelope",
			xslog.Envelope(commandEnvelope),
		)

		return nil, false, commandqueue.Defer(
			ctx,
			tx,
			commandEnvelope.GetBody().GetMessageId(),
		)
	}

	return command, true, nil
}
