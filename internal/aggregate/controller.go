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
	"github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// Controller manages the state of aggregate instances for a single aggregate
// message handler type.
type Controller struct {
	DB             *sql.DB
	Handler        dogma.AggregateMessageHandler[dogma.AggregateRoot]
	CommandTypeIDs []string
	Logger         *slog.Logger
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

				commandForRouting, ok, err := c.unpackCommand(ctx, tx, commandEnvelope)
				if !ok || err != nil {
					return err
				}

				instanceID := c.Handler.RouteCommandToInstance(commandForRouting)
				if instanceID == "" {
					return errors.New("handler returned an empty instance ID")
				}

				// Unpack the command a second time, as the Dogma API requires that each
				// time a message is passed to the application, the application assumes
				// ownership of the message and the values within it.
				commandForHandling, ok, err := c.unpackCommand(ctx, tx, commandEnvelope)
				if !ok || err != nil {
					return err
				}

				root := c.Handler.New()

				c.Handler.HandleCommand(
					root,
					&scope{
						instanceID: instanceID,
					},
					commandForHandling,
				)

				pollImmediately = true

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
