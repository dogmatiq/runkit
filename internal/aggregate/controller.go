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
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
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
	for {
		row := c.DB.QueryRowContext(
			ctx,
			`SELECT
				message_type_id,
				envelope
			FROM dogma.pending_commands
			WHERE message_type_id = ANY($1)
			LIMIT 1`,
			c.CommandTypeIDs,
		)

		commandEnvelope := &envelopepb.Envelope{}
		messageTypeID := &uuidpb.UUID{}

		if err := row.Scan(
			xsql.UUID(messageTypeID),
			xsql.Envelope(commandEnvelope),
		); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("unable to scan pending command: %w", err)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(25 * time.Millisecond):
				continue
			}
		}

		commandForRouting, err := envelopepb.Unpack[dogma.Command](commandEnvelope)
		if err != nil {
			c.Logger.ErrorContext(
				ctx,
				"command cannot be unpacked from envelope",
				xslog.Envelope(commandEnvelope),
			)

			if err := commandqueue.Defer(
				ctx,
				c.DB,
				commandEnvelope.GetBody().GetMessageId(),
			); err != nil {
				return err
			}

			continue
		}

		instanceID := c.Handler.RouteCommandToInstance(commandForRouting)
		if instanceID == "" {
			return errors.New("handler returned an empty instance ID")
		}

		// Unpack the command a second time, as the Dogma API requires that each
		// time a message is passed to the application, the application assumes
		// ownership of the message and the values within it.
		commandForHandling, err := envelopepb.Unpack[dogma.Command](commandEnvelope)
		if err != nil {
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

		return xsql.Transact(
			ctx,
			c.DB,
			func(ctx context.Context, tx *sql.Tx) error {
				return commandqueue.Remove(
					ctx,
					tx,
					commandEnvelope.GetBody().GetMessageId(),
				)
			},
		)
	}
}
