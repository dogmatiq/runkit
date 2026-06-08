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
	"github.com/dogmatiq/reference-engine/internal/database"
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
	c.Logger.InfoContext(
		ctx,
		"aggregate controller started",
		slog.Any("command_types", c.CommandTypeIDs),
	)
	defer func() {
		if err == nil || errors.Is(err, ctx.Err()) {
			c.Logger.InfoContext(
				ctx,
				"aggregate controller stopped",
			)
		} else {
			c.Logger.ErrorContext(
				ctx,
				"aggregate controller stopped",
				slog.String("error", err.Error()),
			)
		}
	}()

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
			database.UUID(messageTypeID),
			database.Envelope(commandEnvelope),
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
			return err
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

		if _, err := c.DB.ExecContext(
			ctx,
			`DELETE FROM dogma.pending_commands
			WHERE message_id = $1`,
			database.UUID(commandEnvelope.GetBody().GetMessageId()),
		); err != nil {
			return fmt.Errorf("unable to delete pending command: %w", err)
		}
	}
}
