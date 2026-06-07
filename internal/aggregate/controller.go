package aggregate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/reference-engine/internal/database"
)

// Controller manages the state of aggregate instances for a single aggregate
// message handler type.
type Controller struct {
	DB      *sql.DB
	Handler dogma.AggregateMessageHandler[dogma.AggregateRoot]
}

func (c *Controller) Run(ctx context.Context) error {
	for {
		row := c.DB.QueryRowContext(
			ctx,
			`SELECT
				envelope
			FROM dogma.pending_commands
			LIMIT 1`,
		)

		commandEnvelope := &envelopepb.Envelope{}
		if err := row.Scan(
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
	}
}
