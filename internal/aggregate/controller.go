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
	"github.com/dogmatiq/reference-engine/internal/x/xerrors"
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
}

var (
	errIdle   = errors.New("no pending commands")
	errLocked = errors.New("instance locked")
	errFailed = errors.New("handling command")
)

// Run handles messages for the controller's handler until ctx is canceled.
func (c *Controller) Run(ctx context.Context) (err error) {
	for {
		if err := xsql.Transact(
			ctx,
			c.DB,
			func(ctx context.Context, tx *sql.Tx) error {
				commandMessageID, commandEnvelopeBytes, err := c.lockCommand(ctx, tx)
				if err != nil {
					return err
				}

				err = c.handleCommand(
					ctx,
					tx,
					commandMessageID,
					commandEnvelopeBytes,
				)

				switch err {
				case nil:
					return commandqueue.Remove(ctx, tx, commandMessageID)
				case errLocked:
					return commandqueue.DeferDueToContention(ctx, tx, commandMessageID)
				case errFailed:
					return commandqueue.DeferDueToFailure(ctx, tx, commandMessageID)
				default:
					return err
				}
			},
		); err != nil {
			if !errors.Is(err, errIdle) {
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

// lockCommand attempts to exclusively lock the next pending command for the
// handler and return its message ID and envelope data.
//
// If there are no pending commands, it returns [errIdle].
func (c *Controller) lockCommand(
	ctx context.Context,
	tx *sql.Tx,
) (
	commandMessageID *uuidpb.UUID,
	commandEnvelopeBytes []byte,
	err error,
) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT
			c.message_id,
			c.envelope
		FROM dogma.pending_commands AS c
		WHERE message_type_id = ANY($1)
		AND execute_at <= clock_timestamp()
		ORDER BY execute_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED`,
		c.CommandTypeIDs,
	)

	commandMessageID = &uuidpb.UUID{}

	if err := row.Scan(
		xsql.UUID(commandMessageID),
		&commandEnvelopeBytes,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, errIdle
		}

		return nil, nil, fmt.Errorf("unable to scan pending command: %w", err)
	}

	return commandMessageID, commandEnvelopeBytes, nil
}

// handleCommand processes the command in the given envelope.
func (c *Controller) handleCommand(
	ctx context.Context,
	tx *sql.Tx,
	commandMessageID *uuidpb.UUID,
	commandEnvelopeBytes []byte,
) error {
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

		return errFailed
	}

	logger := c.Logger.With(
		xslog.Envelope("command", commandEnvelope),
	)

	instanceID, err := c.routeCommandToInstance(ctx, commandForRouting, logger)
	if err != nil {
		return err
	}

	logger = c.Logger.With(
		xslog.Envelope("command", commandEnvelope),
		slog.Group(
			"aggregate_instance",
			slog.String("id", instanceID),
		),
	)

	aggregateRoot, eventStreamID, err := c.loadInstance(ctx, tx, instanceID, logger)
	if err != nil {
		return err
	}

	logger = c.Logger.With(
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

	if err := xerrors.Recover(
		func() error {
			c.Handler.HandleCommand(
				aggregateRoot,
				&scope{
					instanceID:     instanceID,
					aggregateRoot:  aggregateRoot,
					envelopePacker: envelopePacker,
					logger:         logger,
				},
				commandForHandling,
			)
			return nil
		},
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to handle command",
			xslog.Error(err),
		)

		return errFailed
	}

	eventEnvelopes, ok := envelopePacker.Seal()
	if !ok {
		// No events were recorded.
		return nil
	}

	nextOffset, err := eventstream.Append(
		ctx,
		tx,
		eventStreamID,
		eventEnvelopes,
	)
	if err != nil {
		return err
	}

	return c.takeSnapshot(
		ctx,
		tx,
		instanceID,
		aggregateRoot,
		nextOffset,
		logger,
	)
}

// routeCommandToInstance routes the instance ID for the given command by
// calling the handler's RouteCommandToInstance() method.
func (c *Controller) routeCommandToInstance(
	ctx context.Context,
	commandForRouting dogma.Command,
	logger *slog.Logger,
) (string, error) {
	var instanceID string

	if err := xerrors.Recover(
		func() error {
			instanceID = c.Handler.RouteCommandToInstance(commandForRouting)
			if instanceID == "" {
				return errors.New("handler returned an empty instance ID")
			}
			return nil
		},
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to route command to instance",
			xslog.Error(err),
		)

		return "", errFailed
	}

	return instanceID, nil
}

// loadInstance loads the aggregate instance with the given ID and returns its
// aggregate root and the ID of the event stream to which new events must be
// appended.
func (c *Controller) loadInstance(
	ctx context.Context,
	tx *sql.Tx,
	instanceID string,
	logger *slog.Logger,
) (
	aggregateRoot dogma.AggregateRoot,
	eventStreamID *uuidpb.UUID,
	err error,
) {
	eventStreamID, nextOffset, snapshot, exists, locked, err := c.tryLockInstance(ctx, tx, instanceID)
	if err != nil {
		return nil, nil, err
	}

	if !exists {
		eventStreamID, nextOffset, snapshot, err = c.tryCreateInstance(ctx, tx, instanceID)
		if err != nil {
			return nil, nil, err
		}
	} else if !locked {
		logger.DebugContext(
			ctx,
			"instance is locked by another transaction",
		)
		return nil, nil, errLocked
	}

	aggregateRoot, err = c.newRoot(ctx, logger)
	if err != nil {
		return nil, nil, err
	}

	// If the instance has a snapshot, attempt to unmarshal it.
	if nextOffset != 0 {
		if !c.applySnapshot(ctx, aggregateRoot, snapshot, logger) {
			// We couldn't unmarshal the snapshot, so we just create a fresh root
			// and replay all of the instance's historical events.
			nextOffset = 0
			aggregateRoot, err = c.newRoot(ctx, logger)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	// Load events that were recorded after the snapshot was taken.
	if err := c.applyHistoricalEvents(
		ctx,
		tx,
		eventStreamID,
		nextOffset,
		instanceID,
		aggregateRoot,
		logger,
	); err != nil {
		return nil, nil, err
	}

	return aggregateRoot, eventStreamID, nil
}

// newRoot creates a new aggregate root by calling the handler's New() method.
func (c *Controller) newRoot(
	ctx context.Context,
	logger *slog.Logger,
) (aggregateRoot dogma.AggregateRoot, err error) {
	if err := xerrors.Recover(
		func() error {
			aggregateRoot = c.Handler.New()
			return nil
		},
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to create aggregate root",
			xslog.Error(err),
		)

		return nil, errFailed
	}

	return aggregateRoot, nil
}

// tryLockInstance attempts to acquire a lock on the aggregate instance with the
// given ID.
//
// The instance may or may not already exist.
func (c *Controller) tryLockInstance(
	ctx context.Context,
	tx *sql.Tx,
	instanceID string,
) (
	eventStreamID *uuidpb.UUID,
	nextOffset eventstream.Offset,
	snapshot []byte,
	exists, locked bool,
	err error,
) {
	// Try to lock the instance and fetch its data in a single round-trip. The
	// snapshot data is loaded from the CTE that attempts to acquire the lock
	// with FOR UPDATE SKIP LOCKED so that the snapshot data is only send across
	// the network if the lock is acquired successfully.
	row := tx.QueryRowContext(
		ctx,
		`WITH exists AS (
			SELECT EXISTS (
				SELECT 1
				FROM dogma.aggregate_instances
				WHERE handler_key = $1
				AND instance_id = $2
			) AS exists
		), locked AS (
			SELECT
				event_stream_id,
				offset_after_snapshot,
				snapshot
			FROM dogma.aggregate_instances
			WHERE handler_key = $1
			AND instance_id = $2
			FOR UPDATE SKIP LOCKED
		)
		SELECT
			locked.event_stream_id,
			COALESCE(locked.offset_after_snapshot, 0) AS next_offset,
			locked.snapshot,
			exists.exists,
			locked.event_stream_id IS NOT NULL AS locked
		FROM exists
		LEFT JOIN locked
		ON true`,
		xsql.UUID(c.HandlerIdentity.GetKey()),
		instanceID,
	)

	eventStreamID = &uuidpb.UUID{}

	if err := row.Scan(
		xsql.UUID(eventStreamID),
		&nextOffset,
		&snapshot,
		&exists,
		&locked,
	); err != nil {
		return nil, 0, nil, false, false, fmt.Errorf("unable to lock aggregate instance: %w", err)
	}

	return eventStreamID, nextOffset, snapshot, exists, locked, nil
}

// tryCreateInstance attempts to create a new aggregate instance. If it already
// exists it returns the existing instance's data.
func (c *Controller) tryCreateInstance(
	ctx context.Context,
	tx *sql.Tx,
	instanceID string,
) (
	eventStreamID *uuidpb.UUID,
	nextOffset eventstream.Offset,
	snapshot []byte,
	err error,
) {
	candidateEventStreamID, err := eventstream.Acquire(ctx, tx)
	if err != nil {
		return nil, 0, nil, err
	}

	// If another transaction is racing to create the same instance we may
	// lose the race and block until it completes, in which case we must
	// honor the event stream binding that the other transaction
	// established, rather than using the candidate we chose.
	row := tx.QueryRowContext(
		ctx,
		`INSERT INTO dogma.aggregate_instances (
			handler_key,
			instance_id,
			event_stream_id
		) VALUES ($1, $2, $3)
		ON CONFLICT (handler_key, instance_id) DO UPDATE SET
			instance_id = EXCLUDED.instance_id
		RETURNING
			event_stream_id,
			offset_after_snapshot,
			snapshot`,
		xsql.UUID(c.HandlerIdentity.GetKey()),
		instanceID,
		xsql.UUID(candidateEventStreamID),
	)

	eventStreamID = &uuidpb.UUID{}

	if err := row.Scan(
		xsql.UUID(eventStreamID),
		&nextOffset,
		&snapshot,
	); err != nil {
		return nil, 0, nil, fmt.Errorf("unable to insert aggregate instance: %w", err)
	}

	return eventStreamID, nextOffset, snapshot, nil
}

// applyHistoricalEvents applies all events recorded by the aggregate instance
// to the aggregate root starting at the given offset.
func (c *Controller) applyHistoricalEvents(
	ctx context.Context,
	tx *sql.Tx,
	eventStreamID *uuidpb.UUID,
	nextOffset eventstream.Offset,
	instanceID string,
	aggregateRoot dogma.AggregateRoot,
	logger *slog.Logger,
) error {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT
			message_id,
			envelope
		FROM dogma.events
		WHERE event_stream_id = $1
		AND event_stream_offset >= $2
		AND aggregate_handler_key = $3
		AND aggregate_instance_id = $4
		ORDER BY event_stream_offset`,
		xsql.UUID(eventStreamID),
		nextOffset,
		xsql.UUID(c.HandlerIdentity.GetKey()),
		instanceID,
	)
	if err != nil {
		return fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	// Apply the events to the root in order.
	for rows.Next() {
		var (
			eventMessageID     = &uuidpb.UUID{}
			eventEnvelopeBytes []byte
		)

		if err := rows.Scan(
			xsql.UUID(eventMessageID),
			&eventEnvelopeBytes,
		); err != nil {
			return fmt.Errorf("unable to scan event: %w", err)
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
			logger.ErrorContext(
				ctx,
				"unable to unmarshal event",
				slog.Group(
					"event",
					xslog.UUID("message_id", eventMessageID),
				),
				xslog.Error(err),
			)

			return errFailed
		}

		if err := xerrors.Recover(
			func() error {
				aggregateRoot.ApplyEvent(eventForApply)
				return nil
			},
		); err != nil {
			logger.ErrorContext(
				ctx,
				"unable to apply event to aggregate root",
				slog.Group(
					"event",
					xslog.UUID("message_id", eventMessageID),
				),
				xslog.Error(err),
			)

			return errFailed
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("unable to iterate historical events: %w", err)
	}

	return nil
}

func (c *Controller) applySnapshot(
	ctx context.Context,
	aggregateRoot dogma.AggregateRoot,
	snapshot []byte,
	logger *slog.Logger,
) bool {
	if err := xerrors.Recover(
		func() error {
			return aggregateRoot.UnmarshalBinary(snapshot)
		},
	); err != nil {
		if !errors.Is(err, dogma.ErrNotSupported) {
			logger.ErrorContext(
				ctx,
				"unable to unmarshal snapshot",
				xslog.Error(err),
			)
		}

		return false
	}

	return true
}

func (c *Controller) takeSnapshot(
	ctx context.Context,
	tx *sql.Tx,
	instanceID string,
	aggregateRoot dogma.AggregateRoot,
	nextOffset eventstream.Offset,
	logger *slog.Logger,
) error {
	var snapshot []byte

	if err := xerrors.Recover(
		func() error {
			var err error
			snapshot, err = aggregateRoot.MarshalBinary()
			return err
		},
	); err != nil {
		if !errors.Is(err, dogma.ErrNotSupported) {
			logger.ErrorContext(
				ctx,
				"unable to marshal snapshot",
				xslog.Error(err),
			)
		}

		return nil
	}

	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE dogma.aggregate_instances SET
			offset_after_snapshot = $1,
			snapshot = $2
		WHERE handler_key = $3
		AND instance_id = $4`,
		nextOffset,
		snapshot,
		xsql.UUID(c.HandlerIdentity.GetKey()),
		instanceID,
	); err != nil {
		return fmt.Errorf("unable to persist snapshot: %w", err)
	}

	return nil
}
