package aggregate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

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

type commandTask struct {
	Tx                   *sql.Tx
	Handler              dogma.AggregateMessageHandler[dogma.AggregateRoot]
	Identity             *identitypb.Identity
	Packer               *envelopepb.Packer
	MessageID            *uuidpb.UUID
	EnvelopeBytes        []byte
	ParentLogger, Logger *slog.Logger
}

var (
	errLocked = errors.New("instance is locked by another transaction")
	errFailed = errors.New("unable to handle command")
)

// Execute processes the task by handling its command and committing the
// transaction.
func (t *commandTask) Execute(ctx context.Context) error {
	defer t.Tx.Rollback()

	err := t.handleCommand(ctx)

	switch err {
	case nil:
		err = commandqueue.Remove(ctx, t.Tx, t.MessageID)
	case errLocked:
		err = commandqueue.DeferDueToContention(ctx, t.Tx, t.MessageID)
	case errFailed:
		err = commandqueue.DeferDueToFailure(ctx, t.Tx, t.MessageID)
	default:
		return err
	}

	if err := t.Tx.Commit(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}

// handleCommand processes the command in the given envelope.
func (t *commandTask) handleCommand(ctx context.Context) error {
	var (
		commandEnvelope                       = &envelopepb.Envelope{}
		commandForRouting, commandForHandling dogma.Command
	)

	if err := xmessage.Unpack(
		t.EnvelopeBytes,
		commandEnvelope,
		&commandForRouting,
		&commandForHandling,
	); err != nil {
		t.Logger.ErrorContext(
			ctx,
			"unable to unmarshal command",
			slog.Group(
				"command",
				xslog.UUID("message_id", t.MessageID),
			),
			xslog.Error(err),
		)

		return errFailed
	}

	t.Logger = t.ParentLogger.With(
		xslog.Envelope("command", commandEnvelope),
	)

	instanceID, err := t.routeCommandToInstance(ctx, commandForRouting)
	if err != nil {
		return err
	}

	t.Logger = t.ParentLogger.With(
		xslog.Envelope("command", commandEnvelope),
		slog.Group(
			"aggregate_instance",
			slog.String("id", instanceID),
		),
	)

	root, streamID, err := t.loadInstance(ctx, instanceID)
	if err != nil {
		return err
	}

	t.Logger = t.ParentLogger.With(
		xslog.Envelope("command", commandEnvelope),
		slog.Group(
			"aggregate_instance",
			slog.String("id", instanceID),
			slog.String("description", root.AggregateInstanceDescription()),
			xslog.UUID("stream_id", streamID),
		),
	)

	packer := t.Packer.PackEffects(
		commandEnvelope,
		t.Identity,
		envelopepb.WithInstanceID(instanceID),
	)

	if err := xerrors.Recover(
		func() error {
			t.Handler.HandleCommand(
				root,
				&scope{
					instanceID: instanceID,
					root:       root,
					packer:     packer,
					logger:     t.Logger,
				},
				commandForHandling,
			)
			return nil
		},
	); err != nil {
		t.Logger.ErrorContext(
			ctx,
			"unable to handle command",
			xslog.Error(err),
		)

		return errFailed
	}

	eventEnvelopes, ok := packer.Seal()
	if !ok {
		// No events were recorded.
		return nil
	}

	nextOffset, err := eventstream.Append(
		ctx,
		t.Tx,
		streamID,
		eventEnvelopes,
	)
	if err != nil {
		return err
	}

	return t.takeSnapshot(
		ctx,
		instanceID,
		root,
		nextOffset,
	)
}

// routeCommandToInstance routes the instance ID for the given command by
// calling the handler's RouteCommandToInstance() method.
func (t *commandTask) routeCommandToInstance(
	ctx context.Context,
	commandForRouting dogma.Command,
) (instanceID string, err error) {
	if err := xerrors.Recover(
		func() error {
			instanceID = t.Handler.RouteCommandToInstance(commandForRouting)
			if instanceID == "" {
				return errors.New("handler returned an empty instance ID")
			}
			return nil
		},
	); err != nil {
		t.Logger.ErrorContext(
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
func (t *commandTask) loadInstance(
	ctx context.Context,
	instanceID string,
) (
	root dogma.AggregateRoot,
	streamID *uuidpb.UUID,
	err error,
) {
	streamID, nextOffset, snapshot, exists, err := t.tryLockInstance(ctx, instanceID)
	if err != nil {
		return nil, nil, err
	}

	if !exists {
		streamID, nextOffset, snapshot, err = t.tryCreateInstance(ctx, instanceID)
		if err != nil {
			return nil, nil, err
		}
	}

	root, err = t.newRoot(ctx)
	if err != nil {
		return nil, nil, err
	}

	// If the instance has a snapshot, attempt to unmarshal it.
	if nextOffset != 0 {
		if !t.applySnapshot(ctx, root, snapshot) {
			// We couldn't unmarshal the snapshot, so we just create a fresh root
			// and replay all of the instance's historical events.
			nextOffset = 0
			root, err = t.newRoot(ctx)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	// Load events that were recorded after the snapshot was taken.
	if err := t.applyHistoricalEvents(
		ctx,
		streamID,
		nextOffset,
		instanceID,
		root,
	); err != nil {
		return nil, nil, err
	}

	return root, streamID, nil
}

// newRoot creates a new aggregate root by calling the handler's New() method.
func (t *commandTask) newRoot(ctx context.Context) (dogma.AggregateRoot, error) {
	var root dogma.AggregateRoot

	if err := xerrors.Recover(
		func() error {
			root = t.Handler.New()
			return nil
		},
	); err != nil {
		t.Logger.ErrorContext(
			ctx,
			"unable to create aggregate root",
			xslog.Error(err),
		)

		return nil, errFailed
	}

	return root, nil
}

// tryLockInstance attempts to acquire a lock on the aggregate instance with the
// given ID.
//
// The instance may or may not already exist.
func (t *commandTask) tryLockInstance(
	ctx context.Context,
	instanceID string,
) (
	streamID *uuidpb.UUID,
	offset eventstream.Offset,
	snapshot []byte,
	exists bool,
	err error,
) {
	// Try to lock the instance and fetch its data in a single round-trip. The
	// snapshot data is loaded from the CTE that attempts to acquire the lock
	// with FOR UPDATE SKIP LOCKED so that the snapshot data is only send across
	// the network if the lock is acquired successfully.
	row := t.Tx.QueryRowContext(
		ctx,
		`WITH exists AS (
			SELECT EXISTS (
				SELECT 1
				FROM aggregate.instances
				WHERE handler_key = $1
				AND instance_id = $2
			) AS exists
		), locked AS (
			SELECT
				stream_id,
				offset_after_snapshot,
				snapshot
			FROM aggregate.instances
			WHERE handler_key = $1
			AND instance_id = $2
			FOR UPDATE SKIP LOCKED
		)
		SELECT
			locked.stream_id,
			COALESCE(locked.offset_after_snapshot, 0) AS next_offset,
			locked.snapshot,
			exists.exists,
			locked.stream_id IS NOT NULL AS locked
		FROM exists
		LEFT JOIN locked
		ON true`,
		xsql.UUID(t.Identity.GetKey()),
		instanceID,
	)

	streamID = &uuidpb.UUID{}
	var locked bool

	if err := row.Scan(
		xsql.UUID(streamID),
		&offset,
		&snapshot,
		&exists,
		&locked,
	); err != nil {
		return nil, 0, nil, false, fmt.Errorf("unable to lock aggregate instance: %w", err)
	}

	if exists && !locked {
		t.Logger.DebugContext(
			ctx,
			"instance is locked by another transaction",
		)
		return nil, 0, nil, true, errLocked
	}

	return streamID, offset, snapshot, exists, nil
}

// tryCreateInstance attempts to create a new aggregate instance. If it already
// exists it returns the existing instance's data.
func (t *commandTask) tryCreateInstance(ctx context.Context, instanceID string) (
	streamID *uuidpb.UUID,
	offset eventstream.Offset,
	snapshot []byte,
	err error,
) {
	acquiredStreamID, err := eventstream.Acquire(ctx, t.Tx)
	if err != nil {
		return nil, 0, nil, err
	}

	// If another transaction is racing to create the same instance we may
	// lose the race and block until it completes, in which case we must
	// honor the event stream binding that the other transaction
	// established, rather than using the candidate we chose.
	row := t.Tx.QueryRowContext(
		ctx,
		`INSERT INTO aggregate.instances (
			handler_key,
			instance_id,
			stream_id
		) VALUES ($1, $2, $3)
		ON CONFLICT (handler_key, instance_id) DO UPDATE SET
			instance_id = EXCLUDED.instance_id
		RETURNING
			stream_id,
			offset_after_snapshot,
			snapshot`,
		xsql.UUID(t.Identity.GetKey()),
		instanceID,
		xsql.UUID(acquiredStreamID),
	)

	streamID = &uuidpb.UUID{}

	if err := row.Scan(
		xsql.UUID(streamID),
		&offset,
		&snapshot,
	); err != nil {
		return nil, 0, nil, fmt.Errorf("unable to insert aggregate instance: %w", err)
	}

	return streamID, offset, snapshot, nil
}

// applyHistoricalEvents applies all events recorded by the aggregate instance
// to the aggregate root starting at the given offset.
func (t *commandTask) applyHistoricalEvents(
	ctx context.Context,
	streamID *uuidpb.UUID,
	offset eventstream.Offset,
	instanceID string,
	root dogma.AggregateRoot,
) error {
	rows, err := t.Tx.QueryContext(
		ctx,
		`SELECT
			e.message_id,
			e.envelope
		FROM eventstream.events AS e
		WHERE e.stream_id = $1
		AND e.stream_offset >= $2
		AND e.aggregate_handler_key = $3
		AND e.aggregate_instance_id = $4
		ORDER BY e.stream_offset`,
		xsql.UUID(streamID),
		offset,
		xsql.UUID(t.Identity.GetKey()),
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
			t.Logger.ErrorContext(
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
				root.ApplyEvent(eventForApply)
				return nil
			},
		); err != nil {
			t.Logger.ErrorContext(
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

func (t *commandTask) applySnapshot(
	ctx context.Context,
	root dogma.AggregateRoot,
	snapshot []byte,
) bool {
	if err := xerrors.Recover(
		func() error {
			return root.UnmarshalBinary(snapshot)
		},
	); err != nil {
		if !errors.Is(err, dogma.ErrNotSupported) {
			t.Logger.ErrorContext(
				ctx,
				"unable to unmarshal snapshot",
				xslog.Error(err),
			)
		}

		return false
	}

	return true
}

func (t *commandTask) takeSnapshot(
	ctx context.Context,
	instanceID string,
	root dogma.AggregateRoot,
	offset eventstream.Offset,
) error {
	var snapshot []byte

	if err := xerrors.Recover(
		func() error {
			var err error
			snapshot, err = root.MarshalBinary()
			return err
		},
	); err != nil {
		if !errors.Is(err, dogma.ErrNotSupported) {
			t.Logger.ErrorContext(
				ctx,
				"unable to marshal snapshot",
				xslog.Error(err),
			)
		}

		return nil
	}

	if err := xsql.ExecOne(
		ctx,
		t.Tx,
		`UPDATE aggregate.instances SET
			offset_after_snapshot = $1,
			snapshot = $2
		WHERE handler_key = $3
		AND instance_id = $4`,
		offset,
		snapshot,
		xsql.UUID(t.Identity.GetKey()),
		instanceID,
	); err != nil {
		return fmt.Errorf("unable to persist snapshot: %w", err)
	}

	return nil
}
