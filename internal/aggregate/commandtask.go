package aggregate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
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
	BackoffBase, BackoffCap time.Duration
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

	instanceID, err := t.handleCommand(ctx)

	if errors.Is(err, errLocked) {
		err = t.postpone(ctx)
	} else if errors.Is(err, errFailed) {
		err = t.failAndPostpone(ctx, instanceID)
	}

	if err != nil {
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
func (t *commandTask) handleCommand(ctx context.Context) (string, error) {
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

		return "", errFailed
	}

	t.Logger = t.ParentLogger.With(
		xslog.Envelope("command", commandEnvelope),
	)

	instanceID, err := t.routeCommandToInstance(ctx, commandForRouting)
	if err != nil {
		return "", err
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
		return instanceID, err
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

	if err := xerrors.ConvertPanicToError(
		func() error {
			t.Handler.HandleCommand(
				root,
				&messageScope{
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

		return instanceID, errFailed
	}

	if eventEnvelopes, ok := packer.Seal(); ok {
		return instanceID, t.completeWithEvents(
			ctx,
			instanceID,
			streamID,
			eventEnvelopes,
			root,
		)
	}

	return instanceID, t.completeWithoutEvents(ctx, instanceID)
}

// routeCommandToInstance routes the instance ID for the given command by
// calling the handler's RouteCommandToInstance() method.
func (t *commandTask) routeCommandToInstance(
	ctx context.Context,
	commandForRouting dogma.Command,
) (instanceID string, err error) {
	if err := xerrors.ConvertPanicToError(
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

	// If the instance has no stream binding there are no events to replay.
	if streamID == nil {
		return root, nil, nil
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

	if err := xerrors.ConvertPanicToError(
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
	offset uint64,
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
				snapshot_offset,
				snapshot
			FROM aggregate.instances
			WHERE handler_key = $1
			AND instance_id = $2
			FOR UPDATE SKIP LOCKED
		)
		SELECT
			locked.stream_id,
			COALESCE(locked.snapshot_offset + 1, 0),
			locked.snapshot,
			exists.exists,
			(SELECT COUNT(*) FROM locked) > 0 AS locked
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

	if streamID.Validate() != nil {
		streamID = nil
	}

	return streamID, offset, snapshot, exists, nil
}

// tryCreateInstance attempts to create a new aggregate instance. If it already
// exists it returns the existing instance's data.
func (t *commandTask) tryCreateInstance(ctx context.Context, instanceID string) (
	streamID *uuidpb.UUID,
	offset uint64,
	snapshot []byte,
	err error,
) {
	// If another transaction is racing to create the same instance we may
	// lose the race and block until it completes, in which case we must
	// honor the event stream binding that the other transaction
	// establishes, if any.
	row := t.Tx.QueryRowContext(
		ctx,
		`INSERT INTO aggregate.instances (
			handler_key,
			instance_id
		) VALUES ($1, $2)
		ON CONFLICT (handler_key, instance_id) DO UPDATE SET
			instance_id = EXCLUDED.instance_id
		RETURNING
			stream_id,
			COALESCE(snapshot_offset + 1, 0),
			snapshot`,
		xsql.UUID(t.Identity.GetKey()),
		instanceID,
	)

	streamID = &uuidpb.UUID{}

	if err := row.Scan(
		xsql.UUID(streamID),
		&offset,
		&snapshot,
	); err != nil {
		return nil, 0, nil, fmt.Errorf("unable to insert aggregate instance: %w", err)
	}

	if streamID.Validate() != nil {
		streamID = nil
	}

	return streamID, offset, snapshot, nil
}

// applyHistoricalEvents applies all events recorded by the aggregate instance
// to the aggregate root starting at the given offset.
func (t *commandTask) applyHistoricalEvents(
	ctx context.Context,
	streamID *uuidpb.UUID,
	offset uint64,
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

		if err := xerrors.ConvertPanicToError(
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
	if err := xerrors.ConvertPanicToError(
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

// completeWithoutEvents removes the command from the queue when no events were
// recorded during handling. It deletes the aggregate instance if there are no
// prior events.
func (t *commandTask) completeWithoutEvents(
	ctx context.Context,
	instanceID string,
) error {
	if _, err := t.Tx.ExecContext(
		ctx,
		`SELECT aggregate.complete_without_events($1, $2, $3)`,
		xsql.UUID(t.MessageID),
		xsql.UUID(t.Identity.GetKey()),
		instanceID,
	); err != nil {
		return fmt.Errorf("unable to complete command handling: %w", err)
	}

	return nil
}

// completeWithEvents appends events to the aggregate's event stream, optionally
// persists a snapshot, and removes the command from the queue in a single
// database round-trip.
func (t *commandTask) completeWithEvents(
	ctx context.Context,
	instanceID string,
	streamID *uuidpb.UUID,
	eventEnvelopes *envelopepb.MultiEnvelope,
	root dogma.AggregateRoot,
) error {
	snapshot, hasSnapshot := t.marshalSnapshot(ctx, root)

	var (
		query strings.Builder
		args  []any
	)

	var snapshotArg any
	if hasSnapshot {
		snapshotArg = snapshot
	}

	args = append(
		args,
		xsql.UUID(t.MessageID),
		xsql.UUID(t.Identity.GetKey()),
		instanceID,
		xsql.UUID(streamID),
		xsql.UUID(eventEnvelopes.GetHeader().GetCorrelationId()),
		snapshotArg,
	)

	query.WriteString(`SELECT aggregate.complete_with_events($1, $2, $3, $4, $5, $6, ARRAY[`)

	first := true
	for eventEnvelope := range eventEnvelopes.All() {
		if first {
			first = false
		} else {
			query.WriteString(", ")
		}

		n := len(args)
		fmt.Fprintf(
			&query,
			"ROW($%d, $%d, $%d)::eventstream.event",
			n+1, n+2, n+3,
		)

		args = append(
			args,
			xsql.UUID(eventEnvelope.GetBody().GetMessageId()),
			xsql.UUID(eventEnvelope.GetBody().GetMessage().GetTypeId()),
			xsql.Envelope(eventEnvelope),
		)
	}

	query.WriteString(`])`)

	if _, err := t.Tx.ExecContext(ctx, query.String(), args...); err != nil {
		return fmt.Errorf("unable to complete command handling: %w", err)
	}

	return nil
}

// marshalSnapshot attempts to marshal the aggregate root's state as a binary
// snapshot. It returns false if marshaling is not supported or fails.
func (t *commandTask) marshalSnapshot(
	ctx context.Context,
	root dogma.AggregateRoot,
) ([]byte, bool) {
	var snapshot []byte

	if err := xerrors.ConvertPanicToError(
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

		return nil, false
	}

	return snapshot, true
}

func (t *commandTask) postpone(ctx context.Context) error {
	if _, err := t.Tx.ExecContext(
		ctx,
		`SELECT commandqueue.postpone($1, $2)`,
		xsql.UUID(t.MessageID),
		t.BackoffBase,
	); err != nil {
		return fmt.Errorf("unable to postpone queued command: %w", err)
	}

	return nil
}

func (t *commandTask) failAndPostpone(ctx context.Context, instanceID string) error {
	if instanceID != "" {
		// If the instance was newly created, delete it in case this command is
		// not routed to the same instance in the future.
		if _, err := t.Tx.ExecContext(
			ctx,
			`DELETE FROM aggregate.instances
			WHERE handler_key = $1
			AND instance_id = $2
			AND stream_id IS NULL`,
			xsql.UUID(t.Identity.GetKey()),
			instanceID,
		); err != nil {
			return fmt.Errorf("unable to delete aggregate instance: %w", err)
		}
	}

	if _, err := t.Tx.ExecContext(
		ctx,
		`SELECT commandqueue.fail_and_postpone($1, $2, $3)`,
		xsql.UUID(t.MessageID),
		t.BackoffBase,
		t.BackoffCap,
	); err != nil {
		return fmt.Errorf("unable to postpone queued command after failure: %w", err)
	}

	return nil
}
