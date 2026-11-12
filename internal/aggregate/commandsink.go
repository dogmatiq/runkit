package aggregate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// CommandSink is a [messagequeue.Sink] that delivers commands to a
// [dogma.AggregateMessageHandler].
type CommandSink struct {
	// Handler is the aggregate message handler to which commands are
	// delivered.
	Handler dogma.AggregateMessageHandler[dogma.AggregateRoot]

	// Identity is the identity of the aggregate handler.
	Identity *identitypb.Identity

	// Packer packs events produced by the handler.
	Packer *envelope.Packer

	// OutboundMessageTypes contains the types the handler is allowed to produce.
	OutboundMessageTypes map[reflect.Type]struct{}

	// EngineLogger is the target for the engine's records about the sink and
	// the handler.
	EngineLogger *slog.Logger

	// AppLogger is the target for the application's records from the handler.
	AppLogger *slog.Logger
}

// Deliver delivers the command in the given envelope to the handler.
func (s *CommandSink) Deliver(
	ctx context.Context,
	tx *sql.Tx,
	d messagequeue.Delivery,
	env *envelopepb.Envelope,
) error {
	engineLogger := s.EngineLogger.With(
		xslog.Envelope("handled_message", env),
		slog.Uint64("failures", d.Failures),
	)

	var commandForRouting, commandForHandling dogma.Command

	if err := xmessage.UnpackMessage(
		env,
		&commandForRouting,
		&commandForHandling,
	); err != nil {
		engineLogger.ErrorContext(
			ctx,
			"unable to unmarshal command",
			xslog.Error(err),
		)
		return messagequeue.ErrFailed
	}

	instanceID, err := s.routeCommandToInstance(ctx, engineLogger, commandForRouting)
	if err != nil {
		return err
	}

	instanceLogger := engineLogger.With(
		slog.Group(
			"aggregate_instance",
			slog.String("id", instanceID),
		),
	)

	root, streamID, err := s.loadInstance(ctx, tx, instanceLogger, instanceID)
	if err != nil {
		return err
	}

	instanceLogger = engineLogger.With(
		slog.Group(
			"aggregate_instance",
			slog.String("id", instanceID),
			xslog.UUID("stream_id", streamID),
		),
	)

	packer := s.Packer.PackEffects(
		env,
		s.Identity,
		envelope.WithInstanceID(instanceID),
	)

	if err := xerrors.Recover(
		func() error {
			s.Handler.HandleCommand(
				root,
				&commandScope{
					instanceID: instanceID,
					root:       root,
					packer:     packer,
					appLogger: s.AppLogger.With(
						xslog.Envelope("handled_message", env),
					),
					outboundMessageTypes: s.OutboundMessageTypes,
				},
				commandForHandling,
			)
			return nil
		},
	); err != nil {
		instanceLogger.ErrorContext(
			ctx,
			"unable to handle command",
			xslog.Error(err),
		)

		return messagequeue.ErrFailed
	}

	if eventEnvelopes, ok := packer.Seal(); ok {
		return s.completeWithEvents(ctx, tx, instanceLogger, instanceID, streamID, eventEnvelopes, root)
	}

	return s.completeWithoutEvents(ctx, tx, instanceID)
}

// routeCommandToInstance routes the instance ID for the given command by
// calling the handler's RouteCommandToInstance() method.
func (s *CommandSink) routeCommandToInstance(
	ctx context.Context,
	logger *slog.Logger,
	commandForRouting dogma.Command,
) (string, error) {
	instanceID, err := xerrors.RecoverT(
		func() (string, error) {
			instanceID := s.Handler.RouteCommandToInstance(commandForRouting)
			if instanceID == "" {
				return "", errors.New("handler returned an empty instance ID")
			}
			return instanceID, nil
		},
	)
	if err != nil {
		logger.ErrorContext(
			ctx,
			"unable to route command to instance",
			xslog.Error(err),
		)

		return "", messagequeue.ErrFailed
	}

	return instanceID, nil
}

// loadInstance loads the aggregate instance with the given ID and returns its
// aggregate root and the ID of the event stream to which new events must be
// appended.
func (s *CommandSink) loadInstance(
	ctx context.Context,
	tx *sql.Tx,
	logger *slog.Logger,
	instanceID string,
) (
	root dogma.AggregateRoot,
	streamID *uuidpb.UUID,
	err error,
) {
	streamID, nextOffset, snapshot, exists, err := s.tryLockInstance(ctx, tx, logger, instanceID)
	if err != nil {
		return nil, nil, err
	}

	if !exists {
		streamID, nextOffset, snapshot, err = s.tryCreateInstance(ctx, tx, instanceID)
		if err != nil {
			return nil, nil, err
		}
	}

	root, err = s.newRoot(ctx, logger)
	if err != nil {
		return nil, nil, err
	}

	// If the instance has no stream binding there are no events to replay.
	if streamID == nil {
		return root, nil, nil
	}

	// If the instance has a snapshot, attempt to unmarshal it.
	if nextOffset != 0 {
		if !s.applySnapshot(ctx, logger, root, snapshot) {
			// We couldn't unmarshal the snapshot, so we just create a fresh root
			// and replay all of the instance's historical events.
			nextOffset = 0
			root, err = s.newRoot(ctx, logger)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	// Load events that were recorded after the snapshot was taken.
	if err := s.applyHistoricalEvents(
		ctx,
		tx,
		logger,
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
func (s *CommandSink) newRoot(
	ctx context.Context,
	logger *slog.Logger,
) (dogma.AggregateRoot, error) {
	root, err := xerrors.RecoverT(
		func() (dogma.AggregateRoot, error) {
			return s.Handler.New(), nil
		},
	)
	if err != nil {
		logger.ErrorContext(
			ctx,
			"unable to create aggregate root",
			xslog.Error(err),
		)

		return nil, messagequeue.ErrFailed
	}

	return root, nil
}

// tryLockInstance attempts to acquire a lock on the aggregate instance with the
// given ID.
//
// The instance may or may not already exist.
func (s *CommandSink) tryLockInstance(
	ctx context.Context,
	tx *sql.Tx,
	logger *slog.Logger,
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
	// with FOR UPDATE SKIP LOCKED so that the snapshot data is only sent across
	// the network if the lock is acquired successfully.
	row := tx.QueryRowContext(
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
		xsql.UUID(s.Identity.GetKey()),
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
		logger.DebugContext(
			ctx,
			"instance is locked by another transaction",
		)
		return nil, 0, nil, true, messagequeue.ErrBusy
	}

	if streamID.Validate() != nil {
		streamID = nil
	}

	return streamID, offset, snapshot, exists, nil
}

// tryCreateInstance attempts to create a new aggregate instance. If it already
// exists it returns the existing instance's data.
func (s *CommandSink) tryCreateInstance(
	ctx context.Context,
	tx *sql.Tx,
	instanceID string,
) (
	streamID *uuidpb.UUID,
	offset uint64,
	snapshot []byte,
	err error,
) {
	// If another transaction is racing to create the same instance we may
	// lose the race and block until it completes, in which case we must
	// honor the event stream binding that the other transaction
	// establishes, if any.
	row := tx.QueryRowContext(
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
		xsql.UUID(s.Identity.GetKey()),
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
func (s *CommandSink) applyHistoricalEvents(
	ctx context.Context,
	tx *sql.Tx,
	logger *slog.Logger,
	streamID *uuidpb.UUID,
	offset uint64,
	instanceID string,
	root dogma.AggregateRoot,
) error {
	rows, err := tx.QueryContext(
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
		xsql.UUID(s.Identity.GetKey()),
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

			return messagequeue.ErrFailed
		}

		if err := xerrors.Recover(
			func() error {
				root.ApplyEvent(eventForApply)
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

			return messagequeue.ErrFailed
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("unable to iterate historical events: %w", err)
	}

	return nil
}

func (s *CommandSink) applySnapshot(
	ctx context.Context,
	logger *slog.Logger,
	root dogma.AggregateRoot,
	snapshot []byte,
) bool {
	if err := xerrors.Recover(
		func() error {
			return root.UnmarshalBinary(snapshot)
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

// completeWithoutEvents deletes the aggregate instance after handling a
// command that recorded no events, if the instance has no event stream.
func (s *CommandSink) completeWithoutEvents(
	ctx context.Context,
	tx *sql.Tx,
	instanceID string,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`SELECT aggregate.complete_without_events($1, $2)`,
		xsql.UUID(s.Identity.GetKey()),
		instanceID,
	); err != nil {
		return fmt.Errorf("unable to complete command handling: %w", err)
	}

	return nil
}

// completeWithEvents appends events to the aggregate's event stream, and
// optionally persists a snapshot, in a single database round-trip.
func (s *CommandSink) completeWithEvents(
	ctx context.Context,
	tx *sql.Tx,
	logger *slog.Logger,
	instanceID string,
	streamID *uuidpb.UUID,
	eventEnvelopes *envelopepb.MultiEnvelope,
	root dogma.AggregateRoot,
) error {
	snapshot, hasSnapshot := s.marshalSnapshot(ctx, logger, root)

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
		xsql.UUID(s.Identity.GetKey()),
		instanceID,
		xsql.UUID(streamID),
		xsql.UUID(eventEnvelopes.GetHeader().GetCorrelationId()),
		snapshotArg,
	)

	query.WriteString(`SELECT aggregate.complete_with_events($1, $2, $3, $4, $5, ARRAY[`)

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

	if _, err := tx.ExecContext(ctx, query.String(), args...); err != nil {
		return fmt.Errorf("unable to complete command handling: %w", err)
	}

	return nil
}

// marshalSnapshot attempts to marshal the aggregate root's state as a binary
// snapshot. It returns false if marshaling is not supported or fails.
func (s *CommandSink) marshalSnapshot(
	ctx context.Context,
	logger *slog.Logger,
	root dogma.AggregateRoot,
) ([]byte, bool) {
	snapshot, err := xerrors.RecoverT(
		func() ([]byte, error) {
			return root.MarshalBinary()
		},
	)
	if err != nil {
		if !errors.Is(err, dogma.ErrNotSupported) {
			logger.ErrorContext(
				ctx,
				"unable to marshal snapshot",
				xslog.Error(err),
			)
		}

		return nil, false
	}

	return snapshot, true
}

// instanceLogAttr returns an attribute that describes the aggregate instance with
// the given ID and root, as it is now.
func instanceLogAttr(instanceID string, root dogma.AggregateRoot) slog.Attr {
	return slog.Group(
		"aggregate_instance",
		slog.String("id", instanceID),
		slog.String("description", root.AggregateInstanceDescription()),
	)
}
