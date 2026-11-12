package aggregate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"reflect"

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

	inst, err := s.tryLockInstance(ctx, tx, instanceLogger, instanceID)
	if err != nil {
		return err
	}

	instanceLogger = engineLogger.With(
		slog.Group(
			"aggregate_instance",
			slog.String("id", inst.ID),
			xslog.UUID("stream_id", inst.StreamID),
		),
	)

	packer := s.Packer.PackEffects(
		env,
		s.Identity,
		envelope.WithInstanceID(inst.ID),
	)

	if err := xerrors.Recover(
		func() error {
			s.Handler.HandleCommand(
				inst.Root,
				&commandScope{
					instanceID: inst.ID,
					root:       inst.Root,
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
		return inst.RecordEvents(ctx, tx, instanceLogger, eventEnvelopes)
	}

	return inst.DeleteIfNoEvents(ctx, tx)
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

// tryLockInstance attempts to lock the aggregate instance with the given ID,
// creating it if it doesn't exist, and returns it with its root.
//
// It returns [messagequeue.ErrBusy] if another transaction holds the lock. It
// waits if another transaction is creating the instance. It returns
// [messagequeue.ErrFailed] if the handler can't create the root, or one of the
// instance's events can't be unmarshaled or applied to it.
func (s *CommandSink) tryLockInstance(
	ctx context.Context,
	tx *sql.Tx,
	logger *slog.Logger,
	instanceID string,
) (*instance, error) {
	// Lock or create the instance's row and fetch its data in one round-trip.
	//
	// If the row existed when the statement started, the locked CTE locks it
	// with FOR UPDATE SKIP LOCKED. It returns no rows if another transaction
	// holds the lock, so the row's data is sent only if tx acquires it.
	//
	// Otherwise, the created CTE inserts the row, and tx holds the lock on the
	// new row. If another transaction inserts the row after the statement
	// starts, the INSERT waits for that transaction to end. If it commits, ON
	// CONFLICT locks the row and returns the data that transaction stored,
	// including any event stream that it bound the instance to.
	//
	// So the statement returns no rows only if another transaction holds the
	// lock on an existing row.
	row := tx.QueryRowContext(
		ctx,
		`WITH existing AS (
			SELECT 1
			FROM aggregate.instances
			WHERE handler_key = $1
			AND instance_id = $2
		), locked AS (
			SELECT
				stream_id,
				snapshot_offset,
				snapshot
			FROM aggregate.instances
			WHERE handler_key = $1
			AND instance_id = $2
			FOR UPDATE SKIP LOCKED
		), created AS (
			INSERT INTO aggregate.instances (
				handler_key,
				instance_id
			)
			SELECT $1, $2
			WHERE NOT EXISTS (SELECT 1 FROM existing)
			ON CONFLICT (handler_key, instance_id) DO UPDATE SET
				instance_id = EXCLUDED.instance_id
			RETURNING
				stream_id,
				snapshot_offset,
				snapshot
		)
		SELECT
			stream_id,
			COALESCE(snapshot_offset + 1, 0),
			snapshot
		FROM locked
		UNION ALL
		SELECT
			stream_id,
			COALESCE(snapshot_offset + 1, 0),
			snapshot
		FROM created`,
		xsql.UUID(s.Identity.GetKey()),
		instanceID,
	)

	inst := &instance{
		HandlerKey: s.Identity.GetKey(),
		ID:         instanceID,
		StreamID:   &uuidpb.UUID{},
	}

	var snap snapshot

	if err := row.Scan(
		xsql.UUID(inst.StreamID),
		&snap.NextOffset,
		&snap.Data,
	); err != nil {
		// Another transaction holds the lock on the existing row.
		if errors.Is(err, sql.ErrNoRows) {
			logger.DebugContext(
				ctx,
				"instance is locked by another transaction",
			)
			return nil, messagequeue.ErrBusy
		}

		return nil, fmt.Errorf("unable to lock aggregate instance: %w", err)
	}

	// A NULL stream_id scans as the nil UUID, whose bits are all zero.
	if inst.StreamID.Equal(nil) {
		inst.StreamID = nil
	}

	root, err := s.newRoot(ctx, logger)
	if err != nil {
		return nil, err
	}
	inst.Root = root

	// An instance that isn't bound to a stream has no events to apply.
	if inst.StreamID == nil {
		return inst, nil
	}

	if snap.NextOffset != 0 {
		if !inst.ApplySnapshot(ctx, logger, snap) {
			// The snapshot can't be unmarshaled, so the instance's events are
			// applied to a new root from the start of its history instead.
			snap.NextOffset = 0
			inst.Root, err = s.newRoot(ctx, logger)
			if err != nil {
				return nil, err
			}
		}
	}

	// Apply the events that were recorded after the snapshot was taken.
	if err := inst.ApplyHistoricalEvents(ctx, tx, logger, snap.NextOffset); err != nil {
		return nil, err
	}

	return inst, nil
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

// instanceLogAttr returns an attribute that describes the aggregate instance with
// the given ID and root, as it is now.
func instanceLogAttr(instanceID string, root dogma.AggregateRoot) slog.Attr {
	return slog.Group(
		"aggregate_instance",
		slog.String("id", instanceID),
		slog.String("description", root.AggregateInstanceDescription()),
	)
}
