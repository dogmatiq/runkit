package process

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// EventSink is an [eventstream.Sink] that dispatches events to a
// [dogma.ProcessMessageHandler].
//
// It commits each event's effects and checkpoint offset in the same database
// transaction.
type EventSink struct {
	// DB is the database that stores process state and checkpoint offsets.
	DB *sql.DB

	// Handler is the process message handler to which events are dispatched.
	Handler dogma.ProcessMessageHandler[dogma.ProcessRoot]

	// Identity is the identity of the process handler.
	Identity *identitypb.Identity

	// Packer packs commands and deadlines produced by the handler.
	Packer *envelope.Packer

	// QueueRouter chooses the queues for commands and deadlines produced by the
	// handler.
	QueueRouter *messagequeue.Router

	// EventTypes is the set of IDs of the event types routed to the handler.
	EventTypes *uuidcollection.Set

	// OutboundMessageTypes contains the types the handler is allowed to produce.
	OutboundMessageTypes map[reflect.Type]struct{}

	// HandleTimeout is the maximum time the handler is given to handle each
	// event.
	HandleTimeout time.Duration

	// RouteTimeout is the maximum time the handler is given to route each
	// event to a process instance.
	RouteTimeout time.Duration

	// EngineLogger is the target for the engine's records about the sink and
	// the handler.
	EngineLogger *slog.Logger

	// AppLogger is the target for the application's records from the handler.
	AppLogger *slog.Logger
}

// RendezvousKey returns the rendezvous key that determines which node consumes
// the stream with the given ID.
func (s *EventSink) RendezvousKey(streamID *uuidpb.UUID) *uuidpb.UUID {
	return uuidpb.Derive(s.Identity.GetKey(), streamID.AsString())
}

// EventTypeIDs returns the event types routed to the process.
func (s *EventSink) EventTypeIDs() *uuidcollection.Set {
	return s.EventTypes
}

// CheckpointOffset returns the handler's checkpoint offset for the stream.
// Streams without a checkpoint are consumed from the beginning.
func (s *EventSink) CheckpointOffset(
	ctx context.Context,
	streamID *uuidpb.UUID,
) (uint64, error) {
	var cp uint64
	err := s.DB.QueryRowContext(
		ctx,
		`SELECT checkpoint_offset
		FROM eventstream.handler_checkpoints
		WHERE handler_key = $1
		AND stream_id = $2`,
		xsql.UUID(s.Identity.GetKey()),
		xsql.UUID(streamID),
	).Scan(&cp)
	if err == sql.ErrNoRows {
		return 0, nil
	}

	return cp, err
}

// ConsumeSegment dispatches events in offset order, committing their effects
// individually. It returns false if another consumer has advanced the checkpoint.
// Events excluded by routing or message type still advance the checkpoint.
func (s *EventSink) ConsumeSegment(
	ctx context.Context,
	cp uint64,
	seg eventstream.Segment,
) (uint64, bool, error) {
	for _, env := range seg.Envelopes {
		pos := envelopepb.MustGetExtension[*envelopepb.EventStreamPosition](env)
		offset := pos.GetOffset()
		if offset < cp {
			continue
		}

		next, ok, err := s.consume(ctx, seg.StreamID, cp, offset+1, env)
		if err != nil || !ok {
			return cp, ok, err
		}

		cp = next
	}

	if cp < seg.End {
		return s.consume(ctx, seg.StreamID, cp, seg.End, nil)
	}

	return cp, true, nil
}

// consume locks the handler's checkpoint before applying an event's effects.
// A nil envelope advances past a range with no events routed to the handler.
func (s *EventSink) consume(
	ctx context.Context,
	streamID *uuidpb.UUID,
	cp, next uint64,
	env *envelopepb.Envelope,
) (uint64, bool, error) {
	var (
		actual uint64
		flush  func(context.Context)
	)

	err := xsql.Transact(ctx, s.DB, func(ctx context.Context, tx *sql.Tx) error {
		// The upsert also locks a checkpoint that has not been created yet, so
		// overlapping first-time consumers cannot both commit the same effects.
		if err := tx.QueryRowContext(
			ctx,
			`INSERT INTO eventstream.handler_checkpoints (
				handler_key,
				stream_id
			)
			VALUES ($1, $2)
			ON CONFLICT (handler_key, stream_id) DO UPDATE SET
				checkpoint_offset = eventstream.handler_checkpoints.checkpoint_offset
			RETURNING checkpoint_offset`,
			xsql.UUID(s.Identity.GetKey()),
			xsql.UUID(streamID),
		).Scan(&actual); err != nil {
			return err
		}

		if actual != cp {
			return nil
		}

		if env != nil {
			engineLogger := s.EngineLogger.With(
				xslog.UUID("stream_id", streamID),
				slog.Uint64("offset", next-1),
				xslog.Envelope("handled_message", env),
			)

			// Each attempt of the transaction buffers its own application
			// records, which are written only once it commits.
			var appLogger *slog.Logger
			appLogger, flush = xslog.Buffered(
				s.AppLogger.With(
					xslog.Envelope("handled_message", env),
				),
			)

			if err := s.dispatch(ctx, tx, env, engineLogger, appLogger); err != nil {
				return err
			}
		}

		return xsql.ExecOne(
			ctx,
			tx,
			`UPDATE eventstream.handler_checkpoints SET
				checkpoint_offset = $3
			WHERE handler_key = $1
			AND stream_id = $2`,
			xsql.UUID(s.Identity.GetKey()),
			xsql.UUID(streamID),
			next,
		)
	})
	if err != nil {
		return cp, false, err
	}

	if actual != cp {
		return actual, false, nil
	}

	if flush != nil {
		flush(ctx)
	}

	return next, true, nil
}

// dispatch routes an event to its instance and applies its effects within tx.
func (s *EventSink) dispatch(
	ctx context.Context,
	tx *sql.Tx,
	env *envelopepb.Envelope,
	engineLogger *slog.Logger,
	appLogger *slog.Logger,
) error {
	messageID := env.GetBody().GetMessageId()

	var event dogma.Event

	// An event that can't be unmarshaled fails the same way however often it's
	// retried.
	if err := xmessage.UnpackMessage(
		env,
		&event,
	); err != nil {
		return supervisor.Deterministic(
			fmt.Errorf("unable to unmarshal event %s: %w", messageID, err),
		)
	}

	instanceID, ok, err := s.routeEventToInstance(ctx, event)
	if err != nil {
		return fmt.Errorf("unable to route event %s to an instance: %w", messageID, err)
	}

	if !ok {
		return nil
	}

	// Until the instance's root exists, a record can identify the instance only
	// by its ID.
	instanceIDAttr := slog.Group(
		"process_instance",
		slog.String("id", instanceID),
	)

	inst, err := lockInstance(
		ctx,
		tx,
		s.Handler,
		s.QueueRouter,
		s.Identity.GetKey(),
		instanceID,
		engineLogger.With(instanceIDAttr),
	)
	if err != nil {
		return err
	}

	// An ended instance doesn't handle any more events.
	if inst.Ended {
		return nil
	}

	scope := &eventScope{
		messageScope{
			instanceID: inst.ID,
			root:       inst.Root,
			commandPacker: s.Packer.PackEffects(
				env,
				s.Identity,
				envelope.WithInstanceID(inst.ID),
			),
			deadlinePacker: s.Packer.PackEffects(
				env,
				s.Identity,
				envelope.WithInstanceID(inst.ID),
			),
			engineLogger:         engineLogger,
			appLogger:            appLogger,
			outboundMessageTypes: s.OutboundMessageTypes,
		},
		env.GetBody().GetCreatedAt().AsTime(),
	}

	if err := xerrors.Recover(
		func() error {
			ctx, cancel := context.WithTimeout(ctx, s.HandleTimeout)
			defer cancel()

			return s.Handler.HandleEvent(ctx, inst.Root, scope, event)
		},
	); err != nil {
		return fmt.Errorf("unable to handle event %s for instance %q: %w", messageID, inst.ID, err)
	}

	return writeEffects(
		ctx,
		tx,
		s.QueueRouter,
		inst,
		&scope.messageScope,
		engineLogger.With(instanceIDAttr),
	)
}

// routeEventToInstance routes the event to a process instance by calling the
// handler's RouteEventToInstance() method.
func (s *EventSink) routeEventToInstance(
	ctx context.Context,
	event dogma.Event,
) (string, bool, error) {
	instanceID, err := xerrors.RecoverT(
		func() (string, error) {
			ctx, cancel := context.WithTimeout(ctx, s.RouteTimeout)
			defer cancel()

			instanceID, ok, err := s.Handler.RouteEventToInstance(ctx, event)
			if !ok || err != nil {
				return "", err
			}

			if instanceID == "" {
				return "", fmt.Errorf("handler returned empty instance ID")
			}

			return instanceID, nil
		},
	)
	if err != nil {
		return "", false, err
	}

	return instanceID, instanceID != "", nil
}
