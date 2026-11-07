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
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Sink dispatches segments of event streams to a process message handler.
// It implements [eventstream.Sink], committing each event's effects and
// checkpoint offset in the same database transaction.
type Sink struct {
	// DB is the database that stores process state and checkpoint offsets.
	DB *sql.DB

	// Handler is the process message handler to which events are dispatched.
	Handler dogma.ProcessMessageHandler[dogma.ProcessRoot]

	// Identity is the identity of the process handler.
	Identity *identitypb.Identity

	// Packer packs commands and deadlines produced by the handler.
	Packer *envelope.Packer

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

	// Logger is the target for log messages produced by the sink and handler.
	Logger *slog.Logger
}

// Workload returns the rendezvous workload that determines which node consumes
// the stream with the given ID.
func (s *Sink) Workload(streamID *uuidpb.UUID) *uuidpb.UUID {
	return uuidpb.Derive(s.Identity.GetKey(), streamID.AsString())
}

// EventTypeIDs returns the event types routed to the process.
func (s *Sink) EventTypeIDs() *uuidcollection.Set {
	return s.EventTypes
}

// CheckpointOffset returns the handler's checkpoint offset for the stream.
// Streams without a checkpoint are consumed from the beginning.
func (s *Sink) CheckpointOffset(
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
func (s *Sink) ConsumeSegment(
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
func (s *Sink) consume(
	ctx context.Context,
	streamID *uuidpb.UUID,
	cp, next uint64,
	env *envelopepb.Envelope,
) (uint64, bool, error) {
	var actual uint64
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
			logger := s.Logger.With(
				xslog.UUID("stream_id", streamID),
				slog.Uint64("offset", next-1),
				xslog.Envelope("message", env),
			)
			if err := s.dispatch(ctx, tx, env, logger); err != nil {
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

	return next, true, nil
}

// dispatch routes an event to its instance and applies its effects within tx.
func (s *Sink) dispatch(
	ctx context.Context,
	tx *sql.Tx,
	env *envelopepb.Envelope,
	logger *slog.Logger,
) error {
	var event dogma.Event

	if err := xmessage.UnpackMessage(
		env,
		&event,
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to unmarshal event",
			xslog.Error(err),
		)
		return fmt.Errorf("unable to unmarshal event: %w", err)
	}

	instanceID, ok, err := s.routeEventToInstance(ctx, logger, event)
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	instanceLogger := logger.With(
		slog.Group(
			"process_instance",
			slog.String("id", instanceID),
		),
	)

	root, err := newRoot(ctx, s.Handler, instanceLogger)
	if err != nil {
		return err
	}

	ok, err = loadInstance(
		ctx,
		tx,
		s.Identity.GetKey(),
		instanceID,
		root,
		instanceLogger,
	)
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	instanceLogger = logger.With(
		slog.Group(
			"process_instance",
			slog.String("id", instanceID),
			slog.String("description", root.ProcessInstanceDescription(false)),
		),
	)

	scope := &eventScope{
		messageScope{
			instanceID: instanceID,
			root:       root,
			commandPacker: s.Packer.PackEffects(
				env,
				s.Identity,
				envelope.WithInstanceID(instanceID),
			),
			deadlinePacker: s.Packer.PackEffects(
				env,
				s.Identity,
				envelope.WithInstanceID(instanceID),
			),
			logger:               instanceLogger,
			outboundMessageTypes: s.OutboundMessageTypes,
		},
		env.GetBody().GetCreatedAt().AsTime(),
	}

	if err := xerrors.Recover(
		func() error {
			ctx, cancel := context.WithTimeout(ctx, s.HandleTimeout)
			defer cancel()

			return s.Handler.HandleEvent(ctx, root, scope, event)
		},
	); err != nil {
		instanceLogger.ErrorContext(
			ctx,
			"unable to handle event",
			xslog.Error(err),
		)

		return fmt.Errorf("unable to handle event: %w", err)
	}

	if err := addCommandsToQueue(ctx, tx, scope.commandPacker); err != nil {
		return err
	}

	if scope.ended {
		if err := endInstance(
			ctx,
			tx,
			s.Identity.GetKey(),
			instanceID,
		); err != nil {
			return err
		}
	} else {
		if scope.mutated {
			if err := saveInstance(
				ctx,
				tx,
				s.Identity.GetKey(),
				instanceID,
				root,
				instanceLogger,
			); err != nil {
				return err
			}
		}

		if err := persistDeadlines(ctx, tx, scope.deadlinePacker); err != nil {
			return err
		}
	}

	return nil
}

// routeEventToInstance routes the event to a process instance by calling the
// handler's RouteEventToInstance() method.
func (s *Sink) routeEventToInstance(
	ctx context.Context,
	logger *slog.Logger,
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
		logger.ErrorContext(
			ctx,
			"unable to route event to instance",
			xslog.Error(err),
		)

		return "", false, fmt.Errorf("unable to route event to instance: %w", err)
	}

	return instanceID, instanceID != "", nil
}
