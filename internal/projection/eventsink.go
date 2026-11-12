package projection

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
)

// EventSink is an [eventstream.Sink] that dispatches events to a
// [dogma.ProjectionMessageHandler].
//
// It reports the handler's checkpoint offset for each stream, and consumption of
// the stream resumes from that offset.
type EventSink struct {
	// Handler is the projection message handler to which events are
	// dispatched.
	Handler dogma.ProjectionMessageHandler

	// Identity is the identity of the projection handler.
	Identity *identitypb.Identity

	// Concurrency is the handler's concurrency preference.
	//
	// If it's [dogma.MinimizeConcurrency], [EventSink.RendezvousKey] returns the
	// same key for every stream, and the sink makes one call at a time to the
	// handler's HandleEvent method.
	Concurrency dogma.ConcurrencyPreference

	// EventTypes is the set of IDs of the event types routed to the handler.
	EventTypes *uuidcollection.Set

	// HandleTimeout is the maximum time the handler is given to handle each
	// event.
	HandleTimeout time.Duration

	// RouteTimeout is the maximum time the handler is given to decide from
	// where consumption of a stream resumes.
	RouteTimeout time.Duration

	// EngineLogger is the target for the engine's records about the sink and
	// the handler.
	EngineLogger *slog.Logger

	// AppLogger is the target for the application's records from the handler.
	AppLogger *slog.Logger

	// handlerLock is held during each call to the handler's HandleEvent method
	// when Concurrency is [dogma.MinimizeConcurrency].
	handlerLock cosyne.Mutex
}

// CheckpointOffset returns the handler's checkpoint offset for the stream
// with the given ID. An error or panic from the handler stops the current
// consumption attempt.
func (s *EventSink) CheckpointOffset(
	ctx context.Context,
	streamID *uuidpb.UUID,
) (uint64, error) {
	cp, err := xerrors.RecoverT(
		func() (uint64, error) {
			ctx, cancel := context.WithTimeout(ctx, s.RouteTimeout)
			defer cancel()

			return s.Handler.CheckpointOffset(ctx, streamID.AsString())
		},
	)
	if err != nil {
		return 0, fmt.Errorf(
			"unable to load checkpoint offset: %w",
			err,
		)
	}

	return cp, nil
}

// ConsumeSegment dispatches the events in a segment, and returns the
// handler's new checkpoint offset for the stream. It returns false if the
// handler reports an optimistic concurrency conflict.
//
// cp is the offset from which the handler last said it would resume
// handling events from the stream.
func (s *EventSink) ConsumeSegment(
	ctx context.Context,
	cp uint64,
	seg eventstream.Segment,
) (uint64, bool, error) {
	// The segment's end is past events that the handler does not consume, but
	// the handler's checkpoint advances only when it handles an event, so the
	// segment's end is not used.
	//
	// See https://github.com/dogmatiq/dogma/issues/221.
	for _, env := range seg.Envelopes {
		pos := envelopepb.MustGetExtension[*envelopepb.EventStreamPosition](env)
		offset := pos.GetOffset()

		next, err := s.dispatch(ctx, seg.StreamID, offset, cp, env)
		if err != nil {
			return cp, false, err
		}

		if next != offset+1 {
			return cp, false, nil
		}

		cp = next
	}

	return cp, true, nil
}

// dispatch passes the event at the given offset to the handler, and returns
// the handler's new checkpoint offset for the stream.
//
// cp is the offset from which the handler last said it would resume
// handling events from the stream.
func (s *EventSink) dispatch(
	ctx context.Context,
	streamID *uuidpb.UUID,
	offset, cp uint64,
	env *envelopepb.Envelope,
) (uint64, error) {
	var event dogma.Event
	if err := xmessage.UnpackMessage(env, &event); err != nil {
		return 0, fmt.Errorf(
			"unable to unmarshal event at offset %d: %w",
			offset,
			err,
		)
	}

	// The application's records are written only if the handler handles the
	// event, which it reports by advancing its checkpoint past it.
	appLogger, flush := xslog.Buffered(
		s.AppLogger.With(
			xslog.Envelope("handled_message", env),
		),
	)

	next, err := s.handleEvent(
		ctx,
		&eventScope{
			streamID:         streamID.AsString(),
			offset:           offset,
			recordedAt:       env.GetBody().GetCreatedAt().AsTime(),
			checkpointOffset: cp,
			engineLogger: s.EngineLogger.With(
				xslog.UUID("stream_id", streamID),
				slog.Uint64("offset", offset),
				xslog.Envelope("handled_message", env),
			),
			appLogger: appLogger,
		},
		event,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"unable to handle event at offset %d: %w",
			offset,
			err,
		)
	}

	if next == offset+1 {
		flush(ctx)
	}

	return next, nil
}

// handleEvent calls the handler's HandleEvent method, and returns the
// handler's new checkpoint offset for the stream.
//
// If Concurrency is [dogma.MinimizeConcurrency], it waits until no other call
// holds handlerLock, or until ctx is done, in which case it returns ctx's error
// without calling the handler.
func (s *EventSink) handleEvent(
	ctx context.Context,
	scope *eventScope,
	event dogma.Event,
) (uint64, error) {
	if s.Concurrency == dogma.MinimizeConcurrency {
		if err := s.handlerLock.Lock(ctx); err != nil {
			return 0, err
		}
		defer s.handlerLock.Unlock()
	}

	return xerrors.RecoverT(
		func() (uint64, error) {
			ctx, cancel := context.WithTimeout(ctx, s.HandleTimeout)
			defer cancel()

			return s.Handler.HandleEvent(ctx, scope, event)
		},
	)
}

// RendezvousKey returns the rendezvous key that determines which node consumes
// the stream with the given ID.
func (s *EventSink) RendezvousKey(streamID *uuidpb.UUID) *uuidpb.UUID {
	if s.Concurrency == dogma.MinimizeConcurrency {
		return s.Identity.GetKey()
	}

	// If the handler does not prefer to minimize concurrency, derive a unique
	// rendezvous key from both the handler identity and the stream ID such that
	// both handlers and streams are distributed evenly across nodes.
	return uuidpb.Derive(s.Identity.GetKey(), streamID.AsString())
}

// EventTypeIDs returns the event types routed to the projection.
func (s *EventSink) EventTypeIDs() *uuidcollection.Set {
	return s.EventTypes
}
