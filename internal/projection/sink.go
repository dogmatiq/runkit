package projection

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"golang.org/x/sync/semaphore"
)

// Sink dispatches segments of event streams to a projection message handler.
// It implements [eventstream.Sink] using the handler's checkpoint offsets.
type Sink struct {
	// Handler is the projection message handler to which events are
	// dispatched.
	Handler dogma.ProjectionMessageHandler

	// Identity is the identity of the projection handler.
	Identity *identitypb.Identity

	// Concurrency is the handler's concurrency preference.
	//
	// It determines whether [Sink.RendezvousKey] returns the same key for every
	// stream.
	Concurrency dogma.ConcurrencyPreference

	// EventTypes is the set of IDs of the event types routed to the handler.
	EventTypes *uuidcollection.Set

	// HandlerLock, if non-nil, is a semaphore with a weight of one that is
	// held while handling each event, so that streams are handled one event
	// at a time. It must be supplied when Concurrency is
	// [dogma.MinimizeConcurrency].
	HandlerLock *semaphore.Weighted

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
}

// CheckpointOffset returns the handler's checkpoint offset for the stream
// with the given ID. An error or panic from the handler stops the current
// consumption attempt.
func (s *Sink) CheckpointOffset(
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
func (s *Sink) ConsumeSegment(
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
func (s *Sink) dispatch(
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

	if s.HandlerLock != nil {
		if err := s.HandlerLock.Acquire(ctx, 1); err != nil {
			return 0, err
		}
		defer s.HandlerLock.Release(1)
	}

	// The application's records are written only if the handler handles the
	// event, which it reports by advancing its checkpoint past it.
	appLogger, flush := xslog.Buffered(
		s.AppLogger.With(
			xslog.Envelope("handled_message", env),
		),
	)

	next, err := xerrors.RecoverT(
		func() (uint64, error) {
			ctx, cancel := context.WithTimeout(ctx, s.HandleTimeout)
			defer cancel()

			return s.Handler.HandleEvent(
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
		},
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

// RendezvousKey returns the rendezvous key that determines which node consumes
// the stream with the given ID.
func (s *Sink) RendezvousKey(streamID *uuidpb.UUID) *uuidpb.UUID {
	if s.Concurrency == dogma.MinimizeConcurrency {
		return s.Identity.GetKey()
	}

	// If the handler does not prefer to minimize concurrency, derive a unique
	// rendezvous key from both the handler identity and the stream ID such that
	// both handlers and streams are distributed evenly across nodes.
	return uuidpb.Derive(s.Identity.GetKey(), streamID.AsString())
}

// EventTypeIDs returns the event types routed to the projection.
func (s *Sink) EventTypeIDs() *uuidcollection.Set {
	return s.EventTypes
}
