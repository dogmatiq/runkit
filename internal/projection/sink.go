package projection

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
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

// NewSink returns a sink that dispatches events to the configured projection.
//
// It preserves the handler's concurrency preference across all of its streams.
func NewSink(
	handlerConfig *config.Projection,
	logger *slog.Logger,
) eventstream.Sink {
	var lock *semaphore.Weighted
	if handlerConfig.ConcurrencyPreference() == dogma.MinimizeConcurrency {
		lock = semaphore.NewWeighted(1)
	}

	types := uuidcollection.NewSet()
	inboundRoutes := handlerConfig.
		RouteSet().
		Filter(config.FilterByMessageDirection(config.InboundDirection)).
		Routes()

	for route := range inboundRoutes {
		types.Add(uuidpb.MustParse(route.MessageTypeID.Get()))
	}

	return &sink{
		Handler:      handlerConfig.Interface(),
		HandlerLock:  lock,
		Logger:       logger,
		identity:     handlerConfig.Identity(),
		concurrency:  handlerConfig.ConcurrencyPreference(),
		eventTypeIDs: types,
	}
}

// sink dispatches segments of event streams to a projection message handler.
// It implements [eventstream.Sink] using the handler's checkpoint offsets.
type sink struct {
	// Handler is the projection message handler to which events are
	// dispatched.
	Handler dogma.ProjectionMessageHandler

	// HandlerLock, if non-nil, is a semaphore with a weight of one that is
	// held while handling each event, so that streams are handled one event
	// at a time when the handler prefers to minimize concurrency.
	HandlerLock *semaphore.Weighted

	// Logger is the target for log messages produced by the sink and the
	// handler.
	Logger *slog.Logger

	// identity identifies the projection when deriving rendezvous workloads.
	identity *identitypb.Identity

	// concurrency determines whether streams share one rendezvous workload.
	concurrency dogma.ConcurrencyPreference

	// eventTypeIDs contains the IDs of events routed to the projection.
	eventTypeIDs *uuidcollection.Set
}

// CheckpointOffset returns the handler's checkpoint offset for the stream
// with the given ID. An error or panic from the handler stops the current
// consumption attempt.
func (sc *sink) CheckpointOffset(
	ctx context.Context,
	streamID *uuidpb.UUID,
) (uint64, error) {
	checkpoint, err := xerrors.RecoverT(
		func() (uint64, error) {
			return sc.Handler.CheckpointOffset(ctx, streamID.AsString())
		},
	)
	if err != nil {
		return 0, fmt.Errorf(
			"unable to load checkpoint offset: %w",
			err,
		)
	}

	return checkpoint, nil
}

// ConsumeSegment dispatches the events in a segment, and returns the
// handler's new checkpoint offset for the stream. It returns false if the
// handler reports an optimistic concurrency conflict.
//
// checkpoint is the offset from which the handler last said it would resume
// handling events from the stream.
func (sc *sink) ConsumeSegment(
	ctx context.Context,
	checkpoint uint64,
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

		next, err := sc.dispatch(ctx, seg.StreamID, offset, checkpoint, env)
		if err != nil {
			return checkpoint, false, err
		}

		if next != offset+1 {
			return checkpoint, false, nil
		}

		checkpoint = next
	}

	return checkpoint, true, nil
}

// dispatch passes the event at the given offset to the handler, and returns
// the handler's new checkpoint offset for the stream.
//
// checkpoint is the offset from which the handler last said it would resume
// handling events from the stream.
func (sc *sink) dispatch(
	ctx context.Context,
	streamID *uuidpb.UUID,
	offset, checkpoint uint64,
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

	if sc.HandlerLock != nil {
		if err := sc.HandlerLock.Acquire(ctx, 1); err != nil {
			return 0, err
		}
		defer sc.HandlerLock.Release(1)
	}

	next, err := xerrors.RecoverT(
		func() (uint64, error) {
			return sc.Handler.HandleEvent(
				ctx,
				&eventScope{
					streamID:         streamID.AsString(),
					offset:           offset,
					recordedAt:       env.GetBody().GetCreatedAt().AsTime(),
					checkpointOffset: checkpoint,
					logger: sc.Logger.With(
						xslog.UUID("stream_id", streamID),
						slog.Uint64("offset", offset),
						xslog.Envelope("message", env),
					),
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

	return next, nil
}

// Workload returns the rendezvous workload that determines which node consumes
// the stream with the given ID.
func (s *sink) Workload(streamID *uuidpb.UUID) *uuidpb.UUID {
	if s.concurrency == dogma.MinimizeConcurrency {
		return s.identity.GetKey()
	}

	// If the handler does not prefer to minimize concurrency, derive a unique
	// workload from both the handler identity and the stream ID such that both
	// handlers and streams are distributed evenly across nodes.
	return uuidpb.Derive(s.identity.GetKey(), streamID.AsString())
}

// EventTypeIDs returns the event types routed to the projection.
func (s *sink) EventTypeIDs() *uuidcollection.Set {
	return s.eventTypeIDs
}
