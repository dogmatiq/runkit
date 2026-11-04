package projection

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"golang.org/x/sync/semaphore"
)

// streamConsumer consumes a single event stream, dispatching its events to a
// projection, on behalf of a [Consumer].
type streamConsumer struct {
	// StreamID is the ID of the stream from which events are consumed.
	StreamID *uuidpb.UUID

	// EventStreams is the catalog from which the stream is consumed.
	EventStreams *eventstream.Catalog

	// EventTypeIDs is the set of IDs of the event types that the handler
	// consumes.
	EventTypeIDs *uuidcollection.Set

	// Handler is the projection message handler to which events are
	// dispatched.
	Handler dogma.ProjectionMessageHandler

	// HandlerLock, if non-nil, is a semaphore with a weight of one that is
	// held while handling each event, so that stream consumers sharing it
	// handle events one at a time.
	HandlerLock *semaphore.Weighted

	// Logger is the target for log messages produced by the stream consumer
	// and the handler.
	Logger *slog.Logger
}

// Run consumes the stream until ctx is canceled or it fails fatally.
func (sc *streamConsumer) Run(ctx context.Context) error {
	sc.Logger.DebugContext(ctx, "started consuming event stream")
	defer sc.Logger.DebugContext(ctx, "stopped consuming event stream")

	return supervisor.Run(ctx, sc.Logger, sc.consume)
}

// consume dispatches events from the stream, resuming from the handler's own
// checkpoint offset, until ctx is canceled, a read from the stream fails, an
// event can not be dispatched, or the handler reports an optimistic
// concurrency conflict.
//
// A conflict is returned as an error so that consumption resumes from the
// handler's checkpoint offset after a delay, rather than immediately.
//
// It also fails if the catalog no longer has the stream, which happens only
// between the stream leaving the catalog and the [Consumer] noticing.
func (sc *streamConsumer) consume(ctx context.Context) error {
	checkpoint, err := xerrors.RecoverT(
		func() (uint64, error) {
			return sc.Handler.CheckpointOffset(ctx, sc.StreamID.AsString())
		},
	)
	if err != nil {
		return supervisor.Persistentf(
			"unable to load checkpoint offset: %w",
			err,
		)
	}

	return sc.EventStreams.Consume(
		ctx,
		sc.StreamID,
		sc.EventTypeIDs,
		checkpoint,
		func(seg eventstream.Segment) error {
			var err error
			checkpoint, err = sc.dispatchSegment(ctx, checkpoint, seg)
			return err
		},
	)
}

// dispatchSegment dispatches the events in a segment, and returns the
// handler's new checkpoint offset for the stream.
//
// checkpoint is the offset from which the handler last said it would resume
// handling events from the stream.
func (sc *streamConsumer) dispatchSegment(
	ctx context.Context,
	checkpoint uint64,
	seg eventstream.Segment,
) (uint64, error) {
	// The segment's end is past events that the handler does not consume, but
	// the handler's checkpoint advances only when it handles an event, so the
	// segment's end is not used.
	//
	// See https://github.com/dogmatiq/dogma/issues/221.
	for _, env := range seg.Envelopes {
		pos := envelopepb.MustGetExtension[*envelopepb.EventStreamPosition](env)
		offset := pos.GetOffset()

		next, err := sc.dispatch(ctx, offset, checkpoint, env)
		if err != nil {
			return checkpoint, err
		}

		if next != offset+1 {
			return checkpoint, fmt.Errorf(
				"optimistic concurrency conflict at offset %d: expected checkpoint offset %d, got %d",
				offset,
				offset+1,
				next,
			)
		}

		checkpoint = next
	}

	return checkpoint, nil
}

// dispatch passes the event at the given offset to the handler, and returns
// the handler's new checkpoint offset for the stream.
//
// checkpoint is the offset from which the handler last said it would resume
// handling events from the stream.
func (sc *streamConsumer) dispatch(
	ctx context.Context,
	offset, checkpoint uint64,
	env *envelopepb.Envelope,
) (uint64, error) {
	var event dogma.Event
	if err := xmessage.UnpackMessage(env, &event); err != nil {
		return 0, supervisor.Persistentf(
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
					streamID:         sc.StreamID.AsString(),
					offset:           offset,
					recordedAt:       env.GetBody().GetCreatedAt().AsTime(),
					checkpointOffset: checkpoint,
					logger: sc.Logger.With(
						slog.Uint64("offset", offset),
						xslog.Envelope("message", env),
					),
				},
				event,
			)
		},
	)
	if err != nil {
		return 0, supervisor.Persistentf(
			"unable to handle event at offset %d: %w",
			offset,
			err,
		)
	}

	return next, nil
}
