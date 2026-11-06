package projection

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"golang.org/x/sync/semaphore"
)

// sink implements eventstream.Sink using a projection's own checkpoint storage.
type sink struct {
	Handler     dogma.ProjectionMessageHandler
	HandlerLock *semaphore.Weighted
	Logger      *slog.Logger
}

func (sc *sink) CheckpointOffset(ctx context.Context, streamID *uuidpb.UUID) (uint64, error) {
	checkpoint, err := xerrors.RecoverT(func() (uint64, error) {
		return sc.Handler.CheckpointOffset(ctx, streamID.AsString())
	})
	if err != nil {
		return 0, supervisor.Persistentf("unable to load checkpoint offset: %w", err)
	}
	return checkpoint, nil
}

// ConsumeSegment dispatches the events in a segment, and returns the
// handler's new checkpoint offset for the stream.
//
// checkpoint is the offset from which the handler last said it would resume
// handling events from the stream.
func (sc *sink) ConsumeSegment(
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

		next, err := sc.dispatch(ctx, seg.StreamID, offset, checkpoint, env)
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
func (sc *sink) dispatch(
	ctx context.Context,
	streamID *uuidpb.UUID,
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
		return 0, supervisor.Persistentf(
			"unable to handle event at offset %d: %w",
			offset,
			err,
		)
	}

	return next, nil
}
