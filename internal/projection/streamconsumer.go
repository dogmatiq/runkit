package projection

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
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

	// Node reports whether this node is responsible for the stream's workload.
	Node *cluster.Node

	// Workload is the rendezvous workload that determines which node consumes
	// the stream.
	Workload *uuidpb.UUID

	// EventStreams is the catalog that offers the stream.
	EventStreams eventstream.Catalog

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

	// BufferSize is the number of batches of events that are buffered while
	// waiting to be dispatched.
	BufferSize int

	// Logger is the target for log messages produced by the stream consumer
	// and the handler.
	Logger *slog.Logger
}

// Run consumes the stream while this node is responsible for its workload,
// until ctx is canceled.
func (sc *streamConsumer) Run(ctx context.Context) error {
	responsible, unobserve := sc.Node.ObserveResponsibility(sc.Workload)
	defer unobserve()

	for {
		if err := sc.step(ctx, responsible); err != nil {
			return err
		}
	}
}

// step runs the stream consumer until ctx is canceled or its responsibility
// changes, consuming the stream if this node is responsible for its workload.
//
// responsible reports whether this node is responsible for the stream's
// workload, and returns a channel that is closed when that changes.
func (sc *streamConsumer) step(
	ctx context.Context,
	responsible func() (bool, <-chan struct{}),
) error {
	isResponsible, responsibilityChanged := responsible()

	var (
		consumerResult  *xsync.Future[error]
		consumerStopped <-chan struct{}
	)

	if isResponsible {
		sc.Logger.DebugContext(ctx, "started consuming event stream")

		var cancel context.CancelFunc
		consumerResult, cancel = supervisor.Go(ctx, sc.Logger, sc.consume)
		consumerStopped = consumerResult.Ready()

		defer func() {
			cancel()
			consumerResult.Wait()

			sc.Logger.DebugContext(ctx, "stopped consuming event stream")
		}()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-responsibilityChanged:
		return nil
	case <-consumerStopped:
		return consumerResult.Wait()
	}
}

// consume dispatches events from the stream, resuming from the handler's own
// checkpoint offset, until ctx is canceled, an event can not be dispatched, the
// handler reports an optimistic concurrency conflict, or the subscription to
// the stream is lost.
//
// A conflict or a lost subscription is returned as an error so that consumption
// resumes from the handler's checkpoint offset after a delay, rather than
// immediately.
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

	batches := make(chan eventstream.Batch, sc.BufferSize)
	lost, unsubscribe := sc.EventStreams.Subscribe(
		sc.StreamID,
		checkpoint,
		sc.EventTypeIDs,
		batches,
	)
	defer unsubscribe()

	for {
		var batch eventstream.Batch

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-lost:
			return errors.New("subscription to event stream lost")
		case batch = <-batches:
		}

		// The batch's own checkpoint advances past events that the handler
		// does not consume, but the handler's checkpoint advances only when
		// it handles an event, so the batch's checkpoint is not used.
		//
		// See https://github.com/dogmatiq/dogma/issues/221.
		for _, env := range batch.Envelopes {
			pos := envelopepb.MustGetExtension[*envelopepb.EventStreamPosition](env)
			offset := pos.GetOffset()

			next, err := sc.dispatch(ctx, offset, checkpoint, env)
			if err != nil {
				return err
			}

			if next != offset+1 {
				return fmt.Errorf(
					"optimistic concurrency conflict at offset %d: expected checkpoint offset %d, got %d",
					offset,
					offset+1,
					next,
				)
			}

			checkpoint = next
		}
	}
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
