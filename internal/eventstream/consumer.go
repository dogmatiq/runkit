package eventstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/failure"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"golang.org/x/sync/errgroup"
)

// Consumer dispatches events from a catalog's event streams to a [Sink].
//
// It consumes a stream only if this node owns the workload with the key that
// [Sink.RendezvousKey] returns for the stream. While nodes are joining or
// leaving, more than one node may briefly consume the same stream, so the sink
// must detect concurrent changes to its checkpoint offsets.
type Consumer struct {
	// Node is this engine's node in the cluster. The nodes it sees as live
	// determine which streams this node consumes.
	Node *cluster.Node

	// EventStreams is the catalog of event streams to consume.
	EventStreams *Catalog

	// Sink is the destination to which segments of each stream are dispatched.
	Sink Sink

	// EngineFailurePolicy is how the engine responds to failures to read the event streams,
	// and to checkpoint conflicts.
	EngineFailurePolicy failure.Policy

	// DispatchFailurePolicy is how the engine responds to failures of the sink.
	DispatchFailurePolicy failure.Policy

	// EngineLogger is the target for log messages produced by the consumer.
	EngineLogger *slog.Logger

	// stopStreamConsumer holds a function for each stream being consumed.
	// Calling it cancels consumption and waits for it to stop.
	stopStreamConsumer uuidcollection.Map[func()]

	// group runs the stream consumers. A fatal error in any one cancels
	// consumption of every stream.
	group *errgroup.Group
}

// Run consumes each event stream while this node owns the stream's workload,
// until ctx is canceled or it is unable to continue.
//
// It must be called at most once.
func (c *Consumer) Run(ctx context.Context) error {
	c.group, ctx = errgroup.WithContext(ctx)

	for {
		liveNodes, liveNodesChanged := c.Node.LiveNodes().Load()
		availableStreamIDs, availableStreamIDsChanged := c.EventStreams.StreamIDs().Load()

		streamIDs := uuidcollection.NewSet()
		for streamID := range availableStreamIDs.All() {
			key := c.Sink.RendezvousKey(streamID)
			if c.Node.OwnsWorkload(key, liveNodes) {
				streamIDs.Add(streamID)
			}
		}

		c.consumeStreams(ctx, streamIDs)

		select {
		case <-ctx.Done():
			if err := c.group.Wait(); err != nil {
				return err
			}

			return ctx.Err()
		case <-liveNodesChanged:
		case <-availableStreamIDsChanged:
		}
	}
}

// consumeStreams starts a stream consumer for each of the given streams that
// doesn't have one, and stops every other stream consumer.
func (c *Consumer) consumeStreams(
	ctx context.Context,
	streamIDs *uuidcollection.Set,
) {
	// Stopping a stream consumer waits for it to return, so a stream is never
	// consumed twice at once on this node.
	var stopping sync.WaitGroup

	for streamID, stop := range c.stopStreamConsumer.All() {
		if !streamIDs.Has(streamID) {
			stopping.Go(stop)
			c.stopStreamConsumer.Delete(streamID)
		}
	}

	stopping.Wait()

	for streamID := range streamIDs.All() {
		if !c.stopStreamConsumer.Has(streamID) {
			c.startStreamConsumer(ctx, streamID)
		}
	}
}

// startStreamConsumer starts a stream consumer for the stream with the given
// ID, which runs until ctx is canceled, it fails fatally, or it is stopped.
func (c *Consumer) startStreamConsumer(
	ctx context.Context,
	streamID *uuidpb.UUID,
) {
	engineLogger := c.EngineLogger.With(
		xslog.UUID("stream_id", streamID),
	)

	ctx, cancel := context.WithCancel(ctx)
	var stopped cosyne.Latch

	c.group.Go(func() error {
		defer stopped.Set()

		engineLogger.DebugContext(ctx, "started consuming event stream")
		defer engineLogger.DebugContext(ctx, "stopped consuming event stream")

		// sinkFailures tracks the failures of the sink, apart from failures to
		// read the stream and checkpoint conflicts, which [supervisor.Run]
		// tracks. Its streak ends as soon as the sink next handles a segment,
		// because that shows that the sink has recovered. A stream with no new
		// events leaves the streak open.
		sinkFailures := &failure.Tracker{
			Description:  "dispatch events",
			Policy:       c.DispatchFailurePolicy,
			EngineLogger: engineLogger,
		}
		defer sinkFailures.Complete()

		err := supervisor.Run(
			ctx,
			"consume event stream",
			c.EngineFailurePolicy,
			engineLogger,
			func(ctx context.Context) error {
				for {
					err := c.consume(ctx, streamID, sinkFailures)

					// A failure while the consumer is stopping isn't a failure
					// of the sink.
					if ctx.Err() != nil {
						return err
					}

					// The sink failed, so nothing is read from the stream
					// until the time that WorkFailed returns. Consumption then
					// resumes from the sink's checkpoint offset. A fatal error
					// is returned to Run, which doesn't retry it.
					if f, ok := errors.AsType[sinkFailure](err); ok && !xerrors.IsFatal(err) {
						nextAttemptAt := sinkFailures.WorkFailed(ctx, f.err)

						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(time.Until(nextAttemptAt)):
						}

						continue
					}

					// A stream that leaves the catalog, such as when the
					// remote engine it comes from is removed, is expected. Run
					// usually stops this stream consumer soon after, but the
					// stream may return first.
					if !errors.Is(err, ErrStreamNotFound) || ctx.Err() != nil {
						return err
					}

					engineLogger.DebugContext(
						ctx,
						"event stream left the catalog, waiting for it to return",
					)

					if err := c.awaitStream(ctx, streamID); err != nil {
						return err
					}
				}
			},
		)

		// Consumption returns the context's error once it is canceled, which
		// is not a failure of the stream consumer.
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return nil
		}

		return err
	})

	c.stopStreamConsumer.Set(streamID, func() {
		cancel()
		stopped.Wait()
	})
}

// consume dispatches the events of the stream with the given ID to the sink,
// beginning at the sink's checkpoint offset, until ctx is canceled or it is
// unable to continue.
//
// It returns a [sinkFailure] if the sink fails. Each time the sink handles a
// segment, it tells sinkFailures that the work succeeded.
func (c *Consumer) consume(
	ctx context.Context,
	streamID *uuidpb.UUID,
	sinkFailures *failure.Tracker,
) error {
	cp, err := c.Sink.CheckpointOffset(ctx, streamID)
	if err != nil {
		return sinkFailure{err}
	}

	return c.EventStreams.Consume(
		ctx,
		streamID,
		c.Sink.EventTypeIDs(),
		cp,
		func(seg Segment) error {
			var ok bool
			cp, ok, err = c.Sink.ConsumeSegment(ctx, cp, seg)
			if err != nil {
				return sinkFailure{
					fmt.Errorf(
						"unable to consume the segment of event stream %s that begins at offset %d: %w",
						streamID,
						seg.Begin,
						err,
					),
				}
			}

			// Another consumer changed the checkpoint offset, which isn't a
			// failure of the sink.
			if !ok {
				return fmt.Errorf(
					"checkpoint conflict on event stream %s",
					streamID,
				)
			}

			sinkFailures.WorkSucceeded(ctx)

			return nil
		},
	)
}

// awaitStream blocks until the catalog has the stream with the given ID, or
// ctx is canceled.
func (c *Consumer) awaitStream(ctx context.Context, streamID *uuidpb.UUID) error {
	for {
		streamIDs, changed := c.EventStreams.StreamIDs().Load()
		if streamIDs.Has(streamID) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// sinkFailure is an error that a [Sink] returned. The stream consumer uses it
// to tell the sink's failures from failures to read the stream, because
// [Catalog.Consume] returns the sink's error as it is.
type sinkFailure struct {
	err error
}

func (e sinkFailure) Error() string {
	return e.err.Error()
}

func (e sinkFailure) Unwrap() error {
	return e.err
}
