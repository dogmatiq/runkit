package projection

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Consumer dispatches events from a catalog's event streams to a projection
// message handler.
//
// Each stream's workload is the responsibility of one of the live nodes in the
// cluster, chosen by rendezvous hashing, and that node consumes the stream.
// While nodes are joining or leaving, more than one node may briefly consume
// the same stream; the handler's optimistic concurrency control ensures each
// event is handled once.
type Consumer struct {
	// Node reports whether this node is responsible for each stream's workload.
	Node *cluster.Node

	// EventStreams is the catalog of event streams to consume.
	EventStreams *eventstream.Catalog

	// Handler is the projection message handler to which events are
	// dispatched.
	Handler dogma.ProjectionMessageHandler

	// Identity is the identity of the handler.
	Identity *identitypb.Identity

	// Concurrency is the handler's concurrency preference.
	Concurrency dogma.ConcurrencyPreference

	// EventTypeIDs is the set of IDs of the event types that the handler
	// consumes. It must not be empty.
	EventTypeIDs *uuidcollection.Set

	// Logger is the target for log messages produced by the consumer and its
	// stream consumers.
	Logger *slog.Logger

	handlerLock        *semaphore.Weighted
	stopStreamConsumer uuidcollection.Map[func()]
	group              *errgroup.Group
}

// Run consumes event streams until ctx is canceled or it is unable to continue.
//
// It must be called at most once.
func (c *Consumer) Run(ctx context.Context) error {
	if c.Concurrency == dogma.MinimizeConcurrency {
		c.handlerLock = semaphore.NewWeighted(1)
	}

	c.group, ctx = errgroup.WithContext(ctx)

	for {
		streamIDs, changed := c.EventStreams.StreamIDs().Load()

		// Stopping a stream consumer waits for it to return, so a stream that
		// leaves the catalog and comes back is never consumed twice at once on
		// this node.
		var stopping sync.WaitGroup

		for streamID, stop := range c.stopStreamConsumer.All() {
			if !streamIDs.Has(streamID) {
				stopping.Go(stop)
				c.stopStreamConsumer.Delete(streamID)
			}
		}

		stopping.Wait()

		for streamID := range streamIDs.All() {
			c.ensureStreamConsumer(ctx, streamID)
		}

		select {
		case <-ctx.Done():
			if err := c.group.Wait(); err != nil {
				return err
			}

			return ctx.Err()
		case <-changed:
		}
	}
}

// ensureStreamConsumer starts a stream consumer for the stream with the given
// ID, unless one is already running.
//
// The stream consumer runs until ctx is canceled, it fails, or it is stopped
// because the stream has left the catalog.
func (c *Consumer) ensureStreamConsumer(
	ctx context.Context,
	streamID *uuidpb.UUID,
) {
	if c.stopStreamConsumer.Has(streamID) {
		return
	}

	// workload is the rendezvous workload that determines which node consumes
	// the stream.
	workload := c.Identity.GetKey()

	if c.Concurrency != dogma.MinimizeConcurrency {
		// If the handler does not prefer to minimize concurrency, derive a
		// unique workload from both the handler identity and the stream ID such
		// that both handlers and streams are distributed evenly across nodes.
		workload = uuidpb.Derive(workload, streamID.AsString())
	}

	sc := &streamConsumer{
		StreamID:     streamID,
		Node:         c.Node,
		Workload:     workload,
		EventStreams: c.EventStreams,
		EventTypeIDs: c.EventTypeIDs,
		Handler:      c.Handler,
		HandlerLock:  c.handlerLock,
		Logger: c.Logger.With(
			xslog.UUID("stream_id", streamID),
		),
	}

	ctx, cancel := context.WithCancel(ctx)
	var stopped xsync.Latch

	c.group.Go(func() error {
		defer stopped.Set()

		err := sc.Run(ctx)

		// Run returns the context's error once the context is canceled, which
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
