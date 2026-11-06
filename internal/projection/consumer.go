package projection

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/uuidcollection"
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
	// Node is this engine's node in the cluster. Its membership determines
	// which streams this node consumes.
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

// Run consumes the event streams that this node is responsible for until ctx is
// canceled or it is unable to continue.
//
// It must be called at most once.
func (c *Consumer) Run(ctx context.Context) error {
	if c.Concurrency == dogma.MinimizeConcurrency {
		c.handlerLock = semaphore.NewWeighted(1)
	}

	c.group, ctx = errgroup.WithContext(ctx)

	for {
		streamIDs, streamIDsChanged := c.EventStreams.StreamIDs().Load()
		membership, membershipChanged := c.Node.Membership().Load()

		c.reconcile(ctx, streamIDs, membership)

		select {
		case <-ctx.Done():
			if err := c.group.Wait(); err != nil {
				return err
			}

			return ctx.Err()
		case <-streamIDsChanged:
		case <-membershipChanged:
		}
	}
}

// reconcile starts a stream consumer for each of the given streams that this
// node is responsible for in the given membership, and stops every other
// stream consumer.
func (c *Consumer) reconcile(
	ctx context.Context,
	streamIDs *uuidcollection.Set,
	membership *cluster.Membership,
) {
	responsible := uuidcollection.NewSet()

	for streamID := range streamIDs.All() {
		if membership.LocalNodeIsResponsibleFor(c.workload(streamID)) {
			responsible.Add(streamID)
		}
	}

	// Stopping a stream consumer waits for it to return, so a stream is never
	// consumed twice at once on this node.
	var stopping sync.WaitGroup

	for streamID, stop := range c.stopStreamConsumer.All() {
		if !responsible.Has(streamID) {
			stopping.Go(stop)
			c.stopStreamConsumer.Delete(streamID)
		}
	}

	stopping.Wait()

	for streamID := range responsible.All() {
		if !c.stopStreamConsumer.Has(streamID) {
			c.startStreamConsumer(ctx, streamID)
		}
	}
}

// workload returns the rendezvous workload that determines which node consumes
// the stream with the given ID.
func (c *Consumer) workload(streamID *uuidpb.UUID) *uuidpb.UUID {
	if c.Concurrency == dogma.MinimizeConcurrency {
		return c.Identity.GetKey()
	}

	// If the handler does not prefer to minimize concurrency, derive a unique
	// workload from both the handler identity and the stream ID such that both
	// handlers and streams are distributed evenly across nodes.
	return uuidpb.Derive(c.Identity.GetKey(), streamID.AsString())
}

// startStreamConsumer starts a stream consumer for the stream with the given
// ID, which runs until ctx is canceled, it fails fatally, or it is stopped.
func (c *Consumer) startStreamConsumer(
	ctx context.Context,
	streamID *uuidpb.UUID,
) {
	sc := &streamConsumer{
		StreamID:     streamID,
		EventStreams: c.EventStreams,
		EventTypeIDs: c.EventTypeIDs,
		Handler:      c.Handler,
		HandlerLock:  c.handlerLock,
		Logger: c.Logger.With(
			xslog.UUID("stream_id", streamID),
		),
	}

	ctx, cancel := context.WithCancel(ctx)
	var stopped cosyne.Latch

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
