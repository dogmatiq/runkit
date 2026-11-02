package projection

import (
	"context"
	"log/slog"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Consumer dispatches events to a projection message handler, consuming each
// event stream whose workload this node is responsible for.
//
// Each stream's workload is the responsibility of one of the live nodes in the
// cluster, chosen by rendezvous hashing. While nodes are joining or leaving,
// more than one node may briefly consume the same stream; the handler's
// optimistic concurrency control ensures each event is handled once.
type Consumer struct {
	// Node reports whether this node is responsible for each stream's workload.
	Node *cluster.Node

	// EventStreams provides the set of event streams.
	EventStreams *eventstream.Catalog

	// Events delivers events from the event streams.
	Events *eventstream.Reader

	// Handler is the projection message handler to which events are
	// dispatched.
	Handler dogma.ProjectionMessageHandler

	// Identity is the identity of the handler.
	Identity *identitypb.Identity

	// Concurrency is the handler's concurrency preference.
	Concurrency dogma.ConcurrencyPreference

	// EventTypeIDs is the set of IDs of the event types that the handler
	// consumes. It must not be empty.
	EventTypeIDs *uuidpb.Set

	// BufferSize is the number of batches of events that each stream consumer
	// buffers while waiting to dispatch them. It must be positive.
	BufferSize int

	// Logger is the target for log messages produced by the consumer and its
	// stream consumers.
	Logger *slog.Logger

	handlerLock     *semaphore.Weighted
	streamConsumers uuidpb.Map[*streamConsumer]
	group           *errgroup.Group
}

// Run consumes event streams until ctx is canceled or it is unable to continue.
//
// It must be called at most once.
func (c *Consumer) Run(ctx context.Context) error {
	if c.BufferSize <= 0 {
		panic("buffer size must be positive")
	}

	if c.Concurrency == dogma.MinimizeConcurrency {
		c.handlerLock = semaphore.NewWeighted(1)
	}

	c.group, ctx = errgroup.WithContext(ctx)

	for {
		streamIDs, changed := c.EventStreams.StreamIDs()

		// Each stream has a single stream consumer for the life of the
		// consumer, so a stream is never consumed twice at once on this node.
		for streamID := range streamIDs.All() {
			c.ensureStreamConsumer(ctx, streamID)
		}

		select {
		case <-ctx.Done():
			return c.group.Wait()
		case <-changed:
		}
	}
}

// ensureStreamConsumer starts a stream consumer for the stream with the given
// ID, unless one is already running.
func (c *Consumer) ensureStreamConsumer(
	ctx context.Context,
	streamID *uuidpb.UUID,
) {
	if c.streamConsumers.Has(streamID) {
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
		Events:       c.Events,
		EventTypeIDs: c.EventTypeIDs,
		Handler:      c.Handler,
		HandlerLock:  c.handlerLock,
		BufferSize:   c.BufferSize,
		Logger: c.Logger.With(
			xslog.UUID("stream_id", streamID),
		),
	}

	c.streamConsumers.Set(streamID, sc)

	c.group.Go(func() error {
		return sc.Run(ctx)
	})
}
