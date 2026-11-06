package projection

import (
	"context"
	"log/slog"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/eventstream"
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
}

// Run consumes the streams assigned to this handler until cancellation.
// It must be called at most once.
func (c *Consumer) Run(ctx context.Context) error {
	var lock *semaphore.Weighted
	if c.Concurrency == dogma.MinimizeConcurrency {
		lock = semaphore.NewWeighted(1)
	}
	consumer := &eventstream.Consumer{
		Node:         c.Node,
		EventStreams: c.EventStreams,
		Sink:         &sink{Handler: c.Handler, HandlerLock: lock, Logger: c.Logger},
		EventTypeIDs: c.EventTypeIDs,
		Workload:     c.workload,
		Logger:       c.Logger,
	}
	return consumer.Run(ctx)
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
