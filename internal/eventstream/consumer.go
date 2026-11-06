package eventstream

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"golang.org/x/sync/errgroup"
)

// Consumer coordinates consumption of catalog streams across cluster nodes.
//
// Each assigned stream runs under one supervisor. Sinks own durable progress
// and must tolerate overlapping consumption during cluster handoff.
type Consumer struct {
	// Node supplies cluster membership.
	Node *cluster.Node

	// EventStreams supplies available streams and segments.
	EventStreams *Catalog

	// Sink owns durable progress and segment dispatch.
	Sink Sink

	// EventTypeIDs must be nonempty and watched by the catalog.
	EventTypeIDs *uuidcollection.Set

	// Workload determines the rendezvous key for a stream. It must be non-nil
	// and return the same key for the same stream on every node.
	Workload func(*uuidpb.UUID) *uuidpb.UUID

	// Logger receives coordinator and per-stream lifecycle messages.
	Logger *slog.Logger

	stopStreamConsumer uuidcollection.Map[func()]
	group              *errgroup.Group
}

// Run consumes the event streams that this node is responsible for until ctx is
// canceled or it is unable to continue.
//
// It must be called at most once.
func (c *Consumer) Run(ctx context.Context) error {
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
		if membership.LocalNodeIsResponsibleFor(c.Workload(streamID)) {
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

// startStreamConsumer starts a stream consumer for the stream with the given
// ID, which runs until ctx is canceled, it fails fatally, or it is stopped.
func (c *Consumer) startStreamConsumer(
	ctx context.Context,
	streamID *uuidpb.UUID,
) {
	logger := c.Logger.With(
		xslog.UUID("stream_id", streamID),
	)

	ctx, cancel := context.WithCancel(ctx)
	var stopped cosyne.Latch

	c.group.Go(func() error {
		defer stopped.Set()

		logger.DebugContext(ctx, "started consuming event stream")
		defer logger.DebugContext(ctx, "stopped consuming event stream")

		err := supervisor.Run(
			ctx,
			logger,
			func(ctx context.Context) error {
				checkpoint, err := c.Sink.CheckpointOffset(ctx, streamID)
				if err != nil {
					return err
				}

				return c.EventStreams.Consume(
					ctx,
					streamID,
					c.EventTypeIDs,
					checkpoint,
					func(seg Segment) error {
						checkpoint, err = c.Sink.ConsumeSegment(ctx, checkpoint, seg)

						return err
					},
				)
			},
		)

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
