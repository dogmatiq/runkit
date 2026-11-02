package eventstream

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/messaginggrpc"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xslog"
)

// RemoteCatalog is the [Catalog] of event streams recorded by a foreign
// application.
//
// Streams are never removed while the catalog runs, so each set returned by
// [RemoteCatalog.StreamIDs] is a superset of the sets returned before it.
type RemoteCatalog struct {
	// Client is the gRPC client used to discover the application's event
	// streams.
	Client messaginggrpc.EventStreamConsumerAPIClient

	// Logger is the target for log messages produced by the catalog.
	Logger *slog.Logger

	streamIDs xsync.Observable[*uuidcollection.Set]
}

// StreamIDs returns the IDs of the known event streams, and a channel that is
// closed when more streams become known.
//
// The returned set must not be modified.
func (c *RemoteCatalog) StreamIDs() (*uuidcollection.Set, <-chan struct{}) {
	return c.streamIDs.Load()
}

// Run discovers event streams until ctx is canceled.
//
// It must be called at most once.
func (c *RemoteCatalog) Run(ctx context.Context) error {
	return supervisor.Run(ctx, c.Logger, c.discover)
}

// discover adds each event stream that the application reports to the set of
// known streams.
func (c *RemoteCatalog) discover(ctx context.Context) error {
	responses, err := c.Client.ListEventStreams(
		ctx,
		messaginggrpc.NewListEventStreamsRequestBuilder().
			Build(),
	)
	if err != nil {
		return fmt.Errorf("unable to list event streams: %w", err)
	}

	for {
		res, err := responses.Recv()
		if err != nil {
			return fmt.Errorf("unable to receive event stream: %w", err)
		}

		streamID := res.GetEventStream().GetEventStreamId()

		if err := streamID.Validate(); err != nil {
			return supervisor.Persistentf("invalid event stream ID: %w", err)
		}

		known, _ := c.streamIDs.Load()

		if known.Has(streamID) {
			continue
		}

		c.Logger.DebugContext(
			ctx,
			"event stream discovered",
			xslog.UUID("stream_id", streamID),
		)

		streamIDs := known.Clone()
		if streamIDs == nil {
			streamIDs = uuidcollection.NewSet()
		}
		streamIDs.Add(streamID)

		c.streamIDs.Store(streamIDs)
	}
}
