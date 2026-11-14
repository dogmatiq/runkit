package remoteengine

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/messaginggrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/eventstream/source"
	"github.com/dogmatiq/runkit/internal/failure"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"google.golang.org/grpc"
)

// Connector runs a function that discovers remote engines, and connects the
// engine to each remote engine while it is available.
type Connector struct {
	// Discover adds and removes remote engines as they become available and
	// unavailable, as described by
	// [github.com/dogmatiq/runkit.RemoteEngineDiscoverer].
	Discover func(
		ctx context.Context,
		add func(description string, conn grpc.ClientConnInterface) (remove func()),
	) error

	// EventStreams is the catalog to which each available remote engine's
	// event streams are added.
	EventStreams *eventstream.Catalog

	// EventTypeIDs is the set of event types to deliver from each remote
	// engine.
	//
	// If it is empty, remote engines are not watched at all, as watching for
	// an empty set of types would deliver events of every type.
	EventTypeIDs *uuidcollection.Set

	// EngineFailurePolicy is how the engine responds to failures to discover remote engines.
	EngineFailurePolicy failure.Policy

	// EngineLogger is the target for log messages produced by the connector and its
	// connections to remote engines.
	EngineLogger *slog.Logger
}

// Run discovers remote engines until ctx is canceled.
func (c *Connector) Run(ctx context.Context) error {
	return supervisor.Run(
		ctx,
		"discover remote engines",
		c.EngineFailurePolicy,
		c.EngineLogger,
		c.discover,
	)
}

// discover calls Discover, watching each remote engine that it adds for event
// streams until the remote engine is removed or Discover returns.
//
// Discover is expected to run until ctx is canceled, so returning before then
// is a failure, even without an error.
func (c *Connector) discover(ctx context.Context) error {
	var watchers sync.WaitGroup
	defer watchers.Wait()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	add := func(description string, conn grpc.ClientConnInterface) func() {
		if conn == nil {
			panic("remote engine connection must not be nil")
		}

		if ctx.Err() != nil || c.EventTypeIDs.IsEmpty() {
			return func() {}
		}

		// Each remote engine is given an ID when it's added, so that its records
		// can be told apart even when two have the same description. A remote
		// engine that's removed and added again gets a new ID.
		src := &source.Remote{
			Client:      messaginggrpc.NewEventStreamConsumerAPIClient(conn),
			ID:          uuidpb.Generate(),
			Description: description,
		}

		engineLogger := c.EngineLogger.With(
			slog.String("event_stream_source", src.String()),
		)

		engineLogger.InfoContext(ctx, "remote engine added")

		ctx, cancel := context.WithCancel(ctx)
		var stopped cosyne.Latch

		// The catalog watches the source until ctx is canceled, retrying it
		// whenever it fails, so it has no other error to report.
		watchers.Go(func() {
			defer stopped.Set()
			c.EventStreams.Watch(ctx, src, c.EventTypeIDs)
		})

		return func() {
			cancel()
			<-stopped.Chan()

			engineLogger.InfoContext(
				context.WithoutCancel(ctx),
				"remote engine removed",
			)
		}
	}

	err := xerrors.Recover(func() error {
		return c.Discover(ctx, add)
	})

	if err == nil && ctx.Err() == nil {
		return errors.New("discoverer stopped before the context was canceled")
	}

	return err
}
