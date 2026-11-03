package eventstream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/messaginggrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RemoteCatalog is the [Catalog] of event streams recorded by a foreign
// application.
//
// Streams are never removed while the catalog runs, so each set returned by
// [RemoteCatalog.StreamIDs] is a superset of the sets returned before it.
//
// A subscription that is disconnected from the application reconnects from the
// checkpoint of the last batch it delivered.
//
// If the application can not record any of a subscription's message types, it
// delivers the existing events and then closes the stream. The subscription is
// not reported lost. Instead, the catalog asks the application again after a
// delay. It does the same if the application does not offer event streams.
type RemoteCatalog struct {
	// Client is the gRPC client used to discover the application's event
	// streams and consume their events.
	Client messaginggrpc.EventStreamConsumerAPIClient

	// RecheckInterval is how long the catalog waits before asking the
	// application again after it reports having nothing to offer. It must be
	// positive.
	RecheckInterval time.Duration

	// Logger is the target for log messages produced by the catalog.
	Logger *slog.Logger

	streamIDs xsync.Observable[*uuidcollection.Set]

	once       sync.Once
	sub, unsub chan *remoteSubscription

	done xsync.Latch
}

// remoteSubscription is a subscription to an event stream in a
// [RemoteCatalog].
type remoteSubscription struct {
	StreamID     *uuidpb.UUID
	MessageTypes *uuidcollection.Set
	Batches      chan<- Batch

	// Checkpoint is the offset from which the subscription resumes when it
	// reconnects. It is advanced only by the subscription's own goroutine.
	Checkpoint uint64

	// Lost is set when the catalog can no longer deliver to the subscription.
	Lost xsync.Latch

	// Cancel stops the subscription's goroutine, and Result holds its error
	// once it has returned.
	Cancel context.CancelFunc
	Result *xsync.Future[error]
}

// StreamIDs returns the IDs of the known event streams, and a channel that is
// closed when more streams become known.
//
// The returned set must not be modified.
func (c *RemoteCatalog) StreamIDs() (*uuidcollection.Set, <-chan struct{}) {
	return c.streamIDs.Load()
}

// Subscribe registers ch to receive events from a specific stream starting
// at the given checkpoint offset. Events are delivered in offset order.
//
// types is the set of message types to deliver. It must not be empty.
//
// lost is closed if the catalog can no longer deliver to the subscription.
func (c *RemoteCatalog) Subscribe(
	streamID *uuidpb.UUID,
	checkpoint uint64,
	types *uuidcollection.Set,
	ch chan<- Batch,
) (lost <-chan struct{}, unsubscribe func()) {
	if types.IsEmpty() {
		panic("message types must not be empty")
	}

	c.init()

	sub := &remoteSubscription{
		StreamID:     streamID,
		MessageTypes: types,
		Batches:      ch,
		Checkpoint:   checkpoint,
	}

	select {
	case <-c.done.Chan():
		return c.done.Chan(), func() {}
	case c.sub <- sub:
	}

	var once sync.Once

	return sub.Lost.Chan(), func() {
		once.Do(func() {
			select {
			case <-c.done.Chan():
			case c.unsub <- sub:
			}
		})
	}
}

func (c *RemoteCatalog) init() {
	c.once.Do(func() {
		c.sub = make(chan *remoteSubscription)
		c.unsub = make(chan *remoteSubscription)
	})
}

// Run discovers event streams and delivers events until ctx is canceled.
//
// It must be called at most once. Subscriptions are lost when it returns.
func (c *RemoteCatalog) Run(ctx context.Context) error {
	if c.RecheckInterval <= 0 {
		panic("recheck interval must be positive")
	}

	c.init()

	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return supervisor.Run(ctx, c.Logger, c.discover)
	})

	group.Go(func() error {
		return c.read(ctx)
	})

	return group.Wait()
}

// discover adds each event stream that the application reports to the set of
// known streams.
//
// If the server does not offer event streams, it asks again after the recheck
// interval.
func (c *RemoteCatalog) discover(ctx context.Context) error {
	reported := false

	for {
		err := c.listEventStreams(ctx)
		if status.Code(err) != codes.Unimplemented {
			return err
		}

		if !reported {
			c.Logger.InfoContext(
				ctx,
				"remote engine does not offer event streams",
			)
			reported = true
		}

		if err := c.waitToRecheck(ctx); err != nil {
			return err
		}
	}
}

// listEventStreams adds each event stream that the application reports to the
// set of known streams, until ctx is canceled or the server stops listing them.
//
// A stream with an invalid ID is ignored, so that it does not prevent the
// streams listed after it from being discovered.
func (c *RemoteCatalog) listEventStreams(ctx context.Context) error {
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
			c.Logger.WarnContext(
				ctx,
				"ignored event stream with invalid ID",
				xslog.Error(err),
			)
			continue
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

// read delivers events to subscriptions until ctx is canceled.
func (c *RemoteCatalog) read(ctx context.Context) error {
	defer c.done.Set()

	subscriptions := map[*remoteSubscription]struct{}{}

	for {
		select {
		case <-ctx.Done():
			c.done.Set()

			for sub := range subscriptions {
				sub.Result.Wait()
				sub.Lost.Set()
			}

			return ctx.Err()
		case sub := <-c.sub:
			subscriptions[sub] = struct{}{}

			logger := c.Logger.With(
				xslog.UUID("stream_id", sub.StreamID),
			)

			sub.Result, sub.Cancel = supervisor.Go(
				ctx,
				logger,
				func(ctx context.Context) error {
					return c.consume(ctx, sub)
				},
			)
		case sub := <-c.unsub:
			sub.Cancel()
			sub.Result.Wait()
			delete(subscriptions, sub)
		}
	}
}

// consume delivers events from the application to a subscription, starting at
// the subscription's checkpoint, until ctx is canceled or it is disconnected.
//
// Each time the application closes the stream, consume asks again after the
// recheck interval.
func (c *RemoteCatalog) consume(ctx context.Context, sub *remoteSubscription) error {
	for {
		if err := c.consumeStream(ctx, sub); err != nil {
			return err
		}

		if err := c.waitToRecheck(ctx); err != nil {
			return err
		}
	}
}

// consumeStream delivers events from the application to a subscription,
// starting at the subscription's checkpoint, until the application closes the
// stream.
//
// The application closes the stream once it has delivered every event of the
// requested types that it is able to record. If the application does not
// recognize the stream, or the checkpoint is beyond the end of the stream, its
// data does not match the subscription's, so the failure is persistent.
func (c *RemoteCatalog) consumeStream(ctx context.Context, sub *remoteSubscription) error {
	responses, err := c.Client.ConsumeEvents(
		ctx,
		messaginggrpc.NewConsumeEventsRequestBuilder().
			WithEventStreamId(sub.StreamID).
			WithCheckpointOffset(sub.Checkpoint).
			WithMessageTypeIds(slices.Collect(sub.MessageTypes.All())).
			Build(),
	)
	if err != nil {
		return fmt.Errorf("unable to consume events: %w", err)
	}

	for {
		res, err := responses.Recv()
		if err == io.EOF {
			c.Logger.DebugContext(
				ctx,
				"remote engine has no more events for subscription",
				xslog.UUID("stream_id", sub.StreamID),
			)
			return nil
		}

		if err != nil {
			switch status.Code(err) {
			case codes.NotFound, codes.OutOfRange:
				return supervisor.Persistentf("unable to receive events: %w", err)
			default:
				return fmt.Errorf("unable to receive events: %w", err)
			}
		}

		batch, err := newRemoteBatch(sub.StreamID, res)
		if err != nil {
			return supervisor.Persistent(err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case sub.Batches <- batch:
			sub.Checkpoint = batch.Checkpoint
		}
	}
}

// waitToRecheck waits for between half and all of the recheck interval, chosen
// at random so that rechecks that become due together are spread out, or until
// ctx is canceled.
func (c *RemoteCatalog) waitToRecheck(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(supervisor.Jitter(c.RecheckInterval)):
		return nil
	}
}

// newRemoteBatch returns a batch containing the events in a response from the
// application.
func newRemoteBatch(
	streamID *uuidpb.UUID,
	res *messaginggrpc.ConsumeEventsResponse,
) (Batch, error) {
	batch := Batch{
		StreamID:   streamID,
		Checkpoint: res.GetCheckpointOffset(),
	}

	for _, multi := range res.GetEnvelopes() {
		if err := multi.Validate(); err != nil {
			return Batch{}, fmt.Errorf("invalid envelope: %w", err)
		}

		for env := range multi.All() {
			_, ok, err := envelopepb.GetExtension[*envelopepb.EventStreamPosition](env)
			if err != nil {
				return Batch{}, fmt.Errorf("invalid event stream position: %w", err)
			}

			if !ok {
				return Batch{}, errors.New("event has no event stream position")
			}

			batch.Envelopes = append(batch.Envelopes, env)
		}
	}

	return batch, nil
}
