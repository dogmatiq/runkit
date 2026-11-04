package eventstream

import (
	"context"
	"errors"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream/internal/scanner"
	"github.com/dogmatiq/runkit/internal/supervisor"
)

// LegacyCatalog is a set of event streams and the events on them.
type LegacyCatalog interface {
	// StreamIDs returns the IDs of the known event streams, and a channel that
	// is closed when that set changes.
	//
	// The returned set must not be modified.
	StreamIDs() (*uuidcollection.Set, <-chan struct{})

	// Subscribe registers ch to receive events from a specific stream starting
	// at the given checkpoint offset. Events are delivered in offset order.
	//
	// types is the set of message types to deliver. It must not be empty.
	//
	// lost is closed if the catalog can no longer deliver to the subscription.
	Subscribe(
		streamID *uuidpb.UUID,
		checkpoint uint64,
		types *uuidcollection.Set,
		ch chan<- Batch,
	) (lost <-chan struct{}, unsubscribe func())
}

// Batch is a set of events delivered to a [LegacyCatalog] subscription, along with
// the checkpoint from which to resume afterwards.
//
// A batch may contain no events, indicating only that the checkpoint has
// advanced past events that are not of a subscribed type.
type Batch = scanner.Batch

// Legacy returns the catalog as a [LegacyCatalog].
func (c *Catalog) Legacy() LegacyCatalog {
	return legacyCatalog{c}
}

// legacyCatalog is a [Catalog] presented as a [LegacyCatalog].
type legacyCatalog struct {
	Catalog *Catalog
}

func (c legacyCatalog) StreamIDs() (*uuidcollection.Set, <-chan struct{}) {
	return c.Catalog.StreamIDs().Load()
}

// Subscribe consumes the stream on a new goroutine, sending each segment to
// ch as a batch.
//
// A failed read from the stream's source is retried, from the end of the last
// batch sent. The returned lost channel is closed once the catalog no longer
// has the stream.
func (c legacyCatalog) Subscribe(
	streamID *uuidpb.UUID,
	checkpoint uint64,
	types *uuidcollection.Set,
	ch chan<- Batch,
) (lost <-chan struct{}, unsubscribe func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	var gone xsync.Latch

	go func() {
		defer close(done)

		err := supervisor.Run(
			ctx,
			c.Catalog.Logger,
			func(ctx context.Context) error {
				err := c.Catalog.Consume(
					ctx,
					streamID,
					types,
					checkpoint,
					func(seg Segment) error {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case ch <- Batch{
							StreamID:   seg.StreamID,
							Envelopes:  seg.Envelopes,
							Checkpoint: seg.End,
						}:
							checkpoint = seg.End
							return nil
						}
					},
				)

				if errors.Is(err, ErrStreamNotFound) {
					return supervisor.Fatal(err)
				}

				return err
			},
		)

		if errors.Is(err, ErrStreamNotFound) {
			gone.Set()
		}
	}()

	return gone.Chan(), func() {
		cancel()
		<-done
	}
}
