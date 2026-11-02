package projection_test

import (
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/eventstream"
	. "github.com/dogmatiq/runkit/internal/projection"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestConsumer(t *testing.T) {
	setup := func(t *testing.T) *catalogStub {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)

		signals := &signal.Bus{
			DB:     db,
			Logger: logger,
		}
		xtesting.GoE(t, signals.Run)

		node := &cluster.Node{
			ID:       uuidpb.Generate(),
			Signals:  signals,
			Interval: 20 * time.Millisecond,
			Timeout:  100 * time.Millisecond,
			Logger:   logger,
		}
		xtesting.GoE(t, node.Run)

		catalog := &catalogStub{
			Subscribed:   make(chan *uuidpb.UUID, 10),
			Unsubscribed: make(chan *uuidpb.UUID, 10),
		}

		consumer := &Consumer{
			Node:         node,
			EventStreams: catalog,
			Handler:      &stubs.ProjectionMessageHandlerStub{},
			Identity:     identitypb.New("<handler>", uuidpb.Generate()),
			EventTypeIDs: uuidcollection.NewSet(uuidpb.Generate()),
			BufferSize:   1,
			Logger:       logger,
		}
		xtesting.GoE(t, consumer.Run)

		return catalog
	}

	t.Run("it stops consuming a stream that leaves the catalog", func(t *testing.T) {
		catalog := setup(t)
		streamID := uuidpb.Generate()

		catalog.Offer(streamID)
		xtesting.ExpectToReceiveN(t, "subscription", catalog.Subscribed, streamID)

		catalog.Offer()
		xtesting.ExpectToReceiveN(t, "unsubscription", catalog.Unsubscribed, streamID)
		xtesting.ExpectNoReceive(t, "subscription", catalog.Subscribed)
	})

	t.Run("it consumes a stream again when it returns to the catalog", func(t *testing.T) {
		catalog := setup(t)
		streamID := uuidpb.Generate()

		catalog.Offer(streamID)
		xtesting.ExpectToReceiveN(t, "subscription", catalog.Subscribed, streamID)

		catalog.Offer()
		xtesting.ExpectToReceiveN(t, "unsubscription", catalog.Unsubscribed, streamID)

		catalog.Offer(streamID)
		xtesting.ExpectToReceiveN(t, "subscription", catalog.Subscribed, streamID)
	})
}

// catalogStub is an [eventstream.Catalog] that offers the streams given to it
// by the test, and records subscriptions to them.
type catalogStub struct {
	streamIDs    xsync.Observable[*uuidcollection.Set]
	Subscribed   chan *uuidpb.UUID
	Unsubscribed chan *uuidpb.UUID
}

func (s *catalogStub) StreamIDs() (*uuidcollection.Set, <-chan struct{}) {
	return s.streamIDs.Load()
}

func (s *catalogStub) Subscribe(
	streamID *uuidpb.UUID,
	_ uint64,
	_ *uuidcollection.Set,
	_ chan<- eventstream.Batch,
) (<-chan struct{}, func()) {
	s.Subscribed <- streamID

	return nil, func() {
		s.Unsubscribed <- streamID
	}
}

// Offer replaces the set of streams that the catalog offers.
func (s *catalogStub) Offer(streamIDs ...*uuidpb.UUID) {
	s.streamIDs.Store(uuidcollection.NewSet(streamIDs...))
}
