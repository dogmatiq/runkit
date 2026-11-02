package eventstream_test

import (
	"strings"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

func TestCompositeCatalog_streamIDs(t *testing.T) {
	t.Run("it reports the streams offered by each catalog", func(t *testing.T) {
		composite := &CompositeCatalog{}
		xtesting.GoE(t, composite.Run)

		a := newCatalogStub()
		b := newCatalogStub()

		streamA := uuidpb.Generate()
		streamB := uuidpb.Generate()

		a.Offer(streamA)
		b.Offer(streamB)

		composite.Add(a)
		expectStreamIDs(t, composite, streamA)

		composite.Add(b)
		expectStreamIDs(t, composite, streamA, streamB)
	})

	t.Run("it reports streams that a catalog offers after it is added", func(t *testing.T) {
		composite := &CompositeCatalog{}
		xtesting.GoE(t, composite.Run)

		cat := newCatalogStub()
		composite.Add(cat)

		first := uuidpb.Generate()
		cat.Offer(first)
		expectStreamIDs(t, composite, first)

		second := uuidpb.Generate()
		cat.Offer(second)
		expectStreamIDs(t, composite, first, second)
	})

	t.Run("it removes a catalog's streams along with it", func(t *testing.T) {
		composite := &CompositeCatalog{}
		xtesting.GoE(t, composite.Run)

		a := newCatalogStub()
		b := newCatalogStub()

		streamA := uuidpb.Generate()
		streamB := uuidpb.Generate()

		a.Offer(streamA)
		b.Offer(streamB)

		removeA := composite.Add(a)
		composite.Add(b)
		expectStreamIDs(t, composite, streamA, streamB)

		removeA()
		expectStreamIDs(t, composite, streamB)
	})

	t.Run("it stops when a stream is offered by more than one catalog", func(t *testing.T) {
		composite := &CompositeCatalog{}

		result := make(chan error, 1)
		go func() {
			result <- composite.Run(t.Context())
		}()

		a := newCatalogStub()
		b := newCatalogStub()

		streamID := uuidpb.Generate()
		a.Offer(streamID)

		composite.Add(a)
		composite.Add(b)
		expectStreamIDs(t, composite, streamID)

		b.Offer(streamID)

		select {
		case <-time.After(xtesting.WaitTimeout):
			t.Fatal("timeout waiting for the composite to stop")
		case err := <-result:
			if err == nil || !strings.Contains(err.Error(), streamID.AsString()) {
				t.Fatalf("unexpected error: %v", err)
			}
		}
	})
}

func TestCompositeCatalog_subscribe(t *testing.T) {
	t.Run("it subscribes via the catalog that offers the stream", func(t *testing.T) {
		composite := &CompositeCatalog{}
		xtesting.GoE(t, composite.Run)

		a := newCatalogStub()
		b := newCatalogStub()

		streamA := uuidpb.Generate()
		streamB := uuidpb.Generate()

		a.Offer(streamA)
		b.Offer(streamB)

		composite.Add(a)
		composite.Add(b)
		expectStreamIDs(t, composite, streamA, streamB)

		types := uuidcollection.NewSet(uuidpb.Generate())
		batches := make(chan Batch, 10)

		lost, unsubscribe := composite.Subscribe(streamB, 3, types, batches)

		sub := expectSubscription(t, b, streamB, 3, types)
		xtesting.ExpectNoReceive(t, "subscription", a.Subscriptions)

		if sub.Batches != chan<- Batch(batches) {
			t.Fatal("unexpected channel")
		}

		xtesting.ExpectNoReceive(t, "lost subscription", lost)

		unsubscribe()
		xtesting.ExpectToReceiveN(t, "unsubscription", sub.Unsubscribed, struct{}{})
	})

	t.Run("it reports a subscription as lost if no catalog offers the stream", func(t *testing.T) {
		composite := &CompositeCatalog{}
		xtesting.GoE(t, composite.Run)

		cat := newCatalogStub()
		composite.Add(cat)

		lost, unsubscribe := composite.Subscribe(
			uuidpb.Generate(),
			0,
			uuidcollection.NewSet(uuidpb.Generate()),
			make(chan Batch, 10),
		)
		defer unsubscribe()

		xtesting.ExpectToReceiveN(t, "lost subscription", lost, struct{}{})
		xtesting.ExpectNoReceive(t, "subscription", cat.Subscriptions)
	})

	t.Run("it reports a subscription as lost when the offering catalog does", func(t *testing.T) {
		composite := &CompositeCatalog{}
		xtesting.GoE(t, composite.Run)

		streamID := uuidpb.Generate()
		types := uuidcollection.NewSet(uuidpb.Generate())

		cat := newCatalogStub()
		cat.Offer(streamID)
		composite.Add(cat)
		expectStreamIDs(t, composite, streamID)

		lost, unsubscribe := composite.Subscribe(streamID, 0, types, make(chan Batch, 10))
		defer unsubscribe()

		sub := expectSubscription(t, cat, streamID, 0, types)
		close(sub.Lost)

		xtesting.ExpectToReceiveN(t, "lost subscription", lost, struct{}{})
	})
}

// catalogStub is a [Catalog] that offers the streams given to it by the test,
// and records the subscriptions made to it.
type catalogStub struct {
	streamIDs     xsync.Observable[*uuidcollection.Set]
	Subscriptions chan subscription
}

// subscription is a subscription made to a [catalogStub].
type subscription struct {
	StreamID     *uuidpb.UUID
	Checkpoint   uint64
	Types        *uuidcollection.Set
	Batches      chan<- Batch
	Unsubscribed chan struct{}

	// Lost is closed by the test to report the subscription as lost.
	Lost chan struct{}
}

func (s *catalogStub) StreamIDs() (*uuidcollection.Set, <-chan struct{}) {
	return s.streamIDs.Load()
}

func (s *catalogStub) Subscribe(
	streamID *uuidpb.UUID,
	checkpoint uint64,
	types *uuidcollection.Set,
	ch chan<- Batch,
) (<-chan struct{}, func()) {
	sub := subscription{
		StreamID:     streamID,
		Checkpoint:   checkpoint,
		Types:        types,
		Batches:      ch,
		Unsubscribed: make(chan struct{}, 1),
		Lost:         make(chan struct{}),
	}

	s.Subscriptions <- sub

	return sub.Lost, func() {
		sub.Unsubscribed <- struct{}{}
	}
}

// expectSubscription waits for a subscription to be made to the stub, and
// asserts that it is for the given stream, checkpoint and message types.
func expectSubscription(
	t testing.TB,
	stub *catalogStub,
	streamID *uuidpb.UUID,
	checkpoint uint64,
	types *uuidcollection.Set,
) subscription {
	t.Helper()

	var sub subscription

	select {
	case <-time.After(xtesting.WaitTimeout):
		t.Fatal("timeout waiting for subscription")
	case sub = <-stub.Subscriptions:
	}

	if !sub.StreamID.Equal(streamID) {
		t.Fatalf("unexpected stream ID: got %s, want %s", sub.StreamID, streamID)
	}

	if sub.Checkpoint != checkpoint {
		t.Fatalf("unexpected checkpoint: got %d, want %d", sub.Checkpoint, checkpoint)
	}

	if sub.Types != types {
		t.Fatal("unexpected message types")
	}

	return sub
}

func newCatalogStub() *catalogStub {
	return &catalogStub{
		Subscriptions: make(chan subscription, 10),
	}
}

// Offer adds a stream to the catalog.
func (s *catalogStub) Offer(streamID *uuidpb.UUID) {
	prev, _ := s.streamIDs.Load()

	next := prev.Clone()
	if next == nil {
		next = uuidcollection.NewSet()
	}
	next.Add(streamID)

	s.streamIDs.Store(next)
}
