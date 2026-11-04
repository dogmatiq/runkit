package projection_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/eventstream/source"
	. "github.com/dogmatiq/runkit/internal/projection"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestConsumer(t *testing.T) {
	// setup starts a consumer of a new catalog, and returns the catalog, the
	// consumer's node, which is not yet running, and a channel that receives
	// the offset of each event that the consumer's handler handles.
	setup := func(t *testing.T) (*eventstream.Catalog, *cluster.Node, <-chan uint64) {
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

		catalog := &eventstream.Catalog{
			CacheSize: 10,
			Logger:    logger,
		}

		handled := make(chan uint64, 10)

		var (
			m          sync.Mutex
			checkpoint uint64
		)

		handler := &ProjectionMessageHandlerStub{
			HandleEventFunc: func(
				_ context.Context,
				s dogma.ProjectionEventScope,
				_ dogma.Event,
			) (uint64, error) {
				m.Lock()
				defer m.Unlock()

				if s.CheckpointOffset() != checkpoint {
					return checkpoint, nil
				}

				checkpoint = s.Offset() + 1
				handled <- s.Offset()

				return checkpoint, nil
			},
			CheckpointOffsetFunc: func(context.Context, string) (uint64, error) {
				m.Lock()
				defer m.Unlock()

				return checkpoint, nil
			},
		}

		consumer := &Consumer{
			Node:         node,
			EventStreams: catalog,
			Handler:      handler,
			Identity:     identitypb.New("<handler>", uuidpb.Generate()),
			EventTypeIDs: uuidcollection.NewSet(MessageTypeUUID[*EventStub[TypeA]]()),
			Logger:       logger,
		}
		xtesting.GoE(t, consumer.Run)

		return catalog, node, handled
	}

	// watch watches the given source with the catalog until the returned
	// function is called.
	watch := func(
		t *testing.T,
		catalog *eventstream.Catalog,
		src source.Source,
	) func() {
		return xtesting.GoE(
			t,
			func(ctx context.Context) error {
				return catalog.Watch(ctx, src, nil)
			},
		)
	}

	t.Run("it consumes a stream that joins the catalog", func(t *testing.T) {
		catalog, node, handled := setup(t)
		xtesting.GoE(t, node.Run)

		src := &source.Memory{}
		watch(t, catalog, src)

		src.Append(newSegment(t, uuidpb.Generate(), 0, EventA1))

		xtesting.ExpectToReceiveN(t, "handled event", handled, 0)
	})

	t.Run("it consumes streams once this node is responsible for them", func(t *testing.T) {
		catalog, node, handled := setup(t)

		src := &source.Memory{}
		watch(t, catalog, src)

		streamID := uuidpb.Generate()
		src.Append(newSegment(t, streamID, 0, EventA1))
		expectStreamIDs(t, catalog, streamID)

		// A node is responsible for nothing until it hears its own heartbeats,
		// after which it is responsible for everything, being the only node.
		xtesting.ExpectNoReceive(t, "handled event", handled)

		xtesting.GoE(t, node.Run)
		xtesting.ExpectToReceiveN(t, "handled event", handled, 0)
	})

	t.Run("it consumes a stream again when it returns to the catalog", func(t *testing.T) {
		catalog, node, handled := setup(t)
		xtesting.GoE(t, node.Run)

		src := &source.Memory{}
		stop := watch(t, catalog, src)

		streamID := uuidpb.Generate()
		src.Append(newSegment(t, streamID, 0, EventA1))
		xtesting.ExpectToReceiveN(t, "handled event", handled, 0)

		stop()
		expectStreamIDs(t, catalog)

		src.Append(newSegment(t, streamID, 1, EventA2))

		watch(t, catalog, src)
		xtesting.ExpectToReceiveN(t, "handled event", handled, 1)
		xtesting.ExpectNoReceive(t, "handled event", handled)
	})

	t.Run("it continues consuming a stream when another source takes over", func(t *testing.T) {
		catalog, node, handled := setup(t)
		xtesting.GoE(t, node.Run)

		first := &source.Memory{}
		second := &deliveringSource{
			Source: &source.Memory{},
		}

		streamID := uuidpb.Generate()
		seg := newSegment(t, streamID, 0, EventA1)
		first.Append(seg)
		second.Source.Append(seg)

		stop := watch(t, catalog, first)
		xtesting.ExpectToReceiveN(t, "handled event", handled, 0)

		watch(t, catalog, second)
		xtesting.WaitForLatches(t, &second.Delivered)

		stop()

		seg = newSegment(t, streamID, 1, EventA2)
		first.Append(seg)
		second.Source.Append(seg)
		xtesting.ExpectToReceiveN(t, "handled event", handled, 1)
		xtesting.ExpectNoReceive(t, "handled event", handled)
	})
}

// deliveringSource is a [source.Memory] that reports when it has delivered a
// segment to a watcher.
type deliveringSource struct {
	Source *source.Memory

	// Delivered is set once a segment delivered by Watch has been handled.
	Delivered xsync.Latch
}

func (s *deliveringSource) Watch(
	ctx context.Context,
	types *uuidcollection.Set,
	fn func(source.Segment) error,
) error {
	return s.Source.Watch(
		ctx,
		types,
		func(seg source.Segment) error {
			if err := fn(seg); err != nil {
				return err
			}

			s.Delivered.Set()

			return nil
		},
	)
}

func (s *deliveringSource) Read(
	ctx context.Context,
	streamID *uuidpb.UUID,
	types *uuidcollection.Set,
	begin uint64,
	fn func(source.Segment) error,
) error {
	return s.Source.Read(ctx, streamID, types, begin, fn)
}

// newSegment returns a segment of a stream holding the given events, beginning
// at the given offset.
func newSegment(
	t *testing.T,
	streamID *uuidpb.UUID,
	begin uint64,
	events ...dogma.Event,
) source.Segment {
	t.Helper()

	seg := source.Segment{
		StreamID: streamID,
		Begin:    begin,
		End:      begin + uint64(len(events)),
	}

	for i, event := range events {
		mt, ok := dogma.RegisteredMessageTypeOf(event)
		if !ok {
			t.Fatalf("%T is not a registered message type", event)
		}

		data, err := event.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}

		env := envelopepb.NewEnvelopeBuilder().
			WithBody(
				envelopepb.NewBodyBuilder().
					WithCreatedAt(timestamppb.Now()).
					WithMessage(
						envelopepb.NewMessageBuilder().
							WithTypeId(uuidpb.MustParse(mt.ID())).
							WithDescription(event.MessageDescription()).
							WithData(data).
							Build(),
					).
					Build(),
			).
			Build()

		envelopepb.SetExtension(
			env.GetBody(),
			envelopepb.NewEventStreamPositionBuilder().
				WithStreamId(streamID).
				WithOffset(begin+uint64(i)).
				Build(),
		)

		seg.Envelopes = append(seg.Envelopes, env)
	}

	return seg
}

// expectStreamIDs waits until the catalog has exactly the given streams.
func expectStreamIDs(
	t *testing.T,
	catalog *eventstream.Catalog,
	want ...*uuidpb.UUID,
) {
	t.Helper()

	timeout := time.After(xtesting.WaitTimeout)

	for {
		got, changed := catalog.StreamIDs().Load()

		if got.IsEqual(uuidcollection.NewSet(want...)) {
			return
		}

		select {
		case <-timeout:
			t.Fatalf("timeout waiting for the catalog to have streams %v", want)
		case <-changed:
		}
	}
}
