package eventstream_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/eventstream/source"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestConsumer(t *testing.T) {
	// setup starts a consumer of a new catalog, and returns the catalog, the
	// consumer's node, which is not yet running, and a channel that receives
	// the offset of each event that the sink consumes.
	setup := func(t *testing.T) (*Catalog, *cluster.Node, <-chan uint64) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)

		signals := &signal.Bus{
			DB:           db,
			EngineLogger: logger,
		}
		xtesting.GoE(t, signals.Run)

		node := &cluster.Node{
			ID:                uuidpb.Generate(),
			DB:                signals.DB,
			Signals:           signals,
			HeartbeatInterval: 20 * time.Millisecond,
			HeartbeatTimeout:  100 * time.Millisecond,
			EngineLogger:      logger,
		}

		catalog := &Catalog{
			CacheSize:    10,
			EngineLogger: logger,
		}

		handled := make(chan uint64, 10)

		var (
			m          sync.Mutex
			checkpoint uint64
		)

		sink := &sinkStub{
			CheckpointOffsetFunc: func(context.Context, *uuidpb.UUID) (uint64, error) {
				m.Lock()
				defer m.Unlock()

				return checkpoint, nil
			},
			ConsumeSegmentFunc: func(_ context.Context, cp uint64, seg Segment) (uint64, bool, error) {
				m.Lock()
				defer m.Unlock()

				if cp != checkpoint {
					return checkpoint, false, nil
				}

				for _, env := range seg.Envelopes {
					offset := envelopepb.MustGetExtension[*envelopepb.EventStreamPosition](env).GetOffset()
					checkpoint = offset + 1
					handled <- offset
				}

				return checkpoint, true, nil
			},
		}

		mt, _ := dogma.RegisteredMessageTypeOf(EventA1)
		sink.EventTypeIDsFunc = func() *uuidcollection.Set {
			return uuidcollection.NewSet(uuidpb.MustParse(mt.ID()))
		}
		consumer := &Consumer{
			Node:         node,
			EventStreams: catalog,
			Sink:         sink,
			EngineLogger: logger,
		}

		xtesting.GoE(t, consumer.Run)

		return catalog, node, handled
	}

	// watch watches the given source with the catalog until the returned
	// function is called.
	watch := func(
		t *testing.T,
		catalog *Catalog,
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

	t.Run("it consumes streams when this node owns their workloads", func(t *testing.T) {
		catalog, node, handled := setup(t)

		src := &source.Memory{}
		watch(t, catalog, src)

		streamID := uuidpb.Generate()
		src.Append(newSegment(t, streamID, 0, EventA1))
		expectStreamIDs(t, catalog, streamID)

		// A node doesn't own any workloads until it hears its own heartbeats,
		// after which it owns every workload, being the only node.
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
	Delivered cosyne.Latch
}

func (s *deliveringSource) String() string {
	return s.Source.String()
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
	catalog *Catalog,
	want ...*uuidpb.UUID,
) {
	t.Helper()

	expected := uuidcollection.NewSet(want...)

	xtesting.WaitForObservable(
		t,
		fmt.Sprintf("the catalog to have streams %s", expected),
		catalog.StreamIDs(),
		expected.IsEqual,
	)
}

// TestConsumer_partialSuccessReloadsDurableCheckpoint verifies that a failed
// segment is retried from the sink's stored checkpoint rather than the beginning
// of the segment or the offset returned with the error.
func TestConsumer_partialSuccessReloadsDurableCheckpoint(t *testing.T) {
	for _, failure := range []struct {
		name string
		err  error
	}{
		{"handling error", errors.New("later event failed")},
		{"checkpoint conflict", nil},
	} {
		t.Run(failure.name, func(t *testing.T) {
			node := newConsumerNode(t)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			logger := spruce.NewTestLogger(t)
			catalog := &Catalog{
				CacheSize:    10,
				EngineLogger: logger,
			}
			src := &source.Memory{}
			id := uuidpb.Generate()
			src.Append(Segment{
				StreamID: id,
				Begin:    0,
				End:      10,
			})

			watchDone := make(chan error, 1)
			go func() {
				watchDone <- catalog.Watch(ctx, src, nil)
			}()
			defer func() {
				cancel()
				<-watchDone
			}()

			for {
				ids, changed := catalog.StreamIDs().Load()
				if ids.Has(id) {
					break
				}

				select {
				case <-changed:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}

			var (
				mu      sync.Mutex
				durable uint64
				loads   []uint64
				calls   int
			)
			done := make(chan struct{})
			sink := &sinkStub{
				CheckpointOffsetFunc: func(context.Context, *uuidpb.UUID) (uint64, error) {
					mu.Lock()
					defer mu.Unlock()

					loads = append(loads, durable)

					return durable, nil
				},
				ConsumeSegmentFunc: func(_ context.Context, cp uint64, seg Segment) (uint64, bool, error) {
					mu.Lock()
					defer mu.Unlock()

					calls++
					if calls == 1 {
						if cp != 0 || seg.Begin != 0 {
							t.Errorf("initial checkpoint/segment = %d/%d", cp, seg.Begin)
						}

						// Earlier event transactions committed before the later failure.
						durable = 4

						return 0, false, failure.err
					}

					if cp != 4 || seg.Begin != 4 {
						t.Errorf("resumed checkpoint/segment = %d/%d, want 4/4", cp, seg.Begin)
					}

					close(done)

					// Empty/filtered segments need not advance a sink's durable checkpoint.
					return cp, true, nil
				},
			}

			c := &Consumer{
				EventStreams: catalog,
				Sink:         sink,
				EngineLogger: logger,
			}

			c.Node = node

			stopped := make(chan error, 1)
			go func() {
				stopped <- c.Run(ctx)
			}()

			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			cancel()
			if err := <-stopped; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}

			mu.Lock()
			defer mu.Unlock()

			if len(loads) != 2 || loads[0] != 0 || loads[1] != 4 {
				t.Fatalf("checkpoint loads = %v, want [0 4]", loads)
			}
		})
	}
}

// newConsumerNode starts a single-node cluster for the consumer tests.
func newConsumerNode(t *testing.T) *cluster.Node {
	t.Helper()

	logger := spruce.NewTestLogger(t)
	signals := &signal.Bus{
		DB:           xtesting.NewDatabase(t),
		EngineLogger: logger,
	}
	xtesting.GoE(t, signals.Run)
	node := &cluster.Node{
		ID:                uuidpb.Generate(),
		DB:                signals.DB,
		Signals:           signals,
		HeartbeatInterval: 20 * time.Millisecond,
		HeartbeatTimeout:  100 * time.Millisecond,
		EngineLogger:      logger,
	}
	xtesting.GoE(t, node.Run)

	return node
}
