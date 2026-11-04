package eventstream_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/eventstream/source"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestCatalog_legacy(t *testing.T) {
	typesE := uuidcollection.NewSet(
		MessageTypeUUID[*EventStub[TypeE]](),
	)

	// setup returns a catalog presented as a legacy catalog, watching the
	// given source until the returned function is called.
	setup := func(t *testing.T, src source.Source) (LegacyCatalog, func()) {
		c := &Catalog{
			CacheSize: 10,
			Logger:    spruce.NewTestLogger(t),
		}

		stop := xtesting.GoE(
			t,
			func(ctx context.Context) error {
				return c.Watch(ctx, src, nil)
			},
		)

		return c.Legacy(), stop
	}

	t.Run("it reports the catalog's streams", func(t *testing.T) {
		src := &source.Memory{}
		streamID, _ := appendToMemory(src, nil, 0)

		catalog, _ := setup(t, src)

		expectStreamIDs(t, catalog, streamID)
	})

	t.Run("it delivers each segment as a batch whose checkpoint is the end of the segment", func(t *testing.T) {
		src := &source.Memory{}
		streamID, _ := appendToMemory(src, nil, 0)

		catalog, _ := setup(t, src)
		expectStreamIDs(t, catalog, streamID)

		batches := make(chan Batch, 10)
		lost, unsubscribe := catalog.Subscribe(streamID, 0, typesE, batches)
		defer unsubscribe()

		_, want := appendToMemory(src, streamID, 0, EventE1, EventX1, EventE2)

		checkpoint := expectToReceiveEnvelopes(t, batches, want[0], want[2])

		if checkpoint != 3 {
			t.Fatalf("unexpected checkpoint: got %d, want 3", checkpoint)
		}

		xtesting.ExpectNoReceive(t, "lost subscription", lost)
	})

	t.Run("it reports the subscription as lost when the catalog no longer has the stream", func(t *testing.T) {
		src := &source.Memory{}
		streamID, _ := appendToMemory(src, nil, 0)

		catalog, stop := setup(t, src)
		expectStreamIDs(t, catalog, streamID)

		lost, unsubscribe := catalog.Subscribe(streamID, 0, typesE, make(chan Batch, 10))
		defer unsubscribe()

		stop()

		xtesting.ExpectToReceiveN(t, "lost subscription", lost, struct{}{})
	})

	t.Run("it does not report the subscription as lost when it is unsubscribed", func(t *testing.T) {
		src := &source.Memory{}
		streamID, _ := appendToMemory(src, nil, 0)

		catalog, _ := setup(t, src)
		expectStreamIDs(t, catalog, streamID)

		lost, unsubscribe := catalog.Subscribe(streamID, 0, typesE, make(chan Batch, 10))
		unsubscribe()

		xtesting.ExpectNoReceive(t, "lost subscription", lost)
	})

	t.Run("it retries a failed read from the source", func(t *testing.T) {
		src := &source.Memory{
			SegmentSize: 1,
		}
		streamID, want := appendToMemory(src, nil, 0, EventE1, EventE2)

		catalog, _ := setup(
			t,
			&failFirstRead{
				Source: src,
			},
		)
		expectStreamIDs(t, catalog, streamID)

		batches := make(chan Batch, 10)
		_, unsubscribe := catalog.Subscribe(streamID, 0, typesE, batches)
		defer unsubscribe()

		expectToReceiveEnvelopes(t, batches, want...)
	})
}

// failFirstRead is a source whose first Read fails.
type failFirstRead struct {
	source.Source

	failed atomic.Bool
}

func (s *failFirstRead) Read(
	ctx context.Context,
	streamID *uuidpb.UUID,
	types *uuidcollection.Set,
	begin uint64,
	fn func(Segment) error,
) error {
	if s.failed.CompareAndSwap(false, true) {
		return errors.New("<error>")
	}

	return s.Source.Read(ctx, streamID, types, begin, fn)
}

// appendToMemory appends the given events to a stream in src, which is
// created with a new ID if streamID is nil, beginning at the given offset. It
// returns the stream's ID and the envelopes appended.
func appendToMemory(
	src *source.Memory,
	streamID *uuidpb.UUID,
	begin uint64,
	events ...dogma.Event,
) (*uuidpb.UUID, []*envelopepb.Envelope) {
	if streamID == nil {
		streamID = uuidpb.Generate()
	}

	seg := Segment{
		StreamID: streamID,
		Begin:    begin,
		End:      begin + uint64(len(events)),
	}

	for i, event := range events {
		mt, ok := dogma.RegisteredMessageTypeOf(event)
		if !ok {
			panic("unregistered message type")
		}

		env := envelopepb.NewEnvelopeBuilder().
			WithBody(
				envelopepb.NewBodyBuilder().
					WithMessage(
						envelopepb.NewMessageBuilder().
							WithTypeId(uuidpb.MustParse(mt.ID())).
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

	src.Append(seg)

	return streamID, seg.Envelopes
}
