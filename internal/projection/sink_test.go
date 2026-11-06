package projection_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream"
	. "github.com/dogmatiq/runkit/internal/projection"
	"github.com/dogmatiq/spruce"
)

func TestNewSink(t *testing.T) {
	const handlerKey = "050415ad-ce90-496f-8987-40467e5415e0"

	for _, preference := range []dogma.ConcurrencyPreference{
		dogma.MinimizeConcurrency,
		dogma.MaximizeConcurrency,
	} {
		t.Run(fmt.Sprint(preference), func(t *testing.T) {
			handler := &ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("projection", handlerKey)
					c.Routes(dogma.HandlesEvent[*EventStub[TypeA]]())
					c.ConcurrencyPreference(preference)
				},
			}
			sink := NewSink(runtimeconfig.FromProjection(handler), spruce.NewTestLogger(t))

			mt, _ := dogma.RegisteredMessageTypeOf(EventA1)
			wantTypes := uuidcollection.NewSet(uuidpb.MustParse(mt.ID()))
			if !wantTypes.IsEqual(sink.EventTypeIDs()) {
				t.Fatalf("unexpected event types: %v", sink.EventTypeIDs())
			}

			for range 2 {
				streamID := uuidpb.Generate()
				want := uuidpb.MustParse(handlerKey)
				if preference == dogma.MaximizeConcurrency {
					want = uuidpb.Derive(want, streamID.AsString())
				}
				if got := sink.Workload(streamID); !got.Equal(want) {
					t.Fatalf("unexpected workload: got %s, want %s", got, want)
				}
			}
		})
	}
}

// TestSink_emptySegmentDoesNotAdvanceCheckpoint verifies that catalog progress
// across filtered events does not become the projection's checkpoint offset.
func TestSink_emptySegmentDoesNotAdvanceCheckpoint(t *testing.T) {
	handler := &ProjectionMessageHandlerStub{
		ConfigureFunc: func(c dogma.ProjectionConfigurer) {
			c.Identity("projection", "050415ad-ce90-496f-8987-40467e5415e0")
			c.Routes(dogma.HandlesEvent[*EventStub[TypeA]]())
		},
		CheckpointOffsetFunc: func(context.Context, string) (uint64, error) {
			return 4, nil
		},
	}
	sink := NewSink(runtimeconfig.FromProjection(handler), spruce.NewTestLogger(t))
	streamID := uuidpb.Generate()
	checkpoint, err := sink.CheckpointOffset(t.Context(), streamID)
	if err != nil {
		t.Fatal(err)
	}

	next, ok, err := sink.ConsumeSegment(t.Context(), checkpoint, eventstream.Segment{
		StreamID: streamID,
		Begin:    10,
		End:      20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || next != checkpoint {
		t.Fatalf("got checkpoint %d, ok %t; want %d, true", next, ok, checkpoint)
	}
}
