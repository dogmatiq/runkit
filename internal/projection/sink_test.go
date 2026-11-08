package projection_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream"
	. "github.com/dogmatiq/runkit/internal/projection"
	"github.com/dogmatiq/spruce"
)

func TestSink_workload(t *testing.T) {
	const handlerKey = "050415ad-ce90-496f-8987-40467e5415e0"

	for _, preference := range []dogma.ConcurrencyPreference{
		dogma.MinimizeConcurrency,
		dogma.MaximizeConcurrency,
	} {
		t.Run(fmt.Sprint(preference), func(t *testing.T) {
			sink := &Sink{
				Identity: identitypb.NewIdentityBuilder().
					WithKey(uuidpb.MustParse(handlerKey)).
					Build(),
				Concurrency: preference,
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
		CheckpointOffsetFunc: func(context.Context, string) (uint64, error) {
			return 4, nil
		},
	}
	sink := &Sink{
		Handler:      handler,
		RouteTimeout: time.Minute,
		EngineLogger: spruce.NewTestLogger(t),
	}
	streamID := uuidpb.Generate()
	cp, err := sink.CheckpointOffset(t.Context(), streamID)
	if err != nil {
		t.Fatal(err)
	}

	next, ok, err := sink.ConsumeSegment(t.Context(), cp, eventstream.Segment{
		StreamID: streamID,
		Begin:    10,
		End:      20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || next != cp {
		t.Fatalf("got cp %d, ok %t; want %d, true", next, ok, cp)
	}
}
