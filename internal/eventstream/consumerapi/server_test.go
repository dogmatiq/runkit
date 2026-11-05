package consumerapi_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/dogmatiq/dapper"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/messaginggrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// consumerAPIApp returns the handler route used by the server tests. The
// application records events of types A, B and C in response to commands of
// the same types.
func consumerAPIApp() dogma.HandlerRoute {
	return dogma.ViaIntegration(
		&stubs.IntegrationMessageHandlerStub{
			ConfigureFunc: func(c dogma.IntegrationConfigurer) {
				c.Identity("<handler>", "4646ab53-38cf-49e4-9cd8-39c1cd39588f")
				c.Routes(
					dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
					dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeB]](),
					dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeC]](),
					dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					dogma.RecordsEvent[*stubs.EventStub[stubs.TypeB]](),
					dogma.RecordsEvent[*stubs.EventStub[stubs.TypeC]](),
				)
			},
			HandleCommandFunc: func(
				_ context.Context,
				s dogma.IntegrationCommandScope,
				m dogma.Command,
			) error {
				switch m.(type) {
				case *stubs.CommandStub[stubs.TypeA]:
					s.RecordEvent(stubs.EventA1)
				case *stubs.CommandStub[stubs.TypeB]:
					s.RecordEvent(stubs.EventB1)
				case *stubs.CommandStub[stubs.TypeC]:
					s.RecordEvent(stubs.EventC1)
				default:
					panic(dogma.UnexpectedMessage)
				}

				return nil
			},
		},
	)
}

func TestServer_contemporaryEvents(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			// Recording an event creates the stream, so that the watch is
			// delivering it before the events under test are recorded.
			xtesting.ExecuteCommandsSequentially(t, cluster, db, stubs.CommandB1)

			client, ctx := newConsumerClient(t, cluster)
			segments := watch(
				t,
				ctx,
				client,
				stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeA]](),
				stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeC]](),
			)

			if first := segments.receive(); first.GetBeginOffset() != 1 || first.GetEndOffset() != 1 {
				t.Fatalf(
					"unexpected first segment: got [%d, %d), want [1, 1)",
					first.GetBeginOffset(),
					first.GetEndOffset(),
				)
			}

			xtesting.ExecuteCommandsSequentially(
				t,
				cluster,
				db,
				stubs.CommandA1,
				stubs.CommandB1,
				stubs.CommandC1,
			)

			expectEvents(
				t,
				segments,
				[]dogma.Event{stubs.EventA1},
				[]dogma.Event{stubs.EventC1},
			)
		},
		consumerAPIApp(),
	)
}

func TestServer_historicalEvents(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			xtesting.ExecuteCommandsSequentially(
				t,
				cluster,
				db,
				stubs.CommandA1,
				stubs.CommandB1,
				stubs.CommandC1,
			)

			typeA := stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeA]]()
			typeC := stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeC]]()

			client, ctx := newConsumerClient(t, cluster)

			// The watch begins after the events already on the stream, so they
			// are read instead.
			first := watch(t, ctx, client, typeA, typeC).receive()
			streamID := first.GetEventStreamId()

			if first.GetBeginOffset() != 3 || first.GetEndOffset() != 3 {
				t.Fatalf(
					"unexpected first segment: got [%d, %d), want [3, 3)",
					first.GetBeginOffset(),
					first.GetEndOffset(),
				)
			}

			segments := read(t, ctx, client, streamID, 0, typeA, typeC)
			segments.ExpectSegment(streamID, 0, 3, stubs.EventA1, stubs.EventC1)
			segments.ExpectEOF()
		},
		consumerAPIApp(),
	)
}

func TestServer_WatchEvents(t *testing.T) {
	typeA := stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeA]]()

	t.Run("it begins each existing stream at its end", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
				xtesting.AppendToEventStream(t, db, streamID, stubs.EventA1, stubs.EventA2)
			},
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client, ctx := newConsumerClient(t, cluster)
				segments := watch(t, ctx, client, typeA)

				segments.ExpectSegment(streamID, 2, 2)
			},
			consumerAPIApp(),
		)
	})

	t.Run("it begins a stream with no events with an empty segment", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
			},
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client, ctx := newConsumerClient(t, cluster)
				segments := watch(t, ctx, client, typeA)

				segments.ExpectSegment(streamID, 0, 0)
			},
			consumerAPIApp(),
		)
	})

	t.Run("it delivers events appended after the client connects", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
			},
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client, ctx := newConsumerClient(t, cluster)
				segments := watch(t, ctx, client, typeA)

				segments.ExpectSegment(streamID, 0, 0)

				xtesting.AppendToEventStream(t, db, streamID, stubs.EventA1)

				segments.ExpectSegment(streamID, 0, 1, stubs.EventA1)
			},
			consumerAPIApp(),
		)
	})

	t.Run("it delivers a stream that is created after the client connects", func(t *testing.T) {
		var existing *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				existing = xtesting.CreateEventStream(t, db)
			},
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client, ctx := newConsumerClient(t, cluster)
				segments := watch(t, ctx, client, typeA)

				// Receiving the existing stream shows that the server is
				// watching the streams.
				segments.ExpectSegment(existing, 0, 0)

				created := xtesting.CreateEventStream(t, db)
				segments.ExpectSegment(created, 0, 0)

				xtesting.AppendToEventStream(t, db, created, stubs.EventA1)
				segments.ExpectSegment(created, 0, 1, stubs.EventA1)
			},
			consumerAPIApp(),
		)
	})

	t.Run("it advances past events of irrelevant types without delivering them", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
			},
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client, ctx := newConsumerClient(t, cluster)
				segments := watch(t, ctx, client, typeA)

				segments.ExpectSegment(streamID, 0, 0)

				xtesting.AppendToEventStream(t, db, streamID, stubs.EventX1, stubs.EventX2)
				segments.ExpectEmptySegmentsUntil(streamID, 2)

				xtesting.AppendToEventStream(t, db, streamID, stubs.EventA1)
				segments.ExpectSegment(streamID, 2, 3, stubs.EventA1)
			},
			consumerAPIApp(),
		)
	})

	t.Run("it delivers events of every type when no types are requested", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
			},
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client, ctx := newConsumerClient(t, cluster)
				segments := watch(t, ctx, client)

				segments.ExpectSegment(streamID, 0, 0)

				xtesting.AppendToEventStream(t, db, streamID, stubs.EventX1, stubs.EventA1)
				segments.ExpectEventsUntil(streamID, 2, stubs.EventX1, stubs.EventA1)
			},
			consumerAPIApp(),
		)
	})

	t.Run("it delivers every event in order to a client that falls behind", func(t *testing.T) {
		const eventCount = 1000

		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
			},
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client := messaginggrpc.NewEventStreamConsumerAPIClient(xtesting.DialGRPC(t, cluster.Nodes()[0]))
				segments := watch(t, t.Context(), client, typeA)

				segments.ExpectSegment(streamID, 0, 0)

				// While the client is not reading, append more data than gRPC
				// flow control can hold, so that the server can not send to
				// the client until it catches up.
				padding := strings.Repeat("x", 10000)
				var want []dogma.Event

				for i := range eventCount {
					event := &stubs.EventStub[stubs.TypeA]{
						Content: stubs.TypeA(fmt.Sprintf("%d %s", i, padding)),
					}
					want = append(want, event)
					xtesting.AppendToEventStream(t, db, streamID, event)
				}

				var got []dogma.Event

				for len(got) < eventCount {
					got = append(got, segments.Next(streamID)...)
				}

				xtesting.Expect(t, "events", got, want)
			},
			consumerAPIApp(),
		)
	})
}

func TestServer_ReadEvents(t *testing.T) {
	typeA := stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeA]]()
	typeX := stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeX]]()

	t.Run("it returns NOT_FOUND for an unrecognized stream", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client, ctx := newConsumerClient(t, cluster)
				streamID := uuidpb.Generate()

				segments := read(t, ctx, client, streamID, 0, typeA)

				err := segments.ExpectError()
				if status.Code(err) != codes.NotFound {
					t.Fatalf("unexpected error: got %v, want NOT_FOUND", err)
				}

				for _, detail := range status.Convert(err).Details() {
					if d, ok := detail.(*messaginggrpc.UnrecognizedEventStream); ok {
						if !d.GetEventStreamId().Equal(streamID) {
							t.Fatalf("unexpected stream ID in error details: got %s, want %s", d.GetEventStreamId(), streamID)
						}
						return
					}
				}

				t.Fatal("expected error details to contain an UnrecognizedEventStream")
			},
			consumerAPIApp(),
		)
	})

	t.Run("it returns OUT_OF_RANGE when the begin offset is beyond the end of the stream", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
				xtesting.AppendToEventStream(t, db, streamID, stubs.EventA1, stubs.EventA2)
			},
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client, ctx := newConsumerClient(t, cluster)
				segments := read(t, ctx, client, streamID, 3, typeA)

				if err := segments.ExpectError(); status.Code(err) != codes.OutOfRange {
					t.Fatalf("unexpected error: got %v, want OUT_OF_RANGE", err)
				}
			},
			consumerAPIApp(),
		)
	})

	t.Run("it delivers events from the begin offset to the end of the stream, then closes", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
				xtesting.AppendToEventStream(t, db, streamID, stubs.EventA1, stubs.EventA2, stubs.EventA3)
			},
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client, ctx := newConsumerClient(t, cluster)
				segments := read(t, ctx, client, streamID, 1, typeA)

				segments.ExpectSegment(streamID, 1, 3, stubs.EventA2, stubs.EventA3)
				segments.ExpectEOF()
			},
			consumerAPIApp(),
		)
	})

	t.Run("it ends the last segment at the end of the stream, past events of irrelevant types", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
				xtesting.AppendToEventStream(t, db, streamID, stubs.EventX1, stubs.EventE1)
			},
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client, ctx := newConsumerClient(t, cluster)
				segments := read(t, ctx, client, streamID, 0, typeX)

				segments.ExpectSegment(streamID, 0, 2, stubs.EventX1)
				segments.ExpectEOF()
			},
			consumerAPIApp(),
		)
	})

	t.Run("it delivers a segment with no events for a range with only events of irrelevant types", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
				xtesting.AppendToEventStream(t, db, streamID, stubs.EventX1, stubs.EventX2)
			},
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client, ctx := newConsumerClient(t, cluster)
				segments := read(t, ctx, client, streamID, 0, typeA)

				segments.ExpectSegment(streamID, 0, 2)
				segments.ExpectEOF()
			},
			consumerAPIApp(),
		)
	})

	t.Run("it delivers events of every type when no types are requested", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
				xtesting.AppendToEventStream(t, db, streamID, stubs.EventX1, stubs.EventA1)
			},
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client, ctx := newConsumerClient(t, cluster)
				segments := read(t, ctx, client, streamID, 0)

				segments.ExpectSegment(streamID, 0, 2, stubs.EventX1, stubs.EventA1)
				segments.ExpectEOF()
			},
			consumerAPIApp(),
		)
	})

	t.Run("it closes without delivering anything when beginning at the end of the stream", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
				xtesting.AppendToEventStream(t, db, streamID, stubs.EventA1)
			},
			func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
				client, ctx := newConsumerClient(t, cluster)
				segments := read(t, ctx, client, streamID, 1, typeA)

				segments.ExpectEOF()
			},
			consumerAPIApp(),
		)
	})
}

// newConsumerClient returns a client for the cluster's consumer API, and a
// context that is canceled when the test's wait timeout elapses.
func newConsumerClient(
	t testing.TB,
	cluster *xtesting.Cluster,
) (messaginggrpc.EventStreamConsumerAPIClient, context.Context) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
	t.Cleanup(cancel)

	return messaginggrpc.NewEventStreamConsumerAPIClient(xtesting.DialGRPC(t, cluster.Nodes()[0])), ctx
}

// watch begins watching events of the given types.
func watch(
	t testing.TB,
	ctx context.Context,
	client messaginggrpc.EventStreamConsumerAPIClient,
	messageTypeIDs ...*uuidpb.UUID,
) *segmentReceiver {
	t.Helper()

	responses, err := client.WatchEvents(
		ctx,
		messaginggrpc.NewWatchEventsRequestBuilder().
			WithMessageTypeIds(messageTypeIDs).
			Build(),
	)
	if err != nil {
		t.Fatal(err)
	}

	return &segmentReceiver{
		T:         t,
		Responses: responses,
	}
}

// read begins reading events of the given types from the given stream.
func read(
	t testing.TB,
	ctx context.Context,
	client messaginggrpc.EventStreamConsumerAPIClient,
	streamID *uuidpb.UUID,
	begin uint64,
	messageTypeIDs ...*uuidpb.UUID,
) *segmentReceiver {
	t.Helper()

	responses, err := client.ReadEvents(
		ctx,
		messaginggrpc.NewReadEventsRequestBuilder().
			WithEventStreamId(streamID).
			WithBeginOffset(begin).
			WithMessageTypeIds(messageTypeIDs).
			Build(),
	)
	if err != nil {
		t.Fatal(err)
	}

	return &segmentReceiver{
		T:         t,
		Responses: responses,
	}
}

// segmentReceiver receives the segments of a WatchEvents or ReadEvents call,
// and fails the test if each stream's segments are not contiguous.
type segmentReceiver struct {
	T         testing.TB
	Responses grpc.ServerStreamingClient[messaginggrpc.EventStreamSegment]

	ends uuidcollection.Map[uint64]
}

// Next receives the next segment, which must be of the given stream, and
// returns its events.
func (r *segmentReceiver) Next(streamID *uuidpb.UUID) []dogma.Event {
	r.T.Helper()

	seg := r.receive()

	if !seg.GetEventStreamId().Equal(streamID) {
		r.T.Fatalf("unexpected stream: got %s, want %s", seg.GetEventStreamId(), streamID)
	}

	return eventsIn(r.T, seg)
}

// ExpectSegment asserts that the next segment is the given range of the given
// stream, holding exactly the given events.
func (r *segmentReceiver) ExpectSegment(
	streamID *uuidpb.UUID,
	begin, end uint64,
	want ...dogma.Event,
) {
	r.T.Helper()

	seg := r.receive()

	if !seg.GetEventStreamId().Equal(streamID) {
		r.T.Fatalf("unexpected stream: got %s, want %s", seg.GetEventStreamId(), streamID)
	}

	if seg.GetBeginOffset() != begin || seg.GetEndOffset() != end {
		r.T.Fatalf(
			"unexpected segment: got [%d, %d), want [%d, %d)",
			seg.GetBeginOffset(),
			seg.GetEndOffset(),
			begin,
			end,
		)
	}

	xtesting.Expect(r.T, "events", eventsIn(r.T, seg), want)
}

// ExpectEmptySegmentsUntil asserts that segments of the given stream with no
// events are received until one ends at the given offset.
func (r *segmentReceiver) ExpectEmptySegmentsUntil(streamID *uuidpb.UUID, end uint64) {
	r.T.Helper()

	for {
		events := r.Next(streamID)
		if len(events) != 0 {
			r.T.Fatalf("unexpected events: %s", dapper.Format(events))
		}

		got, _ := r.ends.Get(streamID)

		if got == end {
			return
		}

		if got > end {
			r.T.Fatalf("unexpected end offset: got %d, want %d", got, end)
		}
	}
}

// ExpectEventsUntil asserts that segments of the given stream are received
// until one ends at the given offset, and that together they hold exactly the
// given events.
func (r *segmentReceiver) ExpectEventsUntil(
	streamID *uuidpb.UUID,
	end uint64,
	want ...dogma.Event,
) {
	r.T.Helper()

	var got []dogma.Event

	for {
		got = append(got, r.Next(streamID)...)

		reached, _ := r.ends.Get(streamID)

		if reached == end {
			break
		}

		if reached > end {
			r.T.Fatalf("unexpected end offset: got %d, want %d", reached, end)
		}
	}

	xtesting.Expect(r.T, "events", got, want)
}

// ExpectEOF asserts that the server closes the call without sending any
// further segments.
func (r *segmentReceiver) ExpectEOF() {
	r.T.Helper()

	if err := r.ExpectError(); !errors.Is(err, io.EOF) {
		r.T.Fatalf("expected the call to be closed, got error %v", err)
	}
}

// ExpectError asserts that the call ends without sending any further segments,
// and returns the error it ends with.
func (r *segmentReceiver) ExpectError() error {
	r.T.Helper()

	seg, err := r.Responses.Recv()
	if err == nil {
		r.T.Fatalf("unexpected segment: %v", seg)
	}

	return err
}

// receive receives the next segment, checking that it begins where the
// previous segment of the same stream ended.
func (r *segmentReceiver) receive() *messaginggrpc.EventStreamSegment {
	r.T.Helper()

	seg, err := r.Responses.Recv()
	if err != nil {
		r.T.Fatal(err)
	}

	streamID := seg.GetEventStreamId()

	if end, ok := r.ends.Get(streamID); ok && seg.GetBeginOffset() != end {
		r.T.Fatalf(
			"segment of stream %s is not contiguous: it begins at offset %d, but the previous segment ended at offset %d",
			streamID,
			seg.GetBeginOffset(),
			end,
		)
	}

	if seg.GetEndOffset() < seg.GetBeginOffset() {
		r.T.Fatalf(
			"segment of stream %s ends at offset %d, before it begins at offset %d",
			streamID,
			seg.GetEndOffset(),
			seg.GetBeginOffset(),
		)
	}

	r.ends.Set(streamID, seg.GetEndOffset())

	return seg
}

// eventsIn returns the events in a segment, checking that each is within the
// segment's range and that their offsets increase.
func eventsIn(t testing.TB, seg *messaginggrpc.EventStreamSegment) []dogma.Event {
	t.Helper()

	var (
		events []dogma.Event
		next   = seg.GetBeginOffset()
	)

	for _, envelopes := range seg.GetEnvelopes() {
		for env := range envelopes.All() {
			pos := envelopepb.MustGetExtension[*envelopepb.EventStreamPosition](env)
			offset := pos.GetOffset()

			if offset < next || offset >= seg.GetEndOffset() {
				t.Fatalf(
					"unexpected offset %d in segment [%d, %d)",
					offset,
					seg.GetBeginOffset(),
					seg.GetEndOffset(),
				)
			}

			next = offset + 1

			event, err := envelope.Unpack[dogma.Event](env)
			if err != nil {
				t.Fatal(err)
			}

			events = append(events, event)
		}
	}

	return events
}

// expectEvents fails the test unless each of the given sequences of events is
// received from the given segments, in order.
func expectEvents(
	t testing.TB,
	segments *segmentReceiver,
	want ...[]dogma.Event,
) {
	t.Helper()

	for len(want) != 0 {
		seg := segments.receive()

	next:
		for _, got := range eventsIn(t, seg) {
			for index, sequence := range want {
				if reflect.DeepEqual(got, sequence[0]) {
					want[index] = sequence[1:]

					if len(want[index]) == 0 {
						want = slices.Delete(want, index, index+1)
					}

					continue next
				}
			}

			t.Fatalf("unexpected event:\n%s", dapper.Format(got))
		}
	}
}
