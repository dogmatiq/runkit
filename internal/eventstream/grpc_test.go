package eventstream_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"testing"

	"github.com/dogmatiq/dapper"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/grpc/messaginggrpc"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// consumerAPIApp returns the handler route used by the consumer API tests. The
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

func TestConsumeAPIServer_contemporary(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
			xtesting.ExecuteCommand(t, engine, stubs.CommandA1)
			xtesting.ExecuteCommand(t, engine, stubs.CommandB1)
			xtesting.ExecuteCommand(t, engine, stubs.CommandC1)

			messageTypeIDs := []*uuidpb.UUID{
				stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeA]](),
				stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeC]](),
			}

			ExpectEvents(
				t,
				engine,
				messageTypeIDs,
				[]dogma.Event{stubs.EventA1},
				[]dogma.Event{stubs.EventC1},
			)
		},
		consumerAPIApp(),
	)
}

func TestConsumeAPIServer_historical(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
			xtesting.ExecuteCommandsSequentially(
				t,
				engine,
				db,
				stubs.CommandA1,
				stubs.CommandB1,
				stubs.CommandC1,
			)

			messageTypeIDs := []*uuidpb.UUID{
				stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeA]](),
				stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeC]](),
			}

			ExpectEvents(
				t,
				engine,
				messageTypeIDs,
				[]dogma.Event{stubs.EventA1},
				[]dogma.Event{stubs.EventC1},
			)
		},
		consumerAPIApp(),
	)
}

func TestConsumerAPI_ConsumeEvents(t *testing.T) {
	typeA := stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeA]]()
	typeX := stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeX]]()

	t.Run("it returns NOT_FOUND for an unrecognized stream", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				client, ctx := newConsumerClient(t, engine)
				streamID := uuidpb.Generate()

				responses := consume(t, ctx, client, streamID, 0, typeA)

				_, err := responses.Recv()
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

	t.Run("it returns OUT_OF_RANGE when the checkpoint is beyond the end of the stream", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
				xtesting.AppendToEventStream(t, db, streamID, stubs.EventA1, stubs.EventA2)
			},
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				client, ctx := newConsumerClient(t, engine)

				responses := consume(t, ctx, client, streamID, 3, typeA)

				_, err := responses.Recv()
				if status.Code(err) != codes.OutOfRange {
					t.Fatalf("unexpected error: got %v, want OUT_OF_RANGE", err)
				}
			},
			consumerAPIApp(),
		)
	})

	t.Run("it closes the stream when none of the requested types can be recorded", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
			},
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				client, ctx := newConsumerClient(t, engine)

				responses := consume(t, ctx, client, streamID, 0, typeX)

				expectEOF(t, responses)
			},
			consumerAPIApp(),
		)
	})

	t.Run("it closes the stream immediately when no message types are requested", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
			},
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				client, ctx := newConsumerClient(t, engine)

				responses := consume(t, ctx, client, streamID, 0)

				expectEOF(t, responses)
			},
			consumerAPIApp(),
		)
	})

	t.Run("it delivers existing events of types the application no longer records, then closes the stream", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
				xtesting.AppendToEventStream(t, db, streamID, stubs.EventX1)
			},
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				client, ctx := newConsumerClient(t, engine)

				responses := consume(t, ctx, client, streamID, 0, typeX)

				expectResponse(t, responses, 1, stubs.EventX1)
				expectEOF(t, responses)
			},
			consumerAPIApp(),
		)
	})

	t.Run("it reports the stream head when closing the stream for unrecordable types", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
				xtesting.AppendToEventStream(t, db, streamID, stubs.EventX1, stubs.EventE1)
			},
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				client, ctx := newConsumerClient(t, engine)

				responses := consume(t, ctx, client, streamID, 0, typeX)

				// The checkpoint is the stream head, past the trailing event
				// of an irrelevant type.
				expectResponse(t, responses, 2, stubs.EventX1)
				expectEOF(t, responses)
			},
			consumerAPIApp(),
		)
	})

	t.Run("it keeps delivering when a request mixes recordable and unrecordable types", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
				xtesting.AppendToEventStream(t, db, streamID, stubs.EventX1)
			},
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				client, ctx := newConsumerClient(t, engine)

				responses := consume(t, ctx, client, streamID, 0, typeA, typeX)

				expectResponse(t, responses, 1, stubs.EventX1)

				xtesting.AppendToEventStream(t, db, streamID, stubs.EventA1)

				expectResponse(t, responses, 2, stubs.EventA1)
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
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				client, ctx := newConsumerClient(t, engine)

				responses := consume(t, ctx, client, streamID, 0, typeA)

				xtesting.AppendToEventStream(t, db, streamID, stubs.EventA1)

				expectResponse(t, responses, 1, stubs.EventA1)
			},
			consumerAPIApp(),
		)
	})

	t.Run("it advances the checkpoint past events of irrelevant types without delivering them", func(t *testing.T) {
		var streamID *uuidpb.UUID

		xtesting.SetupThenRunEngines(
			t,
			func(t testing.TB, db *sql.DB) {
				streamID = xtesting.CreateEventStream(t, db)
			},
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				client, ctx := newConsumerClient(t, engine)

				responses := consume(t, ctx, client, streamID, 0, typeA)

				xtesting.AppendToEventStream(t, db, streamID, stubs.EventX1, stubs.EventX2)

				expectCheckpoint(t, responses, 2)

				xtesting.AppendToEventStream(t, db, streamID, stubs.EventA1)

				expectResponse(t, responses, 3, stubs.EventA1)
			},
			consumerAPIApp(),
		)
	})
}

// newConsumerClient returns a client for the engine's consumer API, and a
// context that is canceled when the test's wait timeout elapses.
func newConsumerClient(
	t testing.TB,
	engine *runkit.Engine,
) (messaginggrpc.EventStreamConsumerAPIClient, context.Context) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
	t.Cleanup(cancel)

	addr, err := engine.ListenAddr(ctx)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := grpc.NewClient(
		addr.String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return messaginggrpc.NewEventStreamConsumerAPIClient(conn), ctx
}

// consume begins consuming events of the given types from the given stream.
func consume(
	t testing.TB,
	ctx context.Context,
	client messaginggrpc.EventStreamConsumerAPIClient,
	streamID *uuidpb.UUID,
	checkpoint uint64,
	messageTypeIDs ...*uuidpb.UUID,
) grpc.ServerStreamingClient[messaginggrpc.ConsumeEventsResponse] {
	t.Helper()

	responses, err := client.ConsumeEvents(
		ctx,
		messaginggrpc.NewConsumeEventsRequestBuilder().
			WithEventStreamId(streamID).
			WithCheckpointOffset(checkpoint).
			WithMessageTypeIds(messageTypeIDs).
			Build(),
	)
	if err != nil {
		t.Fatal(err)
	}

	return responses
}

// expectResponse asserts that the next response contains exactly the given
// events and the given checkpoint offset.
func expectResponse(
	t testing.TB,
	responses grpc.ServerStreamingClient[messaginggrpc.ConsumeEventsResponse],
	checkpoint uint64,
	want ...dogma.Event,
) {
	t.Helper()

	res, err := responses.Recv()
	if err != nil {
		t.Fatal(err)
	}

	var got []dogma.Event

	for _, envelopes := range res.GetEnvelopes() {
		for envelope := range envelopes.All() {
			event, err := envelopepb.Unpack[dogma.Event](envelope)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, event)
		}
	}

	xtesting.Expect(t, "events", got, want)

	if res.GetCheckpointOffset() != checkpoint {
		t.Fatalf("unexpected checkpoint offset: got %d, want %d", res.GetCheckpointOffset(), checkpoint)
	}
}

// expectCheckpoint asserts that a response with the given checkpoint offset is
// received without any intervening events being delivered.
func expectCheckpoint(
	t testing.TB,
	responses grpc.ServerStreamingClient[messaginggrpc.ConsumeEventsResponse],
	want uint64,
) {
	t.Helper()

	for {
		res, err := responses.Recv()
		if err != nil {
			t.Fatal(err)
		}

		if len(res.GetEnvelopes()) != 0 {
			t.Fatalf("unexpected events in response with checkpoint offset %d", res.GetCheckpointOffset())
		}

		if res.GetCheckpointOffset() == want {
			return
		}

		if res.GetCheckpointOffset() > want {
			t.Fatalf("unexpected checkpoint offset: got %d, want %d", res.GetCheckpointOffset(), want)
		}
	}
}

// expectEOF asserts that the server closes the stream without sending any
// further responses.
func expectEOF(
	t testing.TB,
	responses grpc.ServerStreamingClient[messaginggrpc.ConsumeEventsResponse],
) {
	t.Helper()

	res, err := responses.Recv()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected the stream to be closed, got response %v, error %v", res, err)
	}
}

func ExpectEvents(
	t testing.TB,
	engine *runkit.Engine,
	messageTypeIDs []*uuidpb.UUID,
	want ...[]dogma.Event,
) {
	t.Helper()

	addr, err := engine.ListenAddr(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	conn, err := grpc.NewClient(
		addr.String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("unable to create gRPC client: %s", err)
	}
	defer conn.Close()

	client := messaginggrpc.NewEventStreamConsumerAPIClient(conn)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	group, ctx := errgroup.WithContext(ctx)
	eventChannel := make(chan dogma.Event, len(want))

	// Start listening for event streams.
	listResponses, err := client.ListEventStreams(
		ctx,
		messaginggrpc.NewListEventStreamsRequestBuilder().
			Build(),
	)
	if err != nil {
		t.Fatalf("unable to list event streams: %s", err)
	}

	// Wait for responses about new streams.
	group.Go(func() error {
		for {
			listResponse, err := listResponses.Recv()
			if err != nil {
				return fmt.Errorf("unable to receive list response: %w", err)
			}

			// Consume events from the stream and pipe them to the event channel.
			group.Go(func() error {
				return consumeEvents(
					ctx,
					client,
					listResponse.GetEventStream(),
					messageTypeIDs,
					eventChannel,
				)
			})
		}
	})

loop:
	for len(want) != 0 {
		select {
		case <-ctx.Done():
			err := group.Wait()
			t.Fatal(err)

		case gotEvent := <-eventChannel:
			for index, wantContiguous := range want {
				wantEvent := wantContiguous[0]

				if reflect.DeepEqual(gotEvent, wantEvent) {
					wantContiguous = wantContiguous[1:]

					if len(wantContiguous) == 0 {
						want = slices.Delete(want, index, index+1)
					}

					continue loop
				}
			}

			t.Fatalf(
				"unexpected event:\n%s",
				dapper.Format(gotEvent),
			)
		}
	}

	cancel()
	group.Wait()
}

func consumeEvents(
	ctx context.Context,
	client messaginggrpc.EventStreamConsumerAPIClient,
	stream *messaginggrpc.EventStream,
	messageTypeIDs []*uuidpb.UUID,
	eventChannel chan<- dogma.Event,
) error {
	consumeResponses, err := client.ConsumeEvents(
		ctx,
		messaginggrpc.NewConsumeEventsRequestBuilder().
			WithEventStreamId(stream.GetEventStreamId()).
			WithCheckpointOffset(0).
			WithMessageTypeIds(messageTypeIDs).
			Build(),
	)
	if err != nil {
		return fmt.Errorf("unable to consume events: %w", err)
	}

	for {
		consumeResponse, err := consumeResponses.Recv()
		if err != nil {
			return fmt.Errorf("unable to receive consume response: %w", err)
		}

		for _, envelopes := range consumeResponse.GetEnvelopes() {
			for envelope := range envelopes.All() {
				event, err := envelopepb.Unpack[dogma.Event](envelope)
				if err != nil {
					return err
				}
				eventChannel <- event
			}
		}
	}
}
