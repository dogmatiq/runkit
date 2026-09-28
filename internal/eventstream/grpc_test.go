package eventstream_test

import (
	"context"
	"fmt"
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
	"google.golang.org/grpc/credentials/insecure"
)

func TestConsumeAPIServer_contemporary(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *runkit.Engine) {
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
		dogma.ViaIntegration(
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
		),
	)
}

func TestConsumeAPIServer_historical(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *runkit.Engine) {
			xtesting.ExecuteCommandsSequentially(
				t,
				engine,
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
		dogma.ViaIntegration(
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
		),
	)
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
