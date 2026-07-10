package eventstream_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/grpc/eventstreamgrpc"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConsumeAPIServer(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, e *runkit.Engine) {
			addr, err := e.ListenAddr(t.Context())
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

			client := eventstreamgrpc.NewConsumeAPIClient(conn)

			xtesting.ExecuteCommand(t, e, stubs.CommandA1)
			xtesting.ExecuteCommand(t, e, stubs.CommandB1)
			xtesting.ExecuteCommand(t, e, stubs.CommandC1)

			messageTypeIDs := []*uuidpb.UUID{
				stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeA]](),
				stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeC]](),
			}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			group, ctx := errgroup.WithContext(ctx)

			listResponses, err := client.ListStreams(
				ctx,
				eventstreamgrpc.NewListStreamsRequestBuilder().
					WithMessageTypeIds(messageTypeIDs).
					Build(),
			)
			if err != nil {
				t.Fatalf("unable to list event streams: %s", err)
			}

			eventChannel := make(chan dogma.Event, 2)

			// Consume events from each stream as it's discovered.
			group.Go(func() error {
				for {
					listResponse, err := listResponses.Recv()
					if err != nil {
						return fmt.Errorf("unable to receive list response: %w", err)
					}

					stream := listResponse.GetStream()

					group.Go(func() error {
						consumeResponses, err := client.ConsumeEvents(
							ctx,
							eventstreamgrpc.NewConsumeEventsRequestBuilder().
								WithStreamId(stream.GetId()).
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
					})
				}
			})

			var events []dogma.Event

			for range 2 {
				select {
				case <-ctx.Done():
					err := group.Wait()
					t.Fatal(err)
				case event := <-eventChannel:
					events = append(events, event)
				}
			}

			cancel()
			group.Wait()

			xtesting.ExpectEqualUnorderedEvents(
				t,
				"events",
				events,
				stubs.EventA1,
				stubs.EventC1,
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
