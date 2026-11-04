package projection_test

import (
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestForeignEvents_historicalEventsAreHandled verifies that a projection
// handles events that a foreign application recorded before this engine
// started.
func TestForeignEvents_historicalEventsAreHandled(t *testing.T) {
	t.Skip("remote consumption is being rebuilt; see docs/.plans/2026-11-04-event-feed-consumption.md")
	t.Parallel()

	upstream := xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		upstreamApp(),
		3,
	)

	xtesting.ExecuteCommand(t, upstream, stubs.CommandA1)

	handled := make(chan dogma.Event, 10)

	xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		downstreamApp(
			capturingProjection(handled, dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]]()),
		),
		3,
		runkit.WithRemoteEngineDiscoverer(
			runkit.StaticRemoteEngines(xtesting.Dial(t, upstream.Nodes()[0])),
		),
	)

	xtesting.ExpectToReceiveN(t, "handled event", handled, dogma.Event(stubs.EventA1))
}

// TestForeignEvents_contemporaryEventsAreHandled verifies that a projection
// handles events that a foreign application records while this engine is
// running.
func TestForeignEvents_contemporaryEventsAreHandled(t *testing.T) {
	t.Skip("remote consumption is being rebuilt; see docs/.plans/2026-11-04-event-feed-consumption.md")
	t.Parallel()

	upstream := xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		upstreamApp(),
		3,
	)

	handled := make(chan dogma.Event, 10)

	xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		downstreamApp(
			capturingProjection(handled, dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]]()),
		),
		3,
		runkit.WithRemoteEngineDiscoverer(
			runkit.StaticRemoteEngines(xtesting.Dial(t, upstream.Nodes()[0])),
		),
	)

	xtesting.ExecuteCommand(t, upstream, stubs.CommandA1)

	xtesting.ExpectToReceiveN(t, "handled event", handled, dogma.Event(stubs.EventA1))
}

// TestForeignEvents_localAndForeignEventsAreHandled verifies that a projection
// handles events recorded by this application and by a foreign application at
// the same time.
func TestForeignEvents_localAndForeignEventsAreHandled(t *testing.T) {
	t.Skip("remote consumption is being rebuilt; see docs/.plans/2026-11-04-event-feed-consumption.md")
	t.Parallel()

	upstream := xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		upstreamApp(),
		3,
	)

	handled := make(chan dogma.Event, 10)

	downstream := xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		downstreamApp(
			recordingIntegration[stubs.TypeB]("2e6a8c0d-4f1b-4d3e-9a7c-5b8e1f2a3c4d"),
			capturingProjection(
				handled,
				dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
				dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
			),
		),
		3,
		runkit.WithRemoteEngineDiscoverer(
			runkit.StaticRemoteEngines(xtesting.Dial(t, upstream.Nodes()[0])),
		),
	)

	xtesting.ExecuteCommand(t, upstream, stubs.CommandA1)
	xtesting.ExecuteCommand(t, downstream, stubs.CommandB1)

	var got []dogma.Event

	for range 2 {
		select {
		case <-time.After(xtesting.WaitTimeout):
			t.Fatalf("timeout waiting for handled events: got %d of 2", len(got))
		case event := <-handled:
			got = append(got, event)
		}
	}

	// The events are on different streams, so their order is not defined.
	for _, want := range []dogma.Event{stubs.EventA1, stubs.EventB1} {
		if !slices.ContainsFunc(
			got,
			func(e dogma.Event) bool {
				return reflect.DeepEqual(e, want)
			},
		) {
			t.Fatalf("expected %v to be handled, got %v", want, got)
		}
	}
}

// TestForeignEvents_projectionWithoutForeignEventTypesDoesNotRetryImmediately
// verifies that a projection which handles none of the event types that a
// foreign application records does not re-subscribe to its streams as soon as
// the application closes them, but waits for the recheck interval.
func TestForeignEvents_projectionWithoutForeignEventTypesDoesNotRetryImmediately(t *testing.T) {
	t.Skip("remote consumption is being rebuilt; see docs/.plans/2026-11-04-event-feed-consumption.md")
	t.Parallel()

	upstream := xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		upstreamApp(),
		1,
	)

	xtesting.ExecuteCommand(t, upstream, stubs.CommandA1)

	ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
	defer cancel()

	addr, err := upstream.Nodes()[0].ListenAddr(ctx)
	if err != nil {
		t.Fatal(err)
	}

	subscriptions := make(chan struct{}, 10)

	conn, err := grpc.NewClient(
		addr.String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(
			func(
				ctx context.Context,
				desc *grpc.StreamDesc,
				cc *grpc.ClientConn,
				method string,
				streamer grpc.Streamer,
				opts ...grpc.CallOption,
			) (grpc.ClientStream, error) {
				if desc.StreamName == "ConsumeEvents" {
					select {
					case subscriptions <- struct{}{}:
					default:
					}
				}
				return streamer(ctx, desc, cc, method, opts...)
			},
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	// The projection handles only type B, which the upstream application does
	// not record.
	xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		downstreamApp(
			capturingProjection(
				make(chan dogma.Event, 10),
				dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
			),
		),
		1,
		runkit.WithRemoteEngineDiscoverer(
			runkit.StaticRemoteEngines(conn),
		),
	)

	xtesting.ExpectToReceiveN(t, "subscription", subscriptions, struct{}{})

	// Re-subscribing as soon as the upstream closes the stream, even after the
	// supervisor's initial backoff, would happen within this period.
	select {
	case <-subscriptions:
		t.Fatal("unexpected re-subscription")
	case <-time.After(500 * time.Millisecond):
	}
}

// TestForeignEvents_consumptionContinuesWhenAnUpstreamNodeStops verifies that a
// projection keeps handling a foreign application's events when the node of
// the remote engine that it is connected to stops.
func TestForeignEvents_consumptionContinuesWhenAnUpstreamNodeStops(t *testing.T) {
	t.Skip("remote consumption is being rebuilt; see docs/.plans/2026-11-04-event-feed-consumption.md")
	t.Parallel()

	upstream := xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		upstreamApp(),
		2,
	)

	// The downstream cluster's connection to the remote engine sends requests
	// to the first upstream node until it stops, then to the second.
	nodes := upstream.Nodes()
	conn := xtesting.Dial(t, nodes[0], nodes[1])

	handled := make(chan dogma.Event, 10)

	xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		downstreamApp(
			capturingProjection(handled, dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]]()),
		),
		3,
		runkit.WithRemoteEngineDiscoverer(
			runkit.StaticRemoteEngines(conn),
		),
	)

	xtesting.ExecuteCommand(t, upstream, stubs.CommandA1)
	xtesting.ExpectToReceiveN(t, "handled event", handled, dogma.Event(stubs.EventA1))

	upstream.StopNode(nodes[0])

	xtesting.ExecuteCommand(t, upstream, stubs.CommandA2)
	xtesting.ExpectToReceiveN(t, "handled event", handled, dogma.Event(stubs.EventA2))
}

// upstreamApp returns an application that records an event of type A, with the
// same content as the command, when it executes a command of type A.
func upstreamApp() dogma.Application {
	return &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("<upstream>", "0d6d5f0c-7d3e-4b7a-9a8f-2c1b6e4d3a90")
			c.Routes(
				recordingIntegration[stubs.TypeA]("5b2e8c1d-3f4a-4e6b-8d7c-9a0b1c2d3e4f"),
			)
		},
	}
}

// downstreamApp returns an application with the given handlers.
func downstreamApp(routes ...dogma.HandlerRoute) dogma.Application {
	return &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("<downstream>", "8e4f2a6b-1c3d-4e5f-a6b7-c8d9e0f1a2b3")
			c.Routes(routes...)
		},
	}
}

// recordingIntegration returns an integration handler that records an event of
// type T, with the same content as the command, when it handles a command of
// type T.
func recordingIntegration[T ~string](key string) dogma.HandlerRoute {
	return dogma.ViaIntegration(
		&stubs.IntegrationMessageHandlerStub{
			ConfigureFunc: func(c dogma.IntegrationConfigurer) {
				c.Identity("<integration>", key)
				c.Routes(
					dogma.HandlesCommand[*stubs.CommandStub[T]](),
					dogma.RecordsEvent[*stubs.EventStub[T]](),
				)
			},
			HandleCommandFunc: func(
				_ context.Context,
				s dogma.IntegrationCommandScope,
				m dogma.Command,
			) error {
				s.RecordEvent(
					&stubs.EventStub[T]{
						Content: m.(*stubs.CommandStub[T]).Content,
					},
				)
				return nil
			},
		},
	)
}

// capturingProjection returns a projection handler with the given routes that
// sends each event it handles to the given channel.
func capturingProjection(
	handled chan<- dogma.Event,
	routes ...dogma.ProjectionRoute,
) dogma.HandlerRoute {
	var checkpoints checkpointStore

	return dogma.ViaProjection(
		&stubs.ProjectionMessageHandlerStub{
			ConfigureFunc: func(c dogma.ProjectionConfigurer) {
				c.Identity("<projection>", "3c7a9e1f-5b2d-4a6c-8e0f-1a2b3c4d5e6f")
				c.Routes(routes...)
			},
			HandleEventFunc: func(
				_ context.Context,
				s dogma.ProjectionEventScope,
				m dogma.Event,
			) (uint64, error) {
				return checkpoints.HandleEvent(
					s,
					func() error {
						handled <- m
						return nil
					},
				)
			},
			CheckpointOffsetFunc: checkpoints.CheckpointOffset,
		},
	)
}
