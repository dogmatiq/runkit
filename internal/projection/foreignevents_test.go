package projection_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestForeignEvents_eventsAreHandled verifies that a projection handles the
// events that a foreign application recorded before this engine started, the
// events it records while this engine is running, and the events that this
// application records.
func TestForeignEvents_eventsAreHandled(t *testing.T) {
	t.Parallel()

	upstream := xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		upstreamApp(),
		3,
	)

	xtesting.ExecuteCommand(t, upstream, stubs.CommandA1)

	handled := make(chan dogma.Event, 10)

	downstream := xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		downstreamApp(
			recordingIntegration[stubs.TypeB]("2e6a8c0d-4f1b-4d3e-9a7c-5b8e1f2a3c4d"),
			capturingProjection(
				"<projection>",
				"3c7a9e1f-5b2d-4a6c-8e0f-1a2b3c4d5e6f",
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

	xtesting.ExpectToReceiveN(t, "handled event", handled, dogma.Event(stubs.EventA1))

	xtesting.ExecuteCommand(t, upstream, stubs.CommandA2)
	xtesting.ExpectToReceiveN(t, "handled event", handled, dogma.Event(stubs.EventA2))

	xtesting.ExecuteCommand(t, downstream, stubs.CommandB1)
	xtesting.ExpectToReceiveN(t, "handled event", handled, dogma.Event(stubs.EventB1))
}

// TestForeignEvents_nodeWatchesEachRemoteEngineOnce verifies that a node
// watches a remote engine with one WatchEvents call, however many projections
// consume its events.
func TestForeignEvents_nodeWatchesEachRemoteEngineOnce(t *testing.T) {
	t.Parallel()

	upstream := xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		upstreamApp(),
		1,
	)

	conn, watches := dialCountingWatches(t, upstream.Nodes()[0])

	handledX := make(chan dogma.Event, 10)
	handledY := make(chan dogma.Event, 10)

	xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		downstreamApp(
			capturingProjection(
				"<projection-x>",
				"3c7a9e1f-5b2d-4a6c-8e0f-1a2b3c4d5e6f",
				handledX,
				dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
			),
			capturingProjection(
				"<projection-y>",
				"9d1e3f5a-7b2c-4d6e-8f0a-1b2c3d4e5f6a",
				handledY,
				dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
			),
		),
		1,
		runkit.WithRemoteEngineDiscoverer(
			runkit.StaticRemoteEngines(conn),
		),
	)

	xtesting.ExecuteCommand(t, upstream, stubs.CommandA1)

	xtesting.ExpectToReceiveN(t, "event handled by projection X", handledX, dogma.Event(stubs.EventA1))
	xtesting.ExpectToReceiveN(t, "event handled by projection Y", handledY, dogma.Event(stubs.EventA1))

	if got := watches.Load(); got != 1 {
		t.Fatalf("unexpected number of WatchEvents calls: got %d, want 1", got)
	}
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

// capturingProjection returns a projection handler with the given identity and
// routes that sends each event it handles to the given channel.
func capturingProjection(
	name, key string,
	handled chan<- dogma.Event,
	routes ...dogma.ProjectionRoute,
) dogma.HandlerRoute {
	var checkpoints checkpointStore

	return dogma.ViaProjection(
		&stubs.ProjectionMessageHandlerStub{
			ConfigureFunc: func(c dogma.ProjectionConfigurer) {
				c.Identity(name, key)
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

// dialCountingWatches returns a connection to the given engine, which must be
// listening for gRPC requests, and the number of WatchEvents calls made on it.
func dialCountingWatches(t *testing.T, e *runkit.Engine) (*grpc.ClientConn, *atomic.Int32) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
	defer cancel()

	addr, err := e.ListenAddr(ctx)
	if err != nil {
		t.Fatal(err)
	}

	watches := &atomic.Int32{}

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
				if desc.StreamName == "WatchEvents" {
					watches.Add(1)
				}
				return streamer(ctx, desc, cc, method, opts...)
			},
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn, watches
}
