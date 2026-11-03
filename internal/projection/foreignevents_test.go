package projection_test

import (
	"context"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

// TestForeignEvents_historicalEventsAreHandled verifies that a projection
// handles events that a foreign application recorded before this engine
// started.
func TestForeignEvents_historicalEventsAreHandled(t *testing.T) {
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
		downstreamApp(handled),
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
		downstreamApp(handled),
		3,
		runkit.WithRemoteEngineDiscoverer(
			runkit.StaticRemoteEngines(xtesting.Dial(t, upstream.Nodes()[0])),
		),
	)

	xtesting.ExecuteCommand(t, upstream, stubs.CommandA1)

	xtesting.ExpectToReceiveN(t, "handled event", handled, dogma.Event(stubs.EventA1))
}

// TestForeignEvents_consumptionContinuesWhenAnUpstreamNodeStops verifies that a
// projection keeps handling a foreign application's events when the node of
// the remote engine that it is connected to stops.
func TestForeignEvents_consumptionContinuesWhenAnUpstreamNodeStops(t *testing.T) {
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
		downstreamApp(handled),
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
				dogma.ViaIntegration(
					&stubs.IntegrationMessageHandlerStub{
						ConfigureFunc: func(c dogma.IntegrationConfigurer) {
							c.Identity("<integration>", "5b2e8c1d-3f4a-4e6b-8d7c-9a0b1c2d3e4f")
							c.Routes(
								dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
								dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
							)
						},
						HandleCommandFunc: func(
							_ context.Context,
							s dogma.IntegrationCommandScope,
							m dogma.Command,
						) error {
							s.RecordEvent(
								&stubs.EventStub[stubs.TypeA]{
									Content: m.(*stubs.CommandStub[stubs.TypeA]).Content,
								},
							)
							return nil
						},
					},
				),
			)
		},
	}
}

// downstreamApp returns an application with a projection that sends each event
// of type A that it handles to the given channel.
func downstreamApp(handled chan<- dogma.Event) dogma.Application {
	var checkpoints checkpointStore

	return &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("<downstream>", "8e4f2a6b-1c3d-4e5f-a6b7-c8d9e0f1a2b3")
			c.Routes(
				dogma.ViaProjection(
					&stubs.ProjectionMessageHandlerStub{
						ConfigureFunc: func(c dogma.ProjectionConfigurer) {
							c.Identity("<projection>", "3c7a9e1f-5b2d-4a6c-8e0f-1a2b3c4d5e6f")
							c.Routes(
								dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
							)
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
				),
			)
		},
	}
}
