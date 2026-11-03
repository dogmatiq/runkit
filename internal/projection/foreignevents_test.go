package projection_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/x/xsync"
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

	var handled xsync.Latch

	xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		downstreamApp(t, &handled),
		3,
		runkit.WithRemoteEngineDiscoverer(
			runkit.StaticRemoteEngines(xtesting.Dial(t, upstream.Nodes()[0])),
		),
	)

	xtesting.ExpectLatchesSetEventually(t, &handled)
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

	var handled xsync.Latch

	xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		downstreamApp(t, &handled),
		3,
		runkit.WithRemoteEngineDiscoverer(
			runkit.StaticRemoteEngines(xtesting.Dial(t, upstream.Nodes()[0])),
		),
	)

	xtesting.ExecuteCommand(t, upstream, stubs.CommandA1)

	xtesting.ExpectLatchesSetEventually(t, &handled)
}

// upstreamApp returns an application that records [stubs.EventA1] when it
// executes [stubs.CommandA1].
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
							_ dogma.Command,
						) error {
							s.RecordEvent(stubs.EventA1)
							return nil
						},
					},
				),
			)
		},
	}
}

// downstreamApp returns an application with a projection that sets handled
// when it handles [stubs.EventA1].
func downstreamApp(t testing.TB, handled *xsync.Latch) dogma.Application {
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
									if !reflect.DeepEqual(m, stubs.EventA1) {
										t.Errorf("unexpected event: %v", m)
									}

									handled.Set()
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
