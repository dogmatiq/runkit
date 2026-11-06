package process_test

import (
	"context"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

// TestForeignEvents_eventsProduceCommands verifies process-only event discovery
// for remote history, live remote events, and events recorded locally.
func TestForeignEvents_eventsProduceCommands(t *testing.T) {
	t.Parallel()

	upstream := xtesting.StartCluster(t, xtesting.NewDatabase(t), upstreamApp(), 1)
	xtesting.ExecuteCommand(t, upstream, stubs.CommandA1)

	commands := make(chan dogma.Command, 10)
	downstream := xtesting.StartCluster(
		t,
		xtesting.NewDatabase(t),
		downstreamApp(
			recordingIntegration[stubs.TypeB]("2e6a8c0d-4f1b-4d3e-9a7c-5b8e1f2a3c4d"),
			dogma.ViaIntegration(&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<commands>", "90fa7d04-6062-45c3-ab17-b39300385837")
					c.Routes(dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeX]]())
				},
				HandleCommandFunc: func(ctx context.Context, _ dogma.IntegrationCommandScope, m dogma.Command) error {
					select {
					case commands <- m:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				},
			}),
			dogma.ViaProcess(&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<process>", "3c7a9e1f-5b2d-4a6c-8e0f-1a2b3c4d5e6f")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
					)
				},
				RouteEventToInstanceFunc: func(context.Context, dogma.Event) (string, bool, error) {
					return "<instance>", true, nil
				},
				HandleEventFunc: func(_ context.Context, _ *stubs.ProcessRootStub, s dogma.ProcessEventScope[*stubs.ProcessRootStub], m dogma.Event) error {
					var content string
					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						content = string(m.Content)
					case *stubs.EventStub[stubs.TypeB]:
						content = string(m.Content)
					}
					s.ExecuteCommand(&stubs.CommandStub[stubs.TypeX]{Content: stubs.TypeX(content)})
					return nil
				},
			}),
		),
		2,
		runkit.WithRemoteEngineDiscoverer(runkit.StaticRemoteEngines(xtesting.DialGRPC(t, upstream.Nodes()[0]))),
	)

	xtesting.ExpectToReceiveN(t, "command from remote history", commands, dogma.Command(&stubs.CommandStub[stubs.TypeX]{Content: "A1"}))
	xtesting.ExecuteCommand(t, upstream, stubs.CommandA2)
	xtesting.ExpectToReceiveN(t, "command from remote event", commands, dogma.Command(&stubs.CommandStub[stubs.TypeX]{Content: "A2"}))
	xtesting.ExecuteCommand(t, downstream, stubs.CommandB1)
	xtesting.ExpectToReceiveN(t, "command from local event", commands, dogma.Command(&stubs.CommandStub[stubs.TypeX]{Content: "B1"}))
}

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
