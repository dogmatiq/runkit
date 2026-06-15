package projection_test

import (
	"context"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestEventsFromTheSameStreamAreRoutedToTheCorrectHandler(t *testing.T) {
	var (
		handlerACalled xsync.Latch
		handlerBCalled xsync.Latch
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{},
			)

			xtesting.ExpectLatchesSetEventually(
				t,
				&handlerACalled,
				&handlerBCalled,
			)
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<event-producer>", "2a20dee8-ea25-481a-b470-14926c509a3a")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeB]](),
					)
				},
				HandleCommandFunc: func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					_ dogma.Command,
				) error {
					s.RecordEvent(&stubs.EventStub[stubs.TypeA]{})
					s.RecordEvent(&stubs.EventStub[stubs.TypeB]{})
					return nil
				},
			},
		),
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler-a>", "6f6c3151-b980-430c-bb7a-891f12104035")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleEventFunc: func(
					_ context.Context,
					s dogma.ProjectionEventScope,
					m dogma.Event,
				) (uint64, error) {
					defer handlerACalled.Set()

					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						return s.Offset() + 1, nil
					default:
						t.Errorf("unexpected event type routed to handler-a: %T", m)
						return 0, nil
					}
				},
				CheckpointOffsetFunc: func(context.Context, string) (uint64, error) {
					if handlerACalled.IsSet() {
						return 1, nil
					}
					return 0, nil
				},
			},
		),
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler-b>", "93090487-ddbb-417a-9166-9663c303e8c3")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
					)
				},
				HandleEventFunc: func(
					_ context.Context,
					s dogma.ProjectionEventScope,
					m dogma.Event,
				) (uint64, error) {
					defer handlerBCalled.Set()

					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeB]:
						return s.Offset() + 1, nil
					default:
						t.Errorf("unexpected event type routed to handler-b: %T", m)
						return 0, nil
					}
				},
				CheckpointOffsetFunc: func(context.Context, string) (uint64, error) {
					if handlerBCalled.IsSet() {
						return 1, nil
					}
					return 0, nil
				},
			},
		),
	)
}
