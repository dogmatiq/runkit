package aggregate_test

import (
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestCommandsAreRoutedToTheCorrectHandler(t *testing.T) {
	var (
		handlerACalled xsync.Latch
		handlerBCalled xsync.Latch
	)

	xtesting.Run(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{},
			)

			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeB]{},
			)

			xtesting.ExpectLatchesSetEventually(
				t,
				&handlerACalled,
				&handlerBCalled,
			)
		},
		dogma.ViaAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler-a>", "ef0660b4-a68e-4383-b156-5857ac294dce")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				RouteCommandToInstanceFunc: func(m dogma.Command) string {
					return "instance"
				},
				HandleCommandFunc: func(
					r *stubs.AggregateRootStub,
					s dogma.AggregateCommandScope[*stubs.AggregateRootStub],
					m dogma.Command,
				) {
					defer handlerACalled.Set()

					switch m := m.(type) {
					case *stubs.CommandStub[stubs.TypeA]:
						// ok
					default:
						t.Errorf("unexpected command type routed to handler-a: %T", m)
					}
				},
			},
		),
		dogma.ViaAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler-b>", "3d55fb34-c7b6-4dea-a5de-8da4a006d64d")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeB]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeB]](),
					)
				},
				RouteCommandToInstanceFunc: func(m dogma.Command) string {
					return "instance"
				},
				HandleCommandFunc: func(
					r *stubs.AggregateRootStub,
					s dogma.AggregateCommandScope[*stubs.AggregateRootStub],
					m dogma.Command,
				) {
					defer handlerBCalled.Set()

					switch m := m.(type) {
					case *stubs.CommandStub[stubs.TypeB]:
						// ok
					default:
						t.Errorf("unexpected command type routed to handler-b: %T", m)
					}

				},
			},
		),
	)

}

func TestCommandsAreRoutedToTheCorrectInstance(t *testing.T) {
	var handlerCalled xsync.Latch

	xtesting.Run(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{Content: "<content>"},
			)

			xtesting.ExpectLatchesSetEventually(
				t,
				&handlerCalled,
			)
		},
		dogma.ViaAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler>", "ef0660b4-a68e-4383-b156-5857ac294dce")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				RouteCommandToInstanceFunc: func(m dogma.Command) string {
					switch m := m.(type) {
					case *stubs.CommandStub[stubs.TypeA]:
						return "instance:" + string(m.Content)
					default:
						panic(dogma.UnexpectedMessage)
					}
				},
				HandleCommandFunc: func(
					r *stubs.AggregateRootStub,
					s dogma.AggregateCommandScope[*stubs.AggregateRootStub],
					m dogma.Command,
				) {
					defer handlerCalled.Set()

					if got, want := s.InstanceID(), "instance:<content>"; got != want {
						t.Errorf("unexpected instance ID: got %q, want %q", got, want)
					}
				},
			},
		),
	)
}
