package aggregate_test

import (
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

// TestCommandRouting_commandsAreRoutedToTheCorrectHandler verifies that
// commands are routed to the correct handler based on their message type.
func TestCommandRouting_commandsAreRoutedToTheCorrectHandler(t *testing.T) {
	var (
		handlerACalled xsync.Latch
		handlerBCalled xsync.Latch
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Execute two commands of different types, each targetting a
			// different handler.
			xtesting.ExecuteCommand(t, engine, stubs.CommandA1)
			xtesting.ExecuteCommand(t, engine, stubs.CommandB1)

			xtesting.ExpectLatchesSetEventually(
				t,
				&handlerACalled,
				&handlerBCalled,
			)
		},
		dogma.ViaAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler-a>", "5ccb274f-4116-41bd-92f4-3f3b8676891c")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				RouteCommandToInstanceFunc: func(m dogma.Command) string {
					return "<instance>"
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
					return "<instance>"
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

// TestCommandRouting_commandsAreRoutedToTheCorrectInstance verifies that
// commands are routed to the correct instance based on the value returned by
// the handler's RouteCommandToInstance() method.
func TestCommandRouting_commandsAreRoutedToTheCorrectInstance(t *testing.T) {
	var handlerCalled xsync.Latch

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExecuteCommand(t, engine, stubs.CommandA1)

			xtesting.ExpectLatchesSetEventually(
				t,
				&handlerCalled,
			)
		},
		dogma.ViaAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler>", "5fbf4e46-2626-44eb-abe3-74717de849b0")
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

					if got, want := s.InstanceID(), "instance:A1"; got != want {
						t.Errorf("unexpected instance ID: got %q, want %q", got, want)
					}
				},
			},
		),
	)
}
