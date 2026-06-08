package aggregate_test

import (
	"context"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestAggregate(t *testing.T) {
	t.Run("it routes commands to the correct handler", func(t *testing.T) {
		var (
			handlerACalled xsync.Latch
			handlerBCalled xsync.Latch
		)

		app := &stubs.ApplicationStub{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("<app>", "2fba12dd-4608-43e8-9bbd-16fb32ae452e")
				c.Routes(
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
			},
		}

		xtesting.Run(
			t,
			app,
			func(ctx context.Context, engine *dogmaengine.Engine) {
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
		)
	})

	t.Run("it routes commands to the correct aggregate instance", func(t *testing.T) {
		var handlerCalled xsync.Latch

		app := &stubs.ApplicationStub{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("<app>", "2fba12dd-4608-43e8-9bbd-16fb32ae452e")
				c.Routes(
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
			},
		}

		xtesting.Run(
			t,
			app,
			func(ctx context.Context, engine *dogmaengine.Engine) {
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
		)
	})
}
