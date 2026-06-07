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
	t.Run("it dispatches commands to the correct aggregate instance", func(t *testing.T) {
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

								switch m := m.(type) {
								case *stubs.CommandStub[stubs.TypeA]:
									if got, want := string(m.Content), "<content>"; got != want {
										t.Errorf("unexpected command content: got %q, want %q", got, want)
									}
								default:
									panic(dogma.UnexpectedMessage)
								}
							},
						},
					),
				)
			},
		}

		xtesting.RunApp(
			t,
			app,
			func(ctx context.Context, engine *dogmaengine.Engine) {
				if err := engine.ExecuteCommand(
					ctx,
					&stubs.CommandStub[stubs.TypeA]{Content: "<content>"},
				); err != nil {
					t.Fatalf("unable to execute command: %v", err)
				}

				select {
				case <-ctx.Done():
					t.Fatalf("timeout waiting for command to be handled: %v", ctx.Err())
				case <-handlerCalled.Chan():
					// success; handler itself makes assertions about the command it received
				}
			},
		)
	})
}
