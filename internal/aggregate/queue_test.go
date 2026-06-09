package aggregate_test

import (
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/contexthook"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestAggregate_commandQueueUsage(t *testing.T) {
	t.Run("it removes commands from the queue after handling", func(t *testing.T) {
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
							RouteCommandToInstanceFunc: func(dogma.Command) string {
								return "<instance>"
							},
						},
					),
				)
			},
		}

		xtesting.Run(
			t,
			app,
			func(t testing.TB, engine *dogmaengine.Engine) {
				xtesting.ExecuteCommand(
					t,
					engine,
					&stubs.CommandStub[stubs.TypeA]{},
				)

				xtesting.ExpectEmptyCommandQueueEventually(
					t,
					engine.DB,
				)
			},
		)
	})

	t.Run("it does not remove other commands from the queue", func(t *testing.T) {
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
							RouteCommandToInstanceFunc: func(dogma.Command) string {
								return "<instance>"
							},
						},
					),
				)
			},
		}

		xtesting.Run(
			t,
			app,
			func(t testing.TB, engine *dogmaengine.Engine) {
				handledCommandEnvelope := xtesting.ExecuteCommand(
					t,
					engine,
					&stubs.CommandStub[stubs.TypeA]{},
				)

				ignoredCommandEnvelope := xtesting.ExecuteCommand(
					t,
					engine,
					&stubs.CommandStub[stubs.TypeB]{},
				)

				xtesting.ExpectCommandToBeRemovedFromQueueEventually(
					t,
					engine.DB,
					handledCommandEnvelope.GetBody().GetMessageId(),
				)

				xtesting.ExpectCommandToBeQueued(
					t,
					engine.DB,
					ignoredCommandEnvelope.GetBody().GetMessageId(),
				)
			},
		)
	})

	t.Run("it defers commands that cannot be unpacked", func(t *testing.T) {
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
							RouteCommandToInstanceFunc: func(dogma.Command) string {
								return "<instance>"
							},
						},
					),
				)
			},
		}

		xtesting.Run(
			t,
			app,
			func(t testing.TB, engine *dogmaengine.Engine) {
				// Execute an invalid command so that it will be deferred.
				invalidCommandEnvelope := xtesting.ExecuteCommandWithHook(
					t,
					engine,
					&stubs.CommandStub[stubs.TypeA]{},
					func(x contexthook.ExecuteCommand) {
						// Corrupt the command so that it cannot be unpacked.
						x.CommandEnvelope.GetBody().GetMessage().SetData([]byte("<invalid>"))
					},
				)

				xtesting.ExpectCommandToBeDeferredEventually(
					t,
					engine.DB,
					invalidCommandEnvelope.GetBody().GetMessageId(),
				)

				// Execute a valid command to verify that the deferred command
				// does not block handling of other commands.
				validCommandEnvelope := xtesting.ExecuteCommand(
					t,
					engine,
					&stubs.CommandStub[stubs.TypeA]{},
				)

				xtesting.ExpectCommandToBeRemovedFromQueueEventually(
					t,
					engine.DB,
					validCommandEnvelope.GetBody().GetMessageId(),
				)

				// Verify that the invalid command has not been attempted
				// more than once.
				xtesting.ExpectCommandAttemptCount(
					t,
					engine.DB,
					invalidCommandEnvelope.GetBody().GetMessageId(),
					1,
				)
			},
		)
	})
}
