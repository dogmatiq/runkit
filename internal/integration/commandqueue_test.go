package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/contexthook"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

// TestCommandQueue_commandIsRemovedAfterHandling verifies that a command is
// removed from the command queue after it is successfully handled.
func TestCommandQueue_commandIsRemovedAfterHandling(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExecuteCommandAndWait(t, engine, stubs.CommandA1)
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler>", "87f5a992-a3a6-494a-be1c-c01c6fff8ff0")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
			},
		),
	)
}

// TestCommandQueue_unhandledCommandsRemainInQueue verifies that if a command is
// not handled by any handler, it remains in the command queue.
func TestCommandQueue_unhandledCommandsRemainInQueue(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			handledCommandEnvelope := xtesting.ExecuteCommand(t, engine, stubs.CommandA1)

			ignoredCommandEnvelope := xtesting.ExecuteCommandWithHook(
				t,
				engine,
				stubs.CommandA1,
				func(x contexthook.ExecuteCommand) {
					// Mangle the command type so that the handler does not
					// attempt to handle it. We can't simply execute a different
					// command type because the engine would reject it since
					// there is no handler that can handle it.
					x.CommandEnvelope.
						GetBody().
						GetMessage().
						SetTypeId(uuidpb.Generate())
				},
			)

			xtesting.WaitForCommandToBeRemovedFromQueue(
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
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler>", "87f5a992-a3a6-494a-be1c-c01c6fff8ff0")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
			},
		),
	)
}

// TestCommandQueue_invalidCommandsArePostponed verifies that if a command
// cannot be unpacked, it is postponed and it does not prevent the handler from
// processing other commands.
func TestCommandQueue_invalidCommandsArePostponed(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Execute an invalid command.
			invalidCommandEnvelope := xtesting.ExecuteCommandWithHook(
				t,
				engine,
				stubs.CommandA1,
				func(x contexthook.ExecuteCommand) {
					// Corrupt the command envelope so that it cannot be unpacked.
					x.CommandEnvelope.
						GetBody().
						GetMessage().
						SetData([]byte("<invalid>"))
				},
			)

			// Execute a second valid command to verify that the invalid
			// command does not block handling of other commands.
			validCommandEnvelope := xtesting.ExecuteCommand(t, engine, stubs.CommandA1)

			xtesting.WaitForCommandToBeRemovedFromQueue(
				t,
				engine.DB,
				validCommandEnvelope.GetBody().GetMessageId(),
			)

			xtesting.WaitForCommandToBePostponed(
				t,
				engine.DB,
				invalidCommandEnvelope.GetBody().GetMessageId(),
			)
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<handler>", "87f5a992-a3a6-494a-be1c-c01c6fff8ff0")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
			},
		),
	)
}

// TestCommandQueue_handlerFailuresCauseCommandToBePostponed verifies that if
// handling a command fails (either via error return or panic), the command is
// postponed.
func TestCommandQueue_handlerFailuresCauseCommandToBePostponed(t *testing.T) {
	cases := []struct {
		Name              string
		HandleCommandFunc func(
			context.Context,
			dogma.IntegrationCommandScope,
			dogma.Command,
		) error
	}{
		{
			"returns error",
			func(
				context.Context,
				dogma.IntegrationCommandScope,
				dogma.Command,
			) error {
				return fmt.Errorf("<handler error>")
			},
		},
		{
			"panics",
			func(
				context.Context,
				dogma.IntegrationCommandScope,
				dogma.Command,
			) error {
				panic("<handler panic>")
			},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			xtesting.RunEngines(
				t,
				func(t testing.TB, engine *dogmaengine.Engine) {
					commandEnvelope := xtesting.ExecuteCommand(t, engine, stubs.CommandA1)

					xtesting.WaitForCommandToBePostponed(
						t,
						engine.DB,
						commandEnvelope.GetBody().GetMessageId(),
					)
				},
				dogma.ViaIntegration(
					&stubs.IntegrationMessageHandlerStub{
						ConfigureFunc: func(c dogma.IntegrationConfigurer) {
							c.Identity("<handler>", "87f5a992-a3a6-494a-be1c-c01c6fff8ff0")
							c.Routes(
								dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
								dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
							)
						},
						HandleCommandFunc: c.HandleCommandFunc,
					},
				),
			)
		})
	}
}
