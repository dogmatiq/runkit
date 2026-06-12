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

func TestCommandRemovedFromQueueAfterHandling(t *testing.T) {
	xtesting.RunEngines(
		t,
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

func TestUnhandledCommandsRemainQueued(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			handledCommandEnvelope := xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{},
			)

			ignoredCommandEnvelope := xtesting.ExecuteCommandWithHook(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{},
				func(x contexthook.ExecuteCommand) {
					// Mangle the command type so that it's something that
					// is not handled by the aggregate handler.
					x.CommandEnvelope.GetBody().GetMessage().SetTypeId(uuidpb.Generate())
				},
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

func TestInvalidCommandsAreDeferred(t *testing.T) {
	xtesting.RunEngines(
		t,
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

			xtesting.ExpectCommandToBeDeferredDueToFailureEventually(
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

func TestCommandIsDeferredWhenHandlerFails(t *testing.T) {
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
					commandEnvelope := xtesting.ExecuteCommand(
						t,
						engine,
						&stubs.CommandStub[stubs.TypeA]{},
					)

					xtesting.ExpectCommandToBeDeferredDueToFailureEventually(
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
