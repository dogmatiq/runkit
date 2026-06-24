package aggregate_test

import (
	"sync/atomic"
	"testing"
	"time"

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
}

// TestCommandQueue_invalidHistoricalEventCausesCommandToBePostponed verifies
// that if an event in an instance's history cannot be unpacked, commands that
// target the instance are postponed.
func TestCommandQueue_invalidHistoricalEventCausesCommandToBePostponed(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Execute a command to create the instance and record an
			// event in its history.
			xtesting.ExecuteCommandAndWait(t, engine, stubs.CommandA1)

			// Corrupt the stored event envelope so that it cannot be
			// unmarshaled.
			xtesting.ExecOne(
				t,
				engine.DB,
				`UPDATE eventstream.events SET
					envelope = '\x00'::bytea
				WHERE aggregate_handler_key = 'ef0660b4-a68e-4383-b156-5857ac294dce'
				AND aggregate_instance_id = '<instance>'`,
			)

			// Clear the instance's snapshot so the engine must attempt to
			// replay the corrupt event.
			xtesting.ExecOne(
				t,
				engine.DB,
				`UPDATE aggregate.instances SET
					snapshot = NULL,
					snapshot_offset = NULL
				WHERE handler_key = 'ef0660b4-a68e-4383-b156-5857ac294dce'
				AND instance_id = '<instance>'`,
			)

			// Execute another command that targets the same instance.
			commandEnvelope := xtesting.ExecuteCommand(t, engine, stubs.CommandA1)

			xtesting.WaitForCommandToBePostponed(
				t,
				engine.DB,
				commandEnvelope.GetBody().GetMessageId(),
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
				RouteCommandToInstanceFunc: func(dogma.Command) string {
					return "<instance>"
				},
				HandleCommandFunc: func(
					r *stubs.AggregateRootStub,
					s dogma.AggregateCommandScope[*stubs.AggregateRootStub],
					m dogma.Command,
				) {
					s.RecordEvent(stubs.EventA1)
				},
			},
		),
	)
}

// TestCommandQueue_applicationCodePanicsCauseCommandToBePostponed verifies that
// if handling a command causes the application code to panic, the command is
// postponed.
func TestCommandQueue_applicationCodePanicsCauseCommandToBePostponed(t *testing.T) {
	t.Run("panic in RouteCommandToInstance()", func(t *testing.T) {
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
						panic("<panic>")
					},
				},
			),
		)
	})

	t.Run("panic in HandleCommand()", func(t *testing.T) {
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
					HandleCommandFunc: func(
						*stubs.AggregateRootStub,
						dogma.AggregateCommandScope[*stubs.AggregateRootStub],
						dogma.Command,
					) {
						panic("<panic>")
					},
				},
			),
		)
	})

	t.Run("panic in ApplyEvent() for historical event", func(t *testing.T) {
		// The first call to ApplyEvent() occurs within RecordEvent() during
		// HandleCommand(). We need the panic to occur in the second call to
		// ApplyEvent() to ensure that it occurs during replay of historical
		// events.
		//
		// shouldPanic is set to true after the first successful call to
		// ApplyEvent().
		var shouldPanic atomic.Bool

		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *dogmaengine.Engine) {
				// Execute a command to create the instance and record an event
				// in its history.
				xtesting.ExecuteCommandAndWait(t, engine, stubs.CommandA1)

				// Execute another command that targets the same instance. When
				// loading state, replaying the event will panic.
				commandEnvelope := xtesting.ExecuteCommand(t, engine, stubs.CommandA1)

				xtesting.WaitForCommandToBePostponed(
					t,
					engine.DB,
					commandEnvelope.GetBody().GetMessageId(),
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
					NewFunc: func() *stubs.AggregateRootStub {
						return &stubs.AggregateRootStub{
							ApplyEventFunc: func(dogma.Event) {
								if !shouldPanic.CompareAndSwap(false, true) {
									panic("<panic>")
								}
							},
						}
					},
					RouteCommandToInstanceFunc: func(dogma.Command) string {
						return "<instance>"
					},
					HandleCommandFunc: func(
						_ *stubs.AggregateRootStub,
						s dogma.AggregateCommandScope[*stubs.AggregateRootStub],
						_ dogma.Command,
					) {
						s.RecordEvent(stubs.EventA1)
					},
				},
			),
		)
	})
}

// TestCommandQueue_postponedCommandsAreNotHandled verifies that commands with
// deliver_at in the future are not dispatched to the handler.
func TestCommandQueue_postponedCommandsAreNotHandled(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			postponedEnvelope := xtesting.EnqueuePostponedCommand(
				t,
				engine.DB,
				stubs.CommandA1,
			)

			// Allow several poll cycles to pass.
			time.Sleep(50 * time.Millisecond)

			xtesting.ExpectCommandToBeUnattempted(
				t,
				engine.DB,
				postponedEnvelope.GetBody().GetMessageId(),
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
				RouteCommandToInstanceFunc: func(dogma.Command) string {
					return "<instance>"
				},
				HandleCommandFunc: func(
					_ *stubs.AggregateRootStub,
					s dogma.AggregateCommandScope[*stubs.AggregateRootStub],
					_ dogma.Command,
				) {
					t.Error("handler was called for the postponed command")
				},
			},
		),
	)
}
