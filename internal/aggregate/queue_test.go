package aggregate_test

import (
	"database/sql"
	"sync/atomic"
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

func TestInvalidCommandsAreBackedOff(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Execute an invalid command so that it will be backed off.
			invalidCommandEnvelope := xtesting.ExecuteCommandWithHook(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{},
				func(x contexthook.ExecuteCommand) {
					// Corrupt the command so that it cannot be unpacked.
					x.CommandEnvelope.GetBody().GetMessage().SetData([]byte("<invalid>"))
				},
			)

			// Execute a valid command to verify that the backed-off command
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

			xtesting.ExpectCommandToBeBackedOffDueToFailureEventually(
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

func TestCommandIsBackedOffIfStateCannotBeLoaded(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Execute a command to create the instance and record an
			// event in its history.
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{},
			)

			xtesting.ExpectEmptyCommandQueueEventually(
				t,
				engine.DB,
			)

			// Corrupt the stored event envelope so that it cannot be
			// parsed, and clear the snapshot so the engine must attempt
			// to replay the corrupt event.
			xtesting.Transact(
				t,
				engine.DB,
				func(tx *sql.Tx) {
					xtesting.ExecOne(
						t,
						tx,
						`UPDATE eventstream.events SET
							envelope = '\x00'::bytea
						WHERE aggregate_handler_key = 'ef0660b4-a68e-4383-b156-5857ac294dce'
						AND aggregate_instance_id = '<instance>'`,
					)
					xtesting.ExecOne(
						t,
						tx,
						`UPDATE aggregate.instances SET
							snapshot = NULL,
							snapshot_offset = NULL
						WHERE handler_key = 'ef0660b4-a68e-4383-b156-5857ac294dce'
						AND instance_id = '<instance>'`,
					)
				},
			)

			// Execute another command to the same instance.
			commandEnvelope := xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{},
			)

			// The command should be backed off because the instance
			// state cannot be loaded.
			xtesting.ExpectCommandToBeBackedOffDueToFailureEventually(
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
					s.RecordEvent(&stubs.EventStub[stubs.TypeA]{})
				},
			},
		),
	)
}

func TestCommandIsBackedOffWhenApplicationCodePanics(t *testing.T) {
	t.Run("in RouteCommandToInstance()", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *dogmaengine.Engine) {
				commandEnvelope := xtesting.ExecuteCommand(
					t,
					engine,
					&stubs.CommandStub[stubs.TypeA]{},
				)

				xtesting.ExpectCommandToBeBackedOffDueToFailureEventually(
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

	t.Run("in HandleCommand()", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *dogmaengine.Engine) {
				commandEnvelope := xtesting.ExecuteCommand(
					t,
					engine,
					&stubs.CommandStub[stubs.TypeA]{},
				)

				xtesting.ExpectCommandToBeBackedOffDueToFailureEventually(
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

	t.Run("in ApplyEvent()", func(t *testing.T) {
		// eventApplied is set to true after the first successful call to
		// ApplyEvent. The first call occurs within scope.RecordEvent during
		// HandleCommand; subsequent calls occur during state loading when
		// replaying historical events. Only those subsequent calls panic,
		// ensuring the first command succeeds (creating history) while the
		// second command fails during state replay.
		var eventApplied atomic.Bool

		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *dogmaengine.Engine) {
				// Execute a command to create the instance and record an
				// event in its history.
				xtesting.ExecuteCommand(
					t,
					engine,
					&stubs.CommandStub[stubs.TypeA]{},
				)

				xtesting.ExpectEmptyCommandQueueEventually(
					t,
					engine.DB,
				)

				// Execute another command to the same instance. When
				// loading state, replaying the event will panic.
				commandEnvelope := xtesting.ExecuteCommand(
					t,
					engine,
					&stubs.CommandStub[stubs.TypeA]{},
				)

				xtesting.ExpectCommandToBeBackedOffDueToFailureEventually(
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
								if !eventApplied.CompareAndSwap(false, true) {
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
						s.RecordEvent(&stubs.EventStub[stubs.TypeA]{})
					},
				},
			),
		)
	})
}
