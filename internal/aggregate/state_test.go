package aggregate_test

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestEventsAreAppliedToInMemoryAggregateRoot(t *testing.T) {
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
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeB]](),
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
					s.RecordEvent(&stubs.EventStub[stubs.TypeA]{
						Content: "<content-a>",
					})

					// Verify that the first event is applied
					// immediately after recording it.
					want := []dogma.Event{
						&stubs.EventStub[stubs.TypeA]{
							Content: "<content-a>",
						},
					}

					if !reflect.DeepEqual(r.AppliedEvents, want) {
						t.Errorf(
							"unexpected aggregate state: %#v",
							r.AppliedEvents,
						)
					}

					s.RecordEvent(&stubs.EventStub[stubs.TypeB]{
						Content: "<content-b>",
					})

					// Verify that the second event is applied
					// immediately after recording it.
					want = []dogma.Event{
						&stubs.EventStub[stubs.TypeA]{
							Content: "<content-a>",
						},
						&stubs.EventStub[stubs.TypeB]{
							Content: "<content-b>",
						},
					}

					if !reflect.DeepEqual(r.AppliedEvents, want) {
						t.Errorf(
							"unexpected aggregate state: %#v",
							r.AppliedEvents,
						)
					}
				},
			},
		),
	)
}

func TestStateChangesArePersisted(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Force creation of multiple event streams so that the controller
			// doesn't just create a single stream and use it continuously.
			xtesting.CreateEventStreams(t, engine.DB, 3)

			// Send TypeA command to append a TypeA event.
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{},
			)

			// Wait until the first command is handled before sending the
			// second command, to ensure that event history order is
			// deterministic.
			xtesting.ExpectEmptyCommandQueueEventually(
				t,
				engine.DB,
			)

			// Send TypeB command to append a TypeB event.
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeB]{},
			)

			// Wait until the second command is handled before making
			// assertions about the aggregate state.
			xtesting.ExpectEmptyCommandQueueEventually(
				t,
				engine.DB,
			)

			// Send TypeC command which asserts about the aggregate state
			// within the handler.
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeC]{},
			)

			// Wait until the last command is handled once more so that we
			// don't end the test before the assertions are executed.
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
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeB]](),
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeC]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeB]](),
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
					switch m.(type) {

					// When we receive a TypeA command, record a TypeA event.
					case *stubs.CommandStub[stubs.TypeA]:
						s.RecordEvent(&stubs.EventStub[stubs.TypeA]{
							Content: "<content-a>",
						})

					// When we receive a TypeB command, record a TypeB event.
					case *stubs.CommandStub[stubs.TypeB]:
						s.RecordEvent(&stubs.EventStub[stubs.TypeB]{
							Content: "<content-b>",
						})

					// When we receive a TypeC command, check that
					// the aggregate state includes the previously
					// recorded events in the correct order.
					case *stubs.CommandStub[stubs.TypeC]:
						want := []dogma.Event{
							&stubs.EventStub[stubs.TypeA]{
								Content: "<content-a>",
							},
							&stubs.EventStub[stubs.TypeB]{
								Content: "<content-b>",
							},
						}

						if !reflect.DeepEqual(r.AppliedEvents, want) {
							t.Errorf(
								"unexpected aggregate state: %#v",
								r.AppliedEvents,
							)
						}

					default:
						panic(dogma.UnexpectedMessage)
					}
				},
			},
		),
	)
}

func TestStateIsIsolatedToCorrectInstance(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Send TypeA commands to append a TypeA events for two
			// different instances.
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{
					Content: "<instance-a>",
				},
			)
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{
					Content: "<instance-b>",
				},
			)

			// Wait until the commands are handled before making assertions
			// about the aggregate state.
			xtesting.ExpectEmptyCommandQueueEventually(
				t,
				engine.DB,
			)

			// Send TypeB commands which assert about the aggregate state
			// within the handler.
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeB]{
					Content: "<instance-a>",
				},
			)
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeB]{
					Content: "<instance-b>",
				},
			)

			// Wait until the commands are handled once more so that we
			// don't end the test before the assertions are executed.
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
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeB]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				RouteCommandToInstanceFunc: func(m dogma.Command) string {
					switch m := m.(type) {
					case *stubs.CommandStub[stubs.TypeA]:
						return "instance:" + string(m.Content)
					case *stubs.CommandStub[stubs.TypeB]:
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
					switch m.(type) {

					// When we receive a TypeA command, record a TypeA event.
					case *stubs.CommandStub[stubs.TypeA]:
						s.RecordEvent(&stubs.EventStub[stubs.TypeA]{
							Content: stubs.TypeA("recorded against instance: " + s.InstanceID()),
						})

					// When we receive a TypeB command, check that
					// the aggregate state includes only the event
					// that was recorded for this instance.
					case *stubs.CommandStub[stubs.TypeB]:
						want := []dogma.Event{
							&stubs.EventStub[stubs.TypeA]{
								Content: stubs.TypeA("recorded against instance: " + s.InstanceID()),
							},
						}

						if !reflect.DeepEqual(r.AppliedEvents, want) {
							t.Errorf(
								"unexpected aggregate state: %#v",
								r.AppliedEvents,
							)
						}

					default:
						panic(dogma.UnexpectedMessage)
					}
				},
			},
		),
	)
}

func TestMutationsToASingleInstanceAreSerialized(t *testing.T) {
	const commandCount = 20

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Send many TypeA commands at once without waiting between
			// them, so that concurrent engines may attempt to process
			// multiple commands for the same instance simultaneously.
			for range commandCount {
				xtesting.ExecuteCommand(
					t,
					engine,
					&stubs.CommandStub[stubs.TypeA]{},
				)
			}

			// Wait until all TypeA commands are handled before making
			// assertions about the aggregate state.
			xtesting.ExpectEmptyCommandQueueEventually(
				t,
				engine.DB,
			)

			// Send TypeB command which asserts about the aggregate state
			// within the handler.
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeB]{},
			)

			// Wait until the last command is handled once more so that we
			// don't end the test before the assertions are executed.
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
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeB]](),
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
					switch m.(type) {

					// When we receive a TypeA command, record an
					// event that captures how many events the
					// handler observed at the time it was called.
					case *stubs.CommandStub[stubs.TypeA]:
						s.RecordEvent(&stubs.EventStub[stubs.TypeA]{
							Content: stubs.TypeA(fmt.Sprintf("%d", len(r.AppliedEvents))),
						})

					// When we receive a TypeB command, verify that
					// each event recorded the expected sequential
					// count, proving that each command observed the
					// state produced by all prior commands.
					case *stubs.CommandStub[stubs.TypeB]:
						if got := len(r.AppliedEvents); got != commandCount {
							t.Errorf(
								"unexpected event count: got %d, want %d",
								got,
								commandCount,
							)
						}

						for idx, event := range r.AppliedEvents {
							want := stubs.TypeA(fmt.Sprintf("%d", idx))
							got := event.(*stubs.EventStub[stubs.TypeA]).Content

							if got != want {
								t.Errorf(
									"unexpected event content at index %d: got %q, want %q",
									idx,
									got,
									want,
								)
							}
						}

					default:
						panic(dogma.UnexpectedMessage)
					}
				},
			},
		),
	)
}

func TestSnapshotIsPersistedAfterEveryCommand(t *testing.T) {
	// Prove that a snapshot is persisted after handling each command by
	// deleting the historical events after the first command is handled. If
	// the snapshot was persisted, the state is still correct when the next
	// command is handled; if not, the missing events cause incorrect state.
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Execute a command to create the instance and record an event.
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{},
			)

			xtesting.ExpectEmptyCommandQueueEventually(
				t,
				engine.DB,
			)

			// Delete the historical events. If a snapshot was persisted after
			// handling the command above, the state will still be correct.
			xtesting.Transact(
				t,
				engine.DB,
				func(tx *sql.Tx) {
					xtesting.ExecOne(
						t,
						tx,
						`DELETE FROM eventstream.events
						WHERE aggregate_handler_key = 'ef0660b4-a68e-4383-b156-5857ac294dce'
						AND aggregate_instance_id = '<instance>'`,
					)
				},
			)

			// Execute another command that asserts the state was loaded
			// correctly from the snapshot (not by replaying the now-deleted
			// events).
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeB]{},
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
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeB]](),
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
					switch m.(type) {
					case *stubs.CommandStub[stubs.TypeA]:
						s.RecordEvent(&stubs.EventStub[stubs.TypeA]{
							Content: "<content>",
						})

					case *stubs.CommandStub[stubs.TypeB]:
						want := []dogma.Event{
							&stubs.EventStub[stubs.TypeA]{
								Content: "<content>",
							},
						}

						if !reflect.DeepEqual(r.AppliedEvents, want) {
							t.Errorf(
								"unexpected aggregate state: got %#v, want %#v",
								r.AppliedEvents,
								want,
							)
						}

					default:
						panic(dogma.UnexpectedMessage)
					}
				},
			},
		),
	)
}

func TestSnapshotMarshalingErrorsAreNonFatal(t *testing.T) {
	// If MarshalBinary() fails, the command should still be handled
	// successfully, the snapshot just won't be persisted. We verify this by
	// sending two commands: if the first succeeds (queue empties), then the
	// marshaling failure was non-fatal; and since no snapshot is persisted we
	// expect the second command to see state rebuilt from event replay.
	cases := []struct {
		Name              string
		MarshalBinaryFunc func() ([]byte, error)
	}{
		{
			"returns error",
			func() ([]byte, error) {
				return nil, fmt.Errorf("<marshal error>")
			},
		},
		{
			"panics",
			func() ([]byte, error) {
				panic("<marshal panic>")
			},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
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

					xtesting.ExecuteCommand(
						t,
						engine,
						&stubs.CommandStub[stubs.TypeB]{},
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
								dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeB]](),
								dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
							)
						},
						NewFunc: func() *stubs.AggregateRootStub {
							return &stubs.AggregateRootStub{
								MarshalBinaryFunc: c.MarshalBinaryFunc,
							}
						},
						RouteCommandToInstanceFunc: func(dogma.Command) string {
							return "<instance>"
						},
						HandleCommandFunc: func(
							r *stubs.AggregateRootStub,
							s dogma.AggregateCommandScope[*stubs.AggregateRootStub],
							m dogma.Command,
						) {
							switch m.(type) {
							case *stubs.CommandStub[stubs.TypeA]:
								s.RecordEvent(&stubs.EventStub[stubs.TypeA]{
									Content: "<content>",
								})

							case *stubs.CommandStub[stubs.TypeB]:
								want := []dogma.Event{
									&stubs.EventStub[stubs.TypeA]{
										Content: "<content>",
									},
								}

								if !reflect.DeepEqual(r.AppliedEvents, want) {
									t.Errorf(
										"unexpected aggregate state: got %#v, want %#v",
										r.AppliedEvents,
										want,
									)
								}

							default:
								panic(dogma.UnexpectedMessage)
							}
						},
					},
				),
			)
		})
	}
}

func TestSnapshotUnmarshalingErrorsAreNonFatal(t *testing.T) {
	// If UnmarshalBinary() fails when loading a snapshot, the engine should
	// fall back to replaying all historical events. We verify this by sending
	// two commands: the first persists a snapshot, and the second encounters
	// the unmarshal failure but still sees correct state from event replay.
	cases := []struct {
		Name                string
		UnmarshalBinaryFunc func([]byte) error
	}{
		{
			"returns error",
			func([]byte) error {
				return fmt.Errorf("<unmarshal error>")
			},
		},
		{
			"panics",
			func([]byte) error {
				panic("<unmarshal panic>")
			},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
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

					xtesting.ExecuteCommand(
						t,
						engine,
						&stubs.CommandStub[stubs.TypeB]{},
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
								dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeB]](),
								dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
							)
						},
						NewFunc: func() *stubs.AggregateRootStub {
							return &stubs.AggregateRootStub{
								UnmarshalBinaryFunc: c.UnmarshalBinaryFunc,
							}
						},
						RouteCommandToInstanceFunc: func(dogma.Command) string {
							return "<instance>"
						},
						HandleCommandFunc: func(
							r *stubs.AggregateRootStub,
							s dogma.AggregateCommandScope[*stubs.AggregateRootStub],
							m dogma.Command,
						) {
							switch m.(type) {
							case *stubs.CommandStub[stubs.TypeA]:
								s.RecordEvent(&stubs.EventStub[stubs.TypeA]{
									Content: "<content>",
								})

							case *stubs.CommandStub[stubs.TypeB]:
								want := []dogma.Event{
									&stubs.EventStub[stubs.TypeA]{
										Content: "<content>",
									},
								}

								if !reflect.DeepEqual(r.AppliedEvents, want) {
									t.Errorf(
										"unexpected aggregate state: got %#v, want %#v",
										r.AppliedEvents,
										want,
									)
								}

							default:
								panic(dogma.UnexpectedMessage)
							}
						},
					},
				),
			)
		})
	}
}
