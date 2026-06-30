package aggregate_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

// TestInstanceState_eventsAreAppliedInMemory verifies that events are applied
// to the aggregate root immediately after they are recorded, within the same
// command scope.
func TestInstanceState_eventsAreAppliedInMemory(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExecuteCommandAndWait(t, engine, stubs.CommandA1)
		},
		dogma.ViaAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler>", "cd577a96-cd1d-43b1-9686-e382e498c434")
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
					s.RecordEvent(stubs.EventA1)

					// Verify that the first event is applied immediately after
					// recording it.
					xtesting.ExpectEqualEvents(
						t,
						"events are applied immediately",
						r.AppliedEvents,
						stubs.EventA1,
					)

					s.RecordEvent(stubs.EventA2)

					// Verify that the second event is applied immediately after
					// recording it.
					xtesting.ExpectEqualEvents(
						t,
						"events are applied immediately",
						r.AppliedEvents,
						stubs.EventA1,
						stubs.EventA2,
					)
				},
			},
		),
	)
}

// TestInstanceState_stateIsPersisted verifies that the aggregate instance state
// is correctly persisted across multiple commands that target that instance.
func TestInstanceState_stateIsPersisted(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExecuteCommandsSequentially(
				t,
				engine,
				stubs.CommandA1, // record a TypeA event
				stubs.CommandB1, // record a TypeB event
				stubs.CommandX1, // assert about the aggregate state within the handler
			)
		},
		dogma.ViaAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler>", "a026c7b5-b464-4104-b1b8-cd25aacd1026")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeB]](),
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeX]](),
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
					case *stubs.CommandStub[stubs.TypeA]:
						s.RecordEvent(stubs.EventA1)
					case *stubs.CommandStub[stubs.TypeB]:
						s.RecordEvent(stubs.EventB1)
					case *stubs.CommandStub[stubs.TypeX]:
						xtesting.ExpectEqualEvents(
							t,
							"aggregate state is persisted between commands",
							r.AppliedEvents,
							stubs.EventA1,
							stubs.EventB1,
						)
					default:
						panic(dogma.UnexpectedMessage)
					}
				},
			},
		),
	)
}

// TestInstanceState_instancesAreIsolated verifies that the state of one
// instance is not affected by events recorded for another instance.
func TestInstanceState_instancesAreIsolated(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExecuteCommandsSequentially(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{Content: "A"}, // record an event against instance A
				&stubs.CommandStub[stubs.TypeA]{Content: "B"}, // record an event against instance B
				&stubs.CommandStub[stubs.TypeX]{Content: "A"}, // assert about the aggregate state within the handler
				&stubs.CommandStub[stubs.TypeX]{Content: "B"}, // assert about the aggregate state within the handler
			)
		},
		dogma.ViaAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler>", "ddc5eff8-0eaa-46db-9a25-8b3d6c455639")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeX]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				RouteCommandToInstanceFunc: func(m dogma.Command) string {
					switch m := m.(type) {
					case *stubs.CommandStub[stubs.TypeA]:
						return "instance:" + string(m.Content)
					case *stubs.CommandStub[stubs.TypeX]:
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
					case *stubs.CommandStub[stubs.TypeA]:
						s.RecordEvent(&stubs.EventStub[stubs.TypeA]{
							Content: stubs.TypeA(s.InstanceID()),
						})
					case *stubs.CommandStub[stubs.TypeX]:
						xtesting.ExpectEqualEvents(
							t,
							"aggregate state is isolated per instance",
							r.AppliedEvents,
							&stubs.EventStub[stubs.TypeA]{
								Content: stubs.TypeA(s.InstanceID()),
							},
						)
					default:
						panic(dogma.UnexpectedMessage)
					}
				},
			},
		),
	)
}

// TestInstanceState_writesAreSerialized verifies that concurrent commands
// targeting the same aggregate instance are serialized, such that each command
// observes the cumulative state produced by all prior commands.
func TestInstanceState_writesAreSerialized(t *testing.T) {
	const commandCount = 20

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Send many commands at once without waiting between them, the test
			// is running multiple engines, which may attempt to process
			// commands simultaneously. All commands are routed to the same
			// instance.
			for range commandCount {
				xtesting.ExecuteCommand(t, engine, stubs.CommandA1)
			}

			xtesting.WaitForEmptyCommandQueue(t, engine.DB)
		},
		dogma.ViaAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler>", "35782b17-cb1b-4099-9538-4249287ce65b")
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
					switch m.(type) {
					case *stubs.CommandStub[stubs.TypeA]:
						// Record an event with content that reflects the number
						// of historical events so far.
						s.RecordEvent(&stubs.EventStub[stubs.TypeA]{
							Content: stubs.TypeA(strconv.Itoa(len(r.AppliedEvents))),
						})

						// Assert that the historical events have been applied
						// to the aggregate instance in order.
						for idx, event := range r.AppliedEvents {
							got := event.(*stubs.EventStub[stubs.TypeA]).Content
							want := stubs.TypeA(strconv.Itoa(idx))

							if got != want {
								t.Errorf(
									"aggregate state is not serialized: unexpected event content at index %d: got %q, want %q",
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

// TestInstanceState_snapshotIsTakenAfterEveryCommand verifies that a snapshot
// of the aggregate instance state is persisted after handling each command.
//
// We prove that a snapshot is persisted after handling each command by deleting
// the historical events after the first command is handled. If the snapshot was
// persisted, the state is still correct when the next command is handled; if
// not, the missing events cause incorrect state.
func TestInstanceState_snapshotIsTakenAfterEveryCommand(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Execute a command to create the instance and record an event.
			xtesting.ExecuteCommandAndWait(t, engine, stubs.CommandA1)

			// Delete the historical events. If a snapshot was persisted after
			// handling the command above, the state will still be correct.
			xtesting.ExecOne(
				t,
				engine.DB,
				`DELETE FROM eventstream.events`,
			)

			// Execute another command that asserts the state was loaded
			// correctly from the snapshot (not by replaying the now-deleted
			// events).
			xtesting.ExecuteCommandAndWait(t, engine, stubs.CommandX1)
		},
		dogma.ViaAggregate(
			&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<handler>", "9ca20b0e-98cf-4249-8b7f-f6be581b57b0")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeX]](),
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
						s.RecordEvent(stubs.EventA1)
					case *stubs.CommandStub[stubs.TypeX]:
						xtesting.ExpectEqualEvents(
							t,
							"aggregate state is restored from snapshot",
							r.AppliedEvents,
							stubs.EventA1,
						)
					default:
						panic(dogma.UnexpectedMessage)
					}
				},
			},
		),
	)
}

// TestInstanceState_snapshotMarshalingFailuresAreNonFatal verifies that if
// MarshalBinary() fails when persisting a snapshot, the command is still
// handled successfully, and the snapshot is simply ignored or not persisted at
// all.
func TestInstanceState_snapshotMarshalingFailuresAreNonFatal(t *testing.T) {
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
					xtesting.ExecuteCommandsSequentially(
						t,
						engine,
						stubs.CommandA1, // record an event event
						stubs.CommandX1, // assert about the aggregate state within the handler
					)
				},
				dogma.ViaAggregate(
					&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
						ConfigureFunc: func(c dogma.AggregateConfigurer) {
							c.Identity("<handler>", "9ebb0074-2351-4086-abfa-fc576a6620e5")
							c.Routes(
								dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
								dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeX]](),
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
								s.RecordEvent(stubs.EventA1)
							case *stubs.CommandStub[stubs.TypeX]:
								xtesting.ExpectEqualEvents(
									t,
									"aggregate state is rebuilt from event replay",
									r.AppliedEvents,
									stubs.EventA1,
								)
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

// TestInstanceState_snapshotUnmarshalingFailuresAreNonFatal verifies that if
// UnmarshalBinary() fails when loading a snapshot, the engine falls back to
// replaying all historical events, and the command is still handled
// successfully, with correct state from event replay.
func TestInstanceState_snapshotUnmarshalingFailuresAreNonFatal(t *testing.T) {
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
					xtesting.ExecuteCommandsSequentially(
						t,
						engine,
						stubs.CommandA1, // record an event event
						stubs.CommandX1, // assert about the aggregate state within the handler
					)
				},
				dogma.ViaAggregate(
					&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
						ConfigureFunc: func(c dogma.AggregateConfigurer) {
							c.Identity("<handler>", "d1e64375-6ff8-4f97-aa91-71a5ee4f022b")
							c.Routes(
								dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
								dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeX]](),
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
								s.RecordEvent(stubs.EventA1)
							case *stubs.CommandStub[stubs.TypeX]:
								xtesting.ExpectEqualEvents(
									t,
									"aggregate state is rebuilt from event replay",
									r.AppliedEvents,
									stubs.EventA1,
								)
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
