package process_test

import (
	"context"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

// TestInstanceState_stateIsNotPersistedIfMutateIsNotUsed verifies that the
// process instance state is not persisted if the [dogma.ProcessScope].Mutate()
// method is not called.
func TestInstanceState_stateIsNotPersistedIfMutateIsNotUsed(t *testing.T) {
	var done xsync.Latch

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					switch offset {
					case 0:
						return stubs.EventA1
					default:
						return stubs.EventX1
					}
				},
				2,
			)

			xtesting.ExpectLatchesSetEventually(t, &done)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", "5ca74d02-8dde-4148-8f35-cec0e4c3b241")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeX]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
					)
				},
				RouteEventToInstanceFunc: func(
					context.Context,
					dogma.Event,
				) (string, bool, error) {
					return "<instance>", true, nil
				},
				HandleEventFunc: func(
					_ context.Context,
					r *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					m dogma.Event,
				) error {
					switch m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						// Modify the root directly without calling s.Mutate().
						r.Value = "A"
					case *stubs.EventStub[stubs.TypeX]:
						defer done.Set()

						if r.Value != nil {
							t.Errorf("process state was persisted without Mutate(): got %v, want nil", r.Value)
						}
					default:
						panic(dogma.UnexpectedMessage)
					}

					return nil
				},
			},
		),
	)
}

// TestInstanceState_stateIsPersisted verifies that the process instance state
// is correctly persisted across multiple events that target that instance.
func TestInstanceState_stateIsPersisted(t *testing.T) {
	var done xsync.Latch

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					switch offset {
					case 0:
						return stubs.EventA1
					case 1:
						return stubs.EventB1
					default:
						return stubs.EventX1
					}
				},
				3, // one stream, three events
			)

			xtesting.ExpectLatchesSetEventually(t, &done)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", "dbe022b0-b52c-489a-b855-638e68f6b709")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeX]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
					)
				},
				RouteEventToInstanceFunc: func(
					context.Context,
					dogma.Event,
				) (string, bool, error) {
					return "<instance>", true, nil
				},
				HandleEventFunc: func(
					_ context.Context,
					r *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					m dogma.Event,
				) error {
					switch m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						s.Mutate(func(root *stubs.ProcessRootStub) {
							root.Value = "A"
						})
					case *stubs.EventStub[stubs.TypeB]:
						s.Mutate(func(root *stubs.ProcessRootStub) {
							root.Value = root.Value.(string) + "B"
						})
					case *stubs.EventStub[stubs.TypeX]:
						defer done.Set()

						if got, want := r.Value, "AB"; got != want {
							t.Errorf("process state is not persisted between events: got %v, want %v", got, want)
						}
					default:
						panic(dogma.UnexpectedMessage)
					}

					return nil
				},
			},
		),
	)
}

// TestInstanceState_instancesAreIsolated verifies that the state of one
// instance is not affected by events handled by another instance.
func TestInstanceState_instancesAreIsolated(t *testing.T) {
	var (
		instanceAChecked xsync.Latch
		instanceBChecked xsync.Latch
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					switch offset {
					case 0:
						return &stubs.EventStub[stubs.TypeA]{Content: "A"} // handled by instance A
					case 1:
						return &stubs.EventStub[stubs.TypeA]{Content: "B"} // handled by instance B
					case 2:
						return &stubs.EventStub[stubs.TypeX]{Content: "A"} // assert about instance A
					default:
						return &stubs.EventStub[stubs.TypeX]{Content: "B"} // assert about instance B
					}
				},
				4,
			)

			xtesting.ExpectLatchesSetEventually(
				t,
				&instanceAChecked,
				&instanceBChecked,
			)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", "2b711639-5838-429d-9884-7c687bb06a75")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeX]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
					)
				},
				RouteEventToInstanceFunc: func(
					_ context.Context,
					m dogma.Event,
				) (string, bool, error) {
					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						return "instance:" + string(m.Content), true, nil
					case *stubs.EventStub[stubs.TypeX]:
						return "instance:" + string(m.Content), true, nil
					default:
						panic(dogma.UnexpectedMessage)
					}
				},
				HandleEventFunc: func(
					_ context.Context,
					r *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					m dogma.Event,
				) error {
					switch m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						s.Mutate(func(root *stubs.ProcessRootStub) {
							root.Value = s.InstanceID()
						})
					case *stubs.EventStub[stubs.TypeX]:
						if s.InstanceID() == "instance:A" {
							defer instanceAChecked.Set()
						} else {
							defer instanceBChecked.Set()
						}

						if got, want := r.Value, s.InstanceID(); got != want {
							t.Errorf("process state is not isolated per instance: got %v, want %v", got, want)
						}
					default:
						panic(dogma.UnexpectedMessage)
					}

					return nil
				},
			},
		),
	)
}

// TestInstanceState_writesAreSerialized verifies that concurrent events and
// deadlines targeting the same process instance are serialized, such that each
// handler invocation observes the cumulative state produced by all prior
// invocations, regardless of whether they originated from the event pump or the
// deadline pump.
func TestInstanceState_writesAreSerialized(t *testing.T) {
	const handlerKey = "d5fce239-6c7f-4153-895c-ecb047693319"

	const (
		streamCount     = 4
		eventsPerStream = 5
		totalEvents     = streamCount * eventsPerStream
		totalIncrements = totalEvents * 2 // each event, plus one deadline per event
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			counts := make([]uint64, streamCount)
			for i := range counts {
				counts[i] = eventsPerStream
			}

			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					return stubs.EventA1
				},
				counts...,
			)

			// Wait for all events to be consumed before publishing the final
			// event that will assert that the process state is cumulative
			// across all events and deadlines.
			xtesting.WaitForHandlerToConsumeAllEvents(t, engine.DB, handlerKey)
			xtesting.WaitForNoPendingDeadlines(t, engine.DB)

			// Publish the final event that will assert that the process state
			// is cumulative across all events and deadlines.
			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventX1
				},
				1,
			)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", handlerKey)
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeX]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
						dogma.SchedulesDeadline[*stubs.DeadlineStub[stubs.TypeA]](),
					)
				},
				RouteEventToInstanceFunc: func(
					context.Context,
					dogma.Event,
				) (string, bool, error) {
					return "<instance>", true, nil
				},
				HandleEventFunc: func(
					_ context.Context,
					r *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					e dogma.Event,
				) error {
					switch e.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						s.Mutate(func(root *stubs.ProcessRootStub) {
							if r.Value == nil {
								r.Value = 1
							} else {
								r.Value = r.Value.(float64) + 1
							}
						})

						// Schedule a deadline for immediate delivery so that the
						// deadline pump and event pump race on the same instance.
						s.ScheduleDeadline(stubs.DeadlineA1, time.Now())

					case *stubs.EventStub[stubs.TypeX]:
						if got, want := r.Value, float64(totalIncrements); got != want {
							t.Errorf("process state is not cumulative across events and deadlines: got %v, want %v", got, want)
						}

					default:
						panic(dogma.UnexpectedMessage)
					}

					return nil
				},
				HandleDeadlineFunc: func(
					_ context.Context,
					r *stubs.ProcessRootStub,
					s dogma.ProcessDeadlineScope[*stubs.ProcessRootStub],
					_ dogma.Deadline,
				) error {
					s.Mutate(func(root *stubs.ProcessRootStub) {
						if r.Value == nil {
							r.Value = 1
						} else {
							r.Value = r.Value.(float64) + 1
						}
					})

					return nil
				},
			},
		),
	)
}
