package process_test

import (
	"context"
	"testing"

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
					c.Identity("<handler>", "5ca74d02-8dde-4148-8f35-cec0e4c3b241")
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
					c.Identity("<handler>", "ef0660b4-a68e-4383-b156-5857ac294dce")
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

// TestInstanceState_writesAreSerialized verifies that concurrent events from
// multiple streams targeting the same process instance are serialized, such
// that each event observes the cumulative state produced by all prior events.
func TestInstanceState_writesAreSerialized(t *testing.T) {
	const (
		streamCount     = 4
		eventsPerStream = 5
		totalEvents     = streamCount * eventsPerStream
	)

	var done xsync.Latch

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

			xtesting.ExpectLatchesSetEventually(t, &done)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", "ef0660b4-a68e-4383-b156-5857ac294dce")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
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
					// Each event increments the counter. If writes are not
					// serialized, concurrent handlers would read the same
					// counter value and produce a final count less than
					// totalEvents.
					var n int
					if r.Value != nil {
						// JSON unmarshals numbers as float64 when the target
						// type is any.
						n = int(r.Value.(float64))
					}
					n++

					s.Mutate(func(root *stubs.ProcessRootStub) {
						root.Value = n
					})

					if n == totalEvents {
						done.Set()
					}

					return nil
				},
			},
		),
	)
}
