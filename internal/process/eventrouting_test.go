package process_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

// TestEventRouting_eventsFromTheSameStreamAreRoutedToTheCorrectHandler verifies
// that events of different types on the same stream are routed to the correct
// handler based on their message type.
func TestEventRouting_eventsFromTheSameStreamAreRoutedToTheCorrectHandler(t *testing.T) {
	var (
		handlerACalled xsync.Latch
		handlerBCalled xsync.Latch
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					if offset == 0 {
						return stubs.EventA1
					}
					return stubs.EventB1
				},
				2, // one stream, two events of different types
			)

			xtesting.ExpectLatchesSetEventually(
				t,
				&handlerACalled,
				&handlerBCalled,
			)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler-a>", "4da45c75-cfea-45f6-aa9e-a59e874f1106")
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
					_ *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					m dogma.Event,
				) error {
					defer handlerACalled.Set()

					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
					default:
						t.Errorf("unexpected event type routed to handler-a: %T", m)
					}

					return nil
				},
			},
		),
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler-b>", "33670b6d-b013-4bc3-bd21-d239b9f99385")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
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
					_ *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					m dogma.Event,
				) error {
					defer handlerBCalled.Set()

					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeB]:
					default:
						t.Errorf("unexpected event type routed to handler-b: %T", m)
					}

					return nil
				},
			},
		),
	)
}

// TestEventRouting_eventsFromDifferentStreamsAreRoutedToTheCorrectHandler
// verifies that events on different streams are routed to the correct handler
// based on their message type.
func TestEventRouting_eventsFromDifferentStreamsAreRoutedToTheCorrectHandler(t *testing.T) {
	var (
		handlerACalled xsync.Latch
		handlerBCalled xsync.Latch
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventA1
				},
				1, // one stream with one TypeA event
			)

			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventB1
				},
				1, // one stream with one TypeB event
			)

			xtesting.ExpectLatchesSetEventually(
				t,
				&handlerACalled,
				&handlerBCalled,
			)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler-a>", "5e9d0042-3cb2-4965-87ef-473e4e33880b")
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
					_ *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					m dogma.Event,
				) error {
					defer handlerACalled.Set()

					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
					default:
						t.Errorf("unexpected event type routed to handler-a: %T", m)
					}

					return nil
				},
			},
		),
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler-b>", "82cb75ea-2c50-4ebe-a223-d08ea5845059")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
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
					_ *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					m dogma.Event,
				) error {
					defer handlerBCalled.Set()

					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeB]:
					default:
						t.Errorf("unexpected event type routed to handler-b: %T", m)
					}

					return nil
				},
			},
		),
	)
}

// TestEventRouting_eventsAreRoutedToTheCorrectInstance verifies that
// events are routed to the correct instance based on the value returned by
// the handler's RouteEventToInstance() method.
func TestEventRouting_eventsAreRoutedToTheCorrectInstance(t *testing.T) {
	var handlerCalled xsync.Latch

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventA1
				},
				1, // one stream with one TypeA event
			)

			xtesting.ExpectLatchesSetEventually(
				t,
				&handlerCalled,
			)
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
					_ context.Context,
					m dogma.Event,
				) (string, bool, error) {
					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						return "instance:" + string(m.Content), true, nil
					default:
						panic(dogma.UnexpectedMessage)
					}
				},
				HandleEventFunc: func(
					_ context.Context,
					_ *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					m dogma.Event,
				) error {
					defer handlerCalled.Set()

					if got, want := s.InstanceID(), "instance:A1"; got != want {
						t.Errorf("unexpected instance ID: got %q, want %q", got, want)
					}

					return nil
				},
			},
		),
	)
}

// TestEventRouting_eventsAreSkippedWhenNotRoutedToAnInstance verifies that
// events are skipped when the handler's RouteEventToInstance() method returns
// false.
func TestEventRouting_eventsAreSkippedWhenNotRoutedToAnInstance(t *testing.T) {
	var handlerCalled xsync.Latch

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					if offset == 0 {
						return stubs.EventA1 // TypeA not routed to an instance
					}
					return stubs.EventB1 // TypeB routed to an instance
				},
				2, // one stream with two events
			)

			xtesting.ExpectLatchesSetEventually(
				t,
				&handlerCalled,
			)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", "ef0660b4-a68e-4383-b156-5857ac294dce")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
					)
				},
				RouteEventToInstanceFunc: func(
					_ context.Context,
					m dogma.Event,
				) (string, bool, error) {
					switch m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						return "", false, nil
					case *stubs.EventStub[stubs.TypeB]:
						return "<instance>", true, nil
					default:
						panic(dogma.UnexpectedMessage)
					}
				},
				HandleEventFunc: func(
					_ context.Context,
					_ *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					m dogma.Event,
				) error {
					defer handlerCalled.Set()

					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						t.Errorf("unexpected event type routed to handler: %T", m)
					case *stubs.EventStub[stubs.TypeB]:
						// expected
					default:
						panic(dogma.UnexpectedMessage)
					}

					return nil
				},
			},
		),
	)
}

// TestEventRouting_eventsAreNotRoutedToEndedInstances verifies that once a
// process instance has been ended via [dogma.ProcessScope].End(), subsequent
// events routed to the same instance ID are not delivered to the handler.
func TestEventRouting_eventsAreNotRoutedToEndedInstances(t *testing.T) {
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
						return stubs.EventA1 // creates and ends the instance
					case 1:
						return stubs.EventB1 // routed to the same (ended) instance
					default:
						return stubs.EventX1 // routed to a different instance to signal completion
					}
				},
				3,
			)

			xtesting.ExpectLatchesSetEventually(t, &done)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", "ef0660b4-a68e-4383-b156-5857ac294dce")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeX]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
					)
				},
				RouteEventToInstanceFunc: func(
					_ context.Context,
					m dogma.Event,
				) (string, bool, error) {
					switch m.(type) {
					case *stubs.EventStub[stubs.TypeA], *stubs.EventStub[stubs.TypeB]:
						return "<instance>", true, nil
					case *stubs.EventStub[stubs.TypeX]:
						return "<other-instance>", true, nil
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
						s.End()
					case *stubs.EventStub[stubs.TypeB]:
						t.Errorf("event was routed to an ended instance")
					case *stubs.EventStub[stubs.TypeX]:
						done.Set()
					default:
						panic(dogma.UnexpectedMessage)
					}

					return nil
				},
			},
		),
	)
}

// TestEventRouting_newHandlersDoNotSeeHistoricalEvents verifies that a handler
// deployed for the first time does not receive events that were recorded before
// it started.
func TestEventRouting_newHandlersDoNotSeeHistoricalEvents(t *testing.T) {
	const handlerKey = "ef0660b4-a68e-4383-b156-5857ac294dce"

	var (
		streamID *uuidpb.UUID
		done     xsync.Latch
	)

	xtesting.SetupThenRunEngines(
		t,
		func(t testing.TB, db *sql.DB) {
			// Populate historical events before the engines start.
			streamID = xtesting.PopulateEventStreams(
				t,
				db,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventA1
				},
				1, // one stream with one event
			)[0]
		},
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Wait for the handler to finish discovering and checkpointing the
			// historical stream, without delivering the events.
			xtesting.WaitForHandlerToConsumeAllEvents(t, engine.DB, handlerKey)

			// Append an additional event to the same stream after the handler has
			// caught up.
			xtesting.AppendToEventStream(t, engine.DB, streamID, stubs.EventA2)

			xtesting.ExpectLatchesSetEventually(t, &done)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", handlerKey)
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
					_ *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					m dogma.Event,
				) error {
					switch m := m.(*stubs.EventStub[stubs.TypeA]); m.Content {
					case "A1":
						t.Errorf("historical event was delivered to new handler")
					case "A2":
						done.Set()
					}

					return nil
				},
			},
		),
	)
}

// TestEventRouting_newRoutesDoNotCauseDeliveryOfHistoricalEvents verifies that
// adding a new event route to a handler does not cause the engine to deliver
// historical events of that type.
//
// For streams that are already "tracked" by the handler because they contain
// events of a type the handler is already configured to handle, this property
// is maintained trivially by checkpoint offsets.
//
// The interesting case is a stream that was previously ignored by the handler
// because it did not contain any relevant event types, that has become relevant
// due to the addition of the new route. In this case, the handler must begin
// consuming from the stream's current head, not from the beginning.
func TestEventRouting_newRoutesDoNotCauseDeliveryOfHistoricalEvents(t *testing.T) {
	t.Parallel()

	const handlerKey = "ef0660b4-a68e-4383-b156-5857ac294dce"

	db := xtesting.NewDatabase(t)
	var streamID *uuidpb.UUID

	// Run "version 1" of the handler with only a TypeA route. It consumes from
	// the relevant streams and establishes its checkpoint offsets.
	xtesting.RunEnginesWithDB(
		t,
		db,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Create one stream that contains an event that the handler is
			// configured to handle, and an event that it is not.
			xtesting.PopulateEventStreams(
				t,
				db,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					if offset == 0 {
						return stubs.EventA1
					}

					return stubs.EventB1
				},
				2, // one stream with both a TypeA and TypeB event
			)

			// Create a second stream that contains only an event that the
			// handler is not configured to handle at this stage.
			streamID = xtesting.PopulateEventStreams(
				t,
				db,
				func(streamID *uuidpb.UUID, _ uint64) dogma.Event {
					return stubs.EventB1
				},
				1, // one stream with only a TypeB event
			)[0]

			xtesting.WaitForHandlerToConsumeAllEvents(t, engine.DB, handlerKey)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", handlerKey)
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
					_ *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					m dogma.Event,
				) error {
					return nil
				},
			},
		),
	)

	var done xsync.Latch

	// Restart the engines with "version 2" of the handler, which has an
	// additional TypeB route.
	xtesting.RunEnginesWithDB(
		t,
		db,
		func(t testing.TB, engine *dogmaengine.Engine) {
			// Append an additional TypeB event to the same stream after the
			// handler has checkpointed.
			xtesting.AppendToEventStream(t, engine.DB, streamID, stubs.EventB2)

			// Wait for the additional event to be delivered, confirming the
			// handler is routed events that are appended after the route is
			// added.
			xtesting.ExpectLatchesSetEventually(t, &done)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", handlerKey)
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
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
					_ *stubs.ProcessRootStub,
					s dogma.ProcessEventScope[*stubs.ProcessRootStub],
					m dogma.Event,
				) error {
					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeB]:
						switch m.Content {
						case "B1":
							t.Errorf("historical TypeB event was delivered after adding route")
						case "B2":
							done.Set()
						}
					}

					return nil
				},
			},
		),
	)
}
