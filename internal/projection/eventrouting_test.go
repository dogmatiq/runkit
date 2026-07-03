package projection_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
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
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler-a>", "6f6c3151-b980-430c-bb7a-891f12104035")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleEventFunc: func(
					_ context.Context,
					s dogma.ProjectionEventScope,
					m dogma.Event,
				) (uint64, error) {
					defer handlerACalled.Set()

					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						return s.Offset() + 1, nil
					default:
						t.Errorf("unexpected event type routed to handler-a: %T", m)
						return 0, nil
					}
				},
			},
		),
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler-b>", "93090487-ddbb-417a-9166-9663c303e8c3")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
					)
				},
				HandleEventFunc: func(
					_ context.Context,
					s dogma.ProjectionEventScope,
					m dogma.Event,
				) (uint64, error) {
					defer handlerBCalled.Set()

					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeB]:
						return s.Offset() + 1, nil
					default:
						t.Errorf("unexpected event type routed to handler-b: %T", m)
						return 0, nil
					}
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
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler-a>", "d7e8f9a0-b1c2-3d4e-5f6a-7b8c9d0e1f2a")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleEventFunc: func(
					_ context.Context,
					s dogma.ProjectionEventScope,
					m dogma.Event,
				) (uint64, error) {
					defer handlerACalled.Set()

					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						return s.Offset() + 1, nil
					default:
						t.Errorf("unexpected event type routed to handler-a: %T", m)
						return 0, nil
					}
				},
			},
		),
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler-b>", "c3d4e5f6-a7b8-9c0d-1e2f-3a4b5c6d7e8f")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
					)
				},
				HandleEventFunc: func(
					_ context.Context,
					s dogma.ProjectionEventScope,
					m dogma.Event,
				) (uint64, error) {
					defer handlerBCalled.Set()

					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeB]:
						return s.Offset() + 1, nil
					default:
						t.Errorf("unexpected event type routed to handler-b: %T", m)
						return 0, nil
					}
				},
			},
		),
	)
}

// TestEventRouting_newHandlersSeeHistoricalEvents verifies that a handler
// deployed for the first time receives events that were recorded before it
// existed.
func TestEventRouting_newHandlersSeeHistoricalEvents(t *testing.T) {
	const handlerKey = "a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d"

	var done xsync.Latch

	xtesting.SetupThenRunEngines(
		t,
		func(t testing.TB, db *sql.DB) {
			xtesting.PopulateEventStreams(
				t,
				db,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventA1
				},
				1, // one stream with one historical event
			)
		},
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExpectLatchesSetEventually(t, &done)
		},
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler>", handlerKey)
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleEventFunc: func(
					_ context.Context,
					s dogma.ProjectionEventScope,
					m dogma.Event,
				) (uint64, error) {
					done.Set()
					return s.Offset() + 1, nil
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

	const handlerKey = "6d6fff71-9441-4b9e-aea9-86867f009283"

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
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler>", handlerKey)
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleEventFunc: func(
					_ context.Context,
					s dogma.ProjectionEventScope,
					m dogma.Event,
				) (uint64, error) {
					return s.Offset() + 1, nil
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
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler>", handlerKey)
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
					)
				},
				HandleEventFunc: func(
					_ context.Context,
					s dogma.ProjectionEventScope,
					m dogma.Event,
				) (uint64, error) {
					switch m := m.(type) {
					case *stubs.EventStub[stubs.TypeB]:
						switch m.Content {
						case "B1":
							t.Errorf("historical TypeB event was delivered after adding route")
						case "B2":
							done.Set()
						}
					}

					return s.Offset() + 1, nil
				},
			},
		),
	)
}
