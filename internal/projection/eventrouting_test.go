package projection_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit"
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
		func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
			xtesting.PopulateEventStreams(
				t,
				db,
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
		func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
			xtesting.PopulateEventStreams(
				t,
				db,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventA1
				},
				1, // one stream with one TypeA event
			)

			xtesting.PopulateEventStreams(
				t,
				db,
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
		func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
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

// TestEventRouting_newRoutesCauseDeliveryOfHistoricalEventsAfterTheCheckpoint
// verifies that adding a new event route to a handler causes the engine to
// deliver historical events of that type that are after the handler's
// checkpoint offset for each stream.
//
// The handler's checkpoint offset advances only when it handles an event, so
// it is not moved past events of types that the handler did not consume at the
// time. Dogma allows some, all or none of the historical events of a newly
// routed type to be delivered.
func TestEventRouting_newRoutesCauseDeliveryOfHistoricalEventsAfterTheCheckpoint(t *testing.T) {
	t.Parallel()

	const handlerKey = "6d6fff71-9441-4b9e-aea9-86867f009283"

	var (
		db          = xtesting.NewDatabase(t)
		checkpoints checkpointStore
		mixed       *uuidpb.UUID
		onlyB       *uuidpb.UUID
		handledA    xsync.Latch
	)

	// Run "version 1" of the handler with only a TypeA route. It handles the
	// TypeA event, establishing its checkpoint offset on that stream.
	xtesting.RunEnginesWithDB(
		t,
		db,
		func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
			// Create one stream that contains an event that the handler is
			// configured to handle, followed by an event that it is not.
			mixed = xtesting.PopulateEventStreams(
				t,
				db,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					if offset == 0 {
						return stubs.EventA1
					}

					return stubs.EventB1
				},
				2,
			)[0]

			// Create a second stream that contains only an event that the
			// handler is not configured to handle at this stage.
			onlyB = xtesting.PopulateEventStreams(
				t,
				db,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventB2
				},
				1,
			)[0]

			xtesting.ExpectLatchesSetEventually(t, &handledA)
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
					_ dogma.Event,
				) (uint64, error) {
					return checkpoints.HandleEvent(
						s,
						func() error {
							handledA.Set()
							return nil
						},
					)
				},
				CheckpointOffsetFunc: checkpoints.CheckpointOffset,
			},
		),
	)

	var handledB1, handledB2 xsync.Latch

	// Restart the engines with "version 2" of the handler, which has an
	// additional TypeB route.
	xtesting.RunEnginesWithDB(
		t,
		db,
		func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
			xtesting.ExpectLatchesSetEventually(t, &handledB1, &handledB2)
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
					return checkpoints.HandleEvent(
						s,
						func() error {
							e, ok := m.(*stubs.EventStub[stubs.TypeB])
							if !ok {
								t.Errorf("unexpected event delivered after adding route: %T", m)
								return nil
							}

							switch e.Content {
							case "B1":
								if s.StreamID() != mixed.AsString() || s.Offset() != 1 {
									t.Errorf("unexpected position of B1: stream %s, offset %d", s.StreamID(), s.Offset())
								}
								handledB1.Set()
							case "B2":
								if s.StreamID() != onlyB.AsString() || s.Offset() != 0 {
									t.Errorf("unexpected position of B2: stream %s, offset %d", s.StreamID(), s.Offset())
								}
								handledB2.Set()
							}

							return nil
						},
					)
				},
				CheckpointOffsetFunc: checkpoints.CheckpointOffset,
			},
		),
	)
}
