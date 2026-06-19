package projection_test

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
