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
	t.Skip("not implemented")
}
