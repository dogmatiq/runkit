package projection_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

// TestEventStream_eventsAreDeliveredInOrder verifies that events on the same
// stream are delivered to the handler in the order they were recorded.
func TestEventStream_eventsAreDeliveredInOrder(t *testing.T) {
	const eventCount = 5

	var (
		done             xsync.Latch
		handlerMutex     sync.Mutex
		checkpointOffset uint64
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					return &stubs.EventStub[stubs.TypeA]{
						Content: stubs.TypeA(fmt.Sprintf("event-%d", offset)),
					}
				},
				eventCount,
			)

			xtesting.ExpectLatchesSetEventually(t, &done)
		},
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler>", "b2c3d4e5-6f7a-4b8c-9d0e-1f2a3b4c5d6e")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleEventFunc: func(
					_ context.Context,
					s dogma.ProjectionEventScope,
					m dogma.Event,
				) (uint64, error) {
					handlerMutex.Lock()
					defer handlerMutex.Unlock()

					gotOffset := s.Offset()
					wantOffset := checkpointOffset

					if gotOffset != wantOffset {
						t.Errorf("unexpected event offset: got %d, want %d", gotOffset, wantOffset)
					}

					gotContent := m.(*stubs.EventStub[stubs.TypeA]).Content
					wantContent := stubs.TypeA(fmt.Sprintf("event-%d", gotOffset))

					if gotContent != wantContent {
						t.Errorf("unexpected event content at offset %d: got %q, want %q", gotOffset, gotContent, wantContent)
					}

					checkpointOffset++
					if checkpointOffset == eventCount {
						done.Set()
					}

					return checkpointOffset, nil
				},
			},
		),
	)
}

// TestEventStream_eventsAreRedeliveredInOrderWhenHandlerReturnsAnError verifies
// that when a handler returns an error, the event is redelivered, and
// subsequent events are still delivered in order.
func TestEventStream_eventsAreRedeliveredInOrderWhenHandlerReturnsAnError(t *testing.T) {
	const eventCount = 5

	var (
		done         xsync.Latch
		handlerMutex sync.Mutex
		seen         = map[uint64]bool{}
		handled      uint64
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					return &stubs.EventStub[stubs.TypeA]{
						Content: stubs.TypeA(fmt.Sprintf("event-%d", offset)),
					}
				},
				eventCount,
			)

			xtesting.ExpectLatchesSetEventually(t, &done)
		},
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler>", "d4e5f6a7-8b9c-4d0e-9f2a-3b4c5d6e7f8a")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleEventFunc: func(
					_ context.Context,
					s dogma.ProjectionEventScope,
					m dogma.Event,
				) (uint64, error) {
					handlerMutex.Lock()
					defer handlerMutex.Unlock()

					offset := s.Offset()

					if !seen[offset] {
						seen[offset] = true
						return 0, errors.New("<error>")
					}

					gotContent := m.(*stubs.EventStub[stubs.TypeA]).Content
					wantContent := stubs.TypeA(fmt.Sprintf("event-%d", offset))

					if gotContent != wantContent {
						t.Errorf("unexpected event content at offset %d: got %q, want %q", offset, gotContent, wantContent)
					}

					handled++
					if handled == eventCount {
						done.Set()
					}

					return s.Offset() + 1, nil
				},
			},
		),
	)
}
