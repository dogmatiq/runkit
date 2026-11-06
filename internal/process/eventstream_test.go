package process_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

// TestEventStream_eventsAreDeliveredInOrder verifies that events on the same
// stream are delivered to the handler in the order they were recorded.
func TestEventStream_eventsAreDeliveredInOrder(t *testing.T) {
	const eventCount = 5

	var (
		done             cosyne.Latch
		handlerMutex     sync.Mutex
		checkpointOffset uint64
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			xtesting.PopulateEventStreams(
				t,
				db,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					return &stubs.EventStub[stubs.TypeA]{
						Content: stubs.TypeA(fmt.Sprintf("event-%d", offset)),
					}
				},
				eventCount,
			)

			xtesting.WaitForLatches(t, &done)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", "2560f8b5-cb89-425c-855c-43ec2eb2ebe1")
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
					handlerMutex.Lock()
					defer handlerMutex.Unlock()

					// Setting the latch stops the engines, which can roll back
					// the final delivery after the handler has returned, so
					// another engine may deliver the final event again.
					if checkpointOffset == eventCount {
						return nil
					}

					gotContent := m.(*stubs.EventStub[stubs.TypeA]).Content
					wantContent := stubs.TypeA(fmt.Sprintf("event-%d", checkpointOffset))

					if gotContent != wantContent {
						t.Errorf("unexpected event content at offset %d: got %q, want %q", checkpointOffset, gotContent, wantContent)
					}

					checkpointOffset++
					if checkpointOffset == eventCount {
						done.Set()
					}

					return nil
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
		done             cosyne.Latch
		handlerMutex     sync.Mutex
		seen             = map[stubs.TypeA]struct{}{}
		checkpointOffset uint64
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			xtesting.PopulateEventStreams(
				t,
				db,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					return &stubs.EventStub[stubs.TypeA]{
						Content: stubs.TypeA(fmt.Sprintf("event-%d", offset)),
					}
				},
				eventCount,
			)

			xtesting.WaitForLatches(t, &done)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", "d4e5f6a7-8b9c-4d0e-9f2a-3b4c5d6e7f8a")
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
					handlerMutex.Lock()
					defer handlerMutex.Unlock()

					// Setting the latch stops the engines, which can roll back
					// the final delivery after the handler has returned, so
					// another engine may deliver the final event again.
					if checkpointOffset == eventCount {
						return nil
					}

					gotContent := m.(*stubs.EventStub[stubs.TypeA]).Content

					if _, ok := seen[gotContent]; !ok {
						seen[gotContent] = struct{}{}
						return errors.New("<error>")
					}

					wantContent := fmt.Sprintf("event-%d", checkpointOffset)

					if string(gotContent) != wantContent {
						t.Errorf("unexpected event content: got %q, want %q", gotContent, wantContent)
					}

					checkpointOffset++
					if checkpointOffset == eventCount {
						done.Set()
					}

					return nil
				},
			},
		),
	)
}
