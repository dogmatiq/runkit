package process_test

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
		done             xsync.Latch
		handlerMutex     sync.Mutex
		seen             = map[stubs.TypeA]struct{}{}
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

// // TestEventStream_handlerFailuresCauseStreamConsumptionToBePostponed verifies
// // that if handling an event fails (either via error return or panic),
// // consumption of the stream is postponed.
// func TestEventStream_handlerFailuresCauseStreamConsumptionToBePostponed(t *testing.T) {
// 	const handlerKey = "87f5a992-a3a6-494a-be1c-c01c6fff8ff0"

// 	cases := []struct {
// 		Name            string
// 		HandleEventFunc func(
// 			context.Context,
// 			dogma.ProjectionEventScope,
// 			dogma.Event,
// 		) (uint64, error)
// 	}{
// 		{
// 			"returns error",
// 			func(
// 				context.Context,
// 				dogma.ProjectionEventScope,
// 				dogma.Event,
// 			) (uint64, error) {
// 				return 0, fmt.Errorf("<handler error>")
// 			},
// 		},
// 		{
// 			"panics",
// 			func(
// 				context.Context,
// 				dogma.ProjectionEventScope,
// 				dogma.Event,
// 			) (uint64, error) {
// 				panic("<handler panic>")
// 			},
// 		},
// 	}

// 	for _, c := range cases {
// 		t.Run(c.Name, func(t *testing.T) {
// 			xtesting.RunEngines(
// 				t,
// 				func(t testing.TB, engine *dogmaengine.Engine) {
// 					streamIDs := xtesting.PopulateEventStreams(
// 						t,
// 						engine.DB,
// 						func(*uuidpb.UUID, uint64) dogma.Event {
// 							return stubs.EventA1
// 						},
// 						1,
// 					)

// 					xtesting.WaitForHandlerToPostponeConsumingStream(
// 						t,
// 						engine.DB,
// 						handlerKey,
// 						streamIDs[0],
// 					)
// 				},
// 				dogma.ViaProjection(
// 					&stubs.ProjectionMessageHandlerStub{
// 						ConfigureFunc: func(c dogma.ProjectionConfigurer) {
// 							c.Identity("<handler>", handlerKey)
// 							c.Routes(
// 								dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
// 							)
// 						},
// 						HandleEventFunc: c.HandleEventFunc,
// 					},
// 				),
// 			)
// 		})
// 	}
// }

// // TestEventStream_failureCounterIsResetOnSuccess verifies that after a handler
// // fails and then subsequently succeeds, the failures counter is reset to zero.
// func TestEventStream_failureCounterIsResetOnSuccess(t *testing.T) {
// 	const handlerKey = "c1d2e3f4-5a6b-4c7d-8e9f-0a1b2c3d4e5f"

// 	var (
// 		done      xsync.Latch
// 		hasFailed atomic.Bool
// 	)

// 	xtesting.RunEngines(
// 		t,
// 		func(t testing.TB, engine *dogmaengine.Engine) {
// 			xtesting.PopulateEventStreams(
// 				t,
// 				engine.DB,
// 				func(*uuidpb.UUID, uint64) dogma.Event {
// 					return stubs.EventA1
// 				},
// 				1,
// 			)

// 			xtesting.WaitForQueryResult(
// 				t,
// 				fmt.Sprintf("handler %q has failures reset to zero", handlerKey),
// 				0,
// 				engine.DB,
// 				`SELECT failures
// 				FROM eventstream.handler_checkpoints
// 				WHERE handler_key = $1
// 				AND checkpoint_offset IS NOT NULL`,
// 				handlerKey,
// 			)

// 			done.Set()
// 			xtesting.ExpectLatchesSetEventually(t, &done)
// 		},
// 		dogma.ViaProjection(
// 			&stubs.ProjectionMessageHandlerStub{
// 				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
// 					c.Identity("<handler>", handlerKey)
// 					c.Routes(
// 						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
// 					)
// 				},
// 				HandleEventFunc: func(
// 					_ context.Context,
// 					s dogma.ProjectionEventScope,
// 					_ dogma.Event,
// 				) (uint64, error) {
// 					if hasFailed.CompareAndSwap(false, true) {
// 						return 0, errors.New("<error>")
// 					}

// 					return s.Offset() + 1, nil
// 				},
// 			},
// 		),
// 	)
// }

// // TestEventStream_postponedStreamsAreNotConsumed verifies that a stream with
// // resume_at in the future is not acquired for reading.
// func TestEventStream_postponedStreamsAreNotConsumed(t *testing.T) {
// 	const handlerKey = "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"

// 	xtesting.RunEngines(
// 		t,
// 		func(t testing.TB, engine *dogmaengine.Engine) {
// 			var streamIDs []*uuidpb.UUID

// 			xtesting.Transact(t, engine.DB, func(tx *sql.Tx) {
// 				streamIDs = xtesting.PopulateEventStreams(
// 					t,
// 					tx,
// 					func(*uuidpb.UUID, uint64) dogma.Event {
// 						return stubs.EventA1
// 					},
// 					1,
// 				)

// 				xtesting.PostponeStreamConsumption(
// 					t,
// 					tx,
// 					handlerKey,
// 					streamIDs[0],
// 				)
// 			})

// 			// Allow several poll cycles to pass.
// 			time.Sleep(50 * time.Millisecond)

// 			xtesting.WaitForQueryResult(
// 				t,
// 				fmt.Sprintf("handler %q still has resume_at in the future", handlerKey),
// 				1,
// 				engine.DB,
// 				`SELECT COUNT(*)
// 				FROM eventstream.handler_checkpoints
// 				WHERE handler_key = $1
// 				AND stream_id = $2
// 				AND resume_at > clock_timestamp()`,
// 				handlerKey,
// 				xsql.UUID(streamIDs[0]),
// 			)
// 		},
// 		dogma.ViaProjection(
// 			&stubs.ProjectionMessageHandlerStub{
// 				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
// 					c.Identity("<handler>", handlerKey)
// 					c.Routes(
// 						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
// 					)
// 				},
// 				HandleEventFunc: func(
// 					context.Context,
// 					dogma.ProjectionEventScope,
// 					dogma.Event,
// 				) (uint64, error) {
// 					t.Error("handler was called for a postponed stream")
// 					return 0, nil
// 				},
// 			},
// 		),
// 	)
// }
