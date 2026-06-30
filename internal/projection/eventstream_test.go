package projection_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/reference-engine"
	. "github.com/dogmatiq/reference-engine/internal/projection"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

// TestEventStream_eventsAreDeliveredInOrder verifies that events on the same
// stream are delivered to the handler in the order they were recorded.
func TestEventStream_eventsAreDeliveredInOrder(t *testing.T) {
	cases := []struct {
		Name       string
		EventCount uint64
	}{
		{
			"smaller than batch size",
			EventBatchSize - 1,
		},
		{
			"greater than batch size",
			EventBatchSize + 1,
		},
		{
			"multiple batches",
			(EventBatchSize * 2) + 1,
		},
		{
			"exactly batch size",
			EventBatchSize,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
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
						c.EventCount,
					)

					xtesting.ExpectLatchesSetEventually(t, &done)
				},
				dogma.ViaProjection(
					&stubs.ProjectionMessageHandlerStub{
						ConfigureFunc: func(c dogma.ProjectionConfigurer) {
							c.Identity("<handler>", "98e312ca-2d4c-4b59-bf47-c80428904919")
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
							if checkpointOffset == c.EventCount {
								done.Set()
							}

							return checkpointOffset, nil
						},
					},
				),
			)
		})
	}
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
		handled      int
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
					c.Identity("<handler>", "37807f8b-d00b-4929-831b-8a156220334f")
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

// TestEventStream_handlerFailuresCauseStreamConsumptionToBePostponed verifies
// that if handling an event fails (either via error return or panic),
// consumption of the stream is postponed.
func TestEventStream_handlerFailuresCauseStreamConsumptionToBePostponed(t *testing.T) {
	cases := []struct {
		Name            string
		HandleEventFunc func(
			context.Context,
			dogma.ProjectionEventScope,
			dogma.Event,
		) (uint64, error)
	}{
		{
			"returns error",
			func(
				context.Context,
				dogma.ProjectionEventScope,
				dogma.Event,
			) (uint64, error) {
				return 0, fmt.Errorf("<handler error>")
			},
		},
		{
			"panics",
			func(
				context.Context,
				dogma.ProjectionEventScope,
				dogma.Event,
			) (uint64, error) {
				panic("<handler panic>")
			},
		},
	}

	for _, c := range cases {
		const handlerKey = "626fb93e-35a2-4aa8-a5b4-b5ebbaedc5ff"

		t.Run(c.Name, func(t *testing.T) {
			xtesting.RunEngines(
				t,
				func(t testing.TB, engine *dogmaengine.Engine) {
					streamIDs := xtesting.PopulateEventStreams(
						t,
						engine.DB,
						func(*uuidpb.UUID, uint64) dogma.Event {
							return stubs.EventA1
						},
						1,
					)

					xtesting.WaitForHandlerToPostponeConsumingStream(
						t,
						engine.DB,
						handlerKey,
						streamIDs[0],
					)
				},
				dogma.ViaProjection(
					&stubs.ProjectionMessageHandlerStub{
						ConfigureFunc: func(c dogma.ProjectionConfigurer) {
							c.Identity("<handler>", handlerKey)
							c.Routes(
								dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
							)
						},
						HandleEventFunc: c.HandleEventFunc,
					},
				),
			)
		})
	}
}

// TestEventStream_failureCounterIsResetOnSuccess verifies that after a handler
// fails and then subsequently succeeds, the failures counter is reset to zero.
func TestEventStream_failureCounterIsResetOnSuccess(t *testing.T) {
	const handlerKey = "128649ef-b2d5-4247-a00c-c3d615abd8a5"

	var (
		done      xsync.Latch
		hasFailed atomic.Bool
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			streamIDs := xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventA1
				},
				1,
			)

			xtesting.WaitForStreamFailureCounterToReset(
				t,
				engine.DB,
				handlerKey,
				streamIDs...,
			)

			done.Set()
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
					_ dogma.Event,
				) (uint64, error) {
					if hasFailed.CompareAndSwap(false, true) {
						return 0, errors.New("<error>")
					}

					return s.Offset() + 1, nil
				},
			},
		),
	)
}

// TestEventStream_postponedStreamsAreNotConsumed verifies that a stream with
// resume_at in the future is not acquired for reading.
func TestEventStream_postponedStreamsAreNotConsumed(t *testing.T) {
	const handlerKey = "42e8b109-35f0-45d9-9ec4-959e34cf113e"

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			var streamIDs []*uuidpb.UUID

			xtesting.Transact(t, engine.DB, func(tx *sql.Tx) {
				streamIDs = xtesting.PopulateEventStreams(
					t,
					tx,
					func(*uuidpb.UUID, uint64) dogma.Event {
						return stubs.EventA1
					},
					1,
				)

				xtesting.PostponeStreamConsumption(
					t,
					tx,
					handlerKey,
					streamIDs[0],
				)
			})

			// Allow several poll cycles to pass.
			time.Sleep(50 * time.Millisecond)

			xtesting.WaitForQueryResult(
				t,
				fmt.Sprintf("handler %q still has resume_at in the future", handlerKey),
				1,
				engine.DB,
				`SELECT COUNT(*)
				FROM eventstream.handler_checkpoints
				WHERE handler_key = $1
				AND stream_id = $2
				AND resume_at > clock_timestamp()`,
				handlerKey,
				xsql.UUID(streamIDs[0]),
			)
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
					context.Context,
					dogma.ProjectionEventScope,
					dogma.Event,
				) (uint64, error) {
					t.Error("handler was called for a postponed stream")
					return 0, nil
				},
			},
		),
	)
}
