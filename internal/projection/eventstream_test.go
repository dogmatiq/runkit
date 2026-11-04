package projection_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
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

					if s.CheckpointOffset() != checkpointOffset {
						return checkpointOffset, nil
					}

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
				CheckpointOffsetFunc: func(context.Context, string) (uint64, error) {
					handlerMutex.Lock()
					defer handlerMutex.Unlock()

					return checkpointOffset, nil
				},
			},
		),
	)
}

// TestEventStream_eventsAreRedeliveredInOrderWhenHandlerReturnsAnError verifies
// that when a handler returns an error, the event is redelivered from the
// handler's own checkpoint offset, rather than from the start of the stream,
// and each event is handled exactly once, in order.
func TestEventStream_eventsAreRedeliveredInOrderWhenHandlerReturnsAnError(t *testing.T) {
	const (
		eventCount   = 5
		failedOffset = 2
	)

	var (
		done        xsync.Latch
		checkpoints checkpointStore
		failed      bool
		handled     []uint64
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
					return checkpoints.HandleEvent(
						s,
						func() error {
							offset := s.Offset()

							if offset == failedOffset && !failed {
								failed = true
								return errors.New("<error>")
							}

							gotContent := m.(*stubs.EventStub[stubs.TypeA]).Content
							wantContent := stubs.TypeA(fmt.Sprintf("event-%d", offset))

							if gotContent != wantContent {
								t.Errorf("unexpected event content at offset %d: got %q, want %q", offset, gotContent, wantContent)
							}

							if want := uint64(len(handled)); offset != want {
								t.Errorf("unexpected event offset: got %d, want %d", offset, want)
							}

							handled = append(handled, offset)
							if len(handled) == eventCount {
								done.Set()
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

// TestEventStream_checkpointOffsetIsUnchangedBySkippedEvents verifies that the
// checkpoint offset passed to the handler is the offset the handler itself last
// returned, even when events of types it does not consume have been skipped.
func TestEventStream_checkpointOffsetIsUnchangedBySkippedEvents(t *testing.T) {
	type delivery struct {
		Offset     uint64
		Checkpoint uint64
	}

	var (
		done        xsync.Latch
		checkpoints checkpointStore
		m           sync.Mutex
		deliveries  []delivery
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			xtesting.PopulateEventStreams(
				t,
				db,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					switch offset {
					case 0:
						return stubs.EventA1
					case 3:
						return stubs.EventA2
					default:
						return stubs.EventB1
					}
				},
				4,
			)

			xtesting.WaitForLatches(t, &done)

			m.Lock()
			defer m.Unlock()

			xtesting.Expect(
				t,
				"unexpected deliveries",
				deliveries,
				[]delivery{
					{
						Offset:     0,
						Checkpoint: 0,
					},
					{
						Offset:     3,
						Checkpoint: 1,
					},
				},
			)
		},
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler>", "0b1c7e3f-5a2d-4e8b-9c6f-1d2e3f4a5b6c")
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
							m.Lock()
							defer m.Unlock()

							deliveries = append(
								deliveries,
								delivery{
									Offset:     s.Offset(),
									Checkpoint: s.CheckpointOffset(),
								},
							)

							if len(deliveries) == 2 {
								done.Set()
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

// TestEventStream_consumptionResumesFromTheHandlersCheckpointOffset verifies
// that the engine starts delivering events from the offset returned by the
// handler's CheckpointOffset() method.
func TestEventStream_consumptionResumesFromTheHandlersCheckpointOffset(t *testing.T) {
	const eventCount = 4

	var (
		done             xsync.Latch
		m                sync.Mutex
		checkpointOffset uint64 = 2
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			xtesting.PopulateEventStreams(
				t,
				db,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventA1
				},
				eventCount,
			)

			xtesting.WaitForLatches(t, &done)
		},
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler>", "7c8d9e0f-1a2b-4c3d-8e4f-5a6b7c8d9e0f")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleEventFunc: func(
					_ context.Context,
					s dogma.ProjectionEventScope,
					_ dogma.Event,
				) (uint64, error) {
					m.Lock()
					defer m.Unlock()

					if s.CheckpointOffset() != checkpointOffset {
						return checkpointOffset, nil
					}

					if s.Offset() < 2 {
						t.Errorf("event before the handler's checkpoint offset was delivered: offset %d", s.Offset())
					}

					checkpointOffset = s.Offset() + 1
					if checkpointOffset == eventCount {
						done.Set()
					}

					return checkpointOffset, nil
				},
				CheckpointOffsetFunc: func(context.Context, string) (uint64, error) {
					m.Lock()
					defer m.Unlock()

					return checkpointOffset, nil
				},
			},
		),
	)
}
