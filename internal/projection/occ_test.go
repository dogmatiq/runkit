package projection_test

import (
	"context"
	"sync"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

// TestOCC_conflictWithHigherCheckpointOffsetSkipsEvents verifies that when the
// handler returns a checkpoint offset higher than expected, intermediate events
// are skipped.
func TestOCC_conflictWithHigherCheckpointOffsetSkipsEvents(t *testing.T) {
	const handlerKey = "a2b3c4d5-6e7f-4a8b-9c0d-1e2f3a4b5c6d"

	var (
		checkpointMutex  sync.Mutex
		checkpointOffset uint64
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			const eventCount = 3

			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventA1
				},
				eventCount,
			)

			xtesting.WaitForHandlerToConsumeAllEvents(t, engine.DB, handlerKey)

			checkpointMutex.Lock()
			defer checkpointMutex.Unlock()

			got := checkpointOffset
			want := uint64(eventCount)

			if got != want {
				t.Errorf("unexpected final checkpoint offset: got %d, want %d", got, want)
			}
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
					checkpointMutex.Lock()
					defer checkpointMutex.Unlock()

					if s.Offset() != checkpointOffset {
						t.Errorf("unexpected offset: got %d, want %d", s.Offset(), checkpointOffset)
					}

					if s.Offset() == 0 {
						checkpointOffset = 2 // skip the event at offset 1
					} else {
						checkpointOffset++
					}

					return checkpointOffset, nil
				},
				CheckpointOffsetFunc: func(context.Context, string) (uint64, error) {
					checkpointMutex.Lock()
					defer checkpointMutex.Unlock()

					return checkpointOffset, nil
				},
			},
		),
	)
}

// TestOCC_conflictWithLowerCheckpointOffsetRedeliversEvents verifies that when
// the handler returns a checkpoint offset lower than expected, previously
// delivered events are redelivered.
func TestOCC_conflictWithLowerCheckpointOffsetRedeliversEvents(t *testing.T) {
	const handlerKey = "b3c4d5e6-7f8a-4b9c-8d1e-2f3a4b5c6d7e"

	var (
		checkpointMutex  sync.Mutex
		checkpointOffset uint64
		conflicted       bool
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			const eventCount = 2

			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventA1
				},
				eventCount,
			)

			xtesting.WaitForHandlerToConsumeAllEvents(t, engine.DB, handlerKey)

			checkpointMutex.Lock()
			defer checkpointMutex.Unlock()

			got := checkpointOffset
			want := uint64(eventCount)

			if got != want {
				t.Errorf("unexpected final checkpoint offset: got %d, want %d", got, want)
			}
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
					checkpointMutex.Lock()
					defer checkpointMutex.Unlock()

					if s.Offset() != checkpointOffset {
						t.Errorf("unexpected offset: got %d, want %d", s.Offset(), checkpointOffset)
					}

					if s.Offset() == 1 && !conflicted {
						conflicted = true
						checkpointOffset = 0 // rewind to offset 0
					} else {
						checkpointOffset++
					}

					return checkpointOffset, nil
				},
				CheckpointOffsetFunc: func(context.Context, string) (uint64, error) {
					checkpointMutex.Lock()
					defer checkpointMutex.Unlock()

					return checkpointOffset, nil
				},
			},
		),
	)
}
