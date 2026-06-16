package projection_test

import (
	"context"
	"sync"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestEventsAreDeliveredStartingAtTheCheckpointOffset(t *testing.T) {
	const (
		handlerKey = "cf5fe7ce-4311-455f-be14-03a3b647cc7a"
		eventCount = 5
	)

	var (
		checkpointMutex  sync.Mutex
		checkpointOffset = uint64(2) // ignore the first two events on the stream
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{},
			)

			xtesting.ExpectEmptyCommandQueueEventually(t, engine.DB)
			xtesting.ExpectNoUnconsumedEventsEventually(t, engine.DB, handlerKey)

			checkpointMutex.Lock()
			defer checkpointMutex.Unlock()

			if checkpointOffset != uint64(eventCount) {
				t.Errorf("unexpected final checkpoint offset: got %d, want %d", checkpointOffset, eventCount)
			}
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<event-producer>", "e5f6a7b8-9c0d-4e1f-8a2b-3c4d5e6f7a8b")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleCommandFunc: func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					_ dogma.Command,
				) error {
					for range eventCount {
						s.RecordEvent(&stubs.EventStub[stubs.TypeA]{})
					}
					return nil
				},
			},
		),
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
					checkpointMutex.Lock()
					defer checkpointMutex.Unlock()

					if s.Offset() != checkpointOffset {
						t.Errorf("unexpected offset: got %d, want %d", s.Offset(), checkpointOffset)
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

func TestOCCConflictWithHigherCheckpointOffset(t *testing.T) {
	const (
		handlerKey = "a2b3c4d5-6e7f-4a8b-9c0d-1e2f3a4b5c6d"
		eventCount = 3
	)

	var (
		checkpointMutex  sync.Mutex
		checkpointOffset uint64
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{},
			)

			xtesting.ExpectEmptyCommandQueueEventually(t, engine.DB)
			xtesting.ExpectNoUnconsumedEventsEventually(t, engine.DB, handlerKey)

			checkpointMutex.Lock()
			defer checkpointMutex.Unlock()

			got := checkpointOffset
			want := uint64(eventCount)

			if got != want {
				t.Errorf("unexpected final checkpoint offset: got %d, want %d", got, want)
			}
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<event-producer>", "e5f6a7b8-9c0d-4e1f-8a2b-3c4d5e6f7a8b")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleCommandFunc: func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					_ dogma.Command,
				) error {
					for range eventCount {
						s.RecordEvent(&stubs.EventStub[stubs.TypeA]{})
					}
					return nil
				},
			},
		),
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

func TestOCCConflictWithLowerCheckpointOffset(t *testing.T) {
	const (
		handlerKey = "b3c4d5e6-7f8a-4b9c-8d1e-2f3a4b5c6d7e"
		eventCount = 2
	)

	var (
		checkpointMutex  sync.Mutex
		checkpointOffset uint64
		conflicted       bool
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExecuteCommand(
				t,
				engine,
				&stubs.CommandStub[stubs.TypeA]{},
			)

			xtesting.ExpectEmptyCommandQueueEventually(t, engine.DB)
			xtesting.ExpectNoUnconsumedEventsEventually(t, engine.DB, handlerKey)

			checkpointMutex.Lock()
			defer checkpointMutex.Unlock()

			got := checkpointOffset
			want := uint64(eventCount)

			if got != want {
				t.Errorf("unexpected final checkpoint offset: got %d, want %d", got, want)
			}
		},
		dogma.ViaIntegration(
			&stubs.IntegrationMessageHandlerStub{
				ConfigureFunc: func(c dogma.IntegrationConfigurer) {
					c.Identity("<event-producer>", "c4d5e6f7-8a9b-4c0d-9e2f-3a4b5c6d7e8f")
					c.Routes(
						dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
						dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				HandleCommandFunc: func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					_ dogma.Command,
				) error {
					for range eventCount {
						s.RecordEvent(&stubs.EventStub[stubs.TypeA]{})
					}
					return nil
				},
			},
		),
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
