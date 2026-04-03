package poisonqueue_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/enginekit/x/xrapid"
	"github.com/dogmatiq/persistencekit/kv"
	. "github.com/dogmatiq/runkit/internal/subsystem/poisonqueue"
	"github.com/dogmatiq/runkit/internal/subsystem/poisonqueue/internal/teststate"
	"github.com/dogmatiq/runkit/internal/x/xtesting/kvtest"
	"pgregory.net/rapid"
)

func TestService(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		requests := make(chan EnqueueRequest)

		// Create a context under which we run the services.
		//
		// Note that it's NOT based on [testing.T.Context], so that we can stop
		// the services' gracefully when the test ends.
		ctx, cancel := context.WithCancelCause(context.Background())

		queue := &teststate.PoisonQueue{
			Context:         ctx,
			EnqueueRequests: requests,
		}

		// stop is used to signal all services to stop gracefully.
		stop := make(chan struct{})

		// Run multiple services in the background for the duration of the
		// tests.
		//
		// Each service represents a separate running instance of the subsystem,
		// as would normally be run on separate machines/containers in a
		// production system.
		var services sync.WaitGroup

		for idx := range 3 {
			services.Go(func() {
				keyspaces := kv.BinaryStore(&queue.Keyspaces)
				telem := telemetry.
					NewTestProvider(t).
					WithAttrs(telemetry.Int("service.id", idx))

				if testing.Verbose() {
					keyspaces = kv.WithTelemetry(
						keyspaces,
						telem.TracerProvider,
						telem.MeterProvider,
						telem.LoggerProvider,
					)
				}

				svc := &Service{
					Keyspaces:       keyspaces,
					Stop:            stop,
					EnqueueRequests: requests,
					Telemetry:       telem,
				}

				if err := svc.Run(ctx); err != nil {
					t.Errorf("service %d failed: %s", idx, err)
					cancel(fmt.Errorf("service %d failed: %w", idx, err))
				}
			})
		}

		// When the test ends signal all services to stop gracefully.
		t.Cleanup(func() {
			close(stop)
			services.Wait()
			cancel(errors.New("test completed"))
		})

		t.Repeat(
			rapid.StateMachineActions(&state{queue}),
		)
	})
}

type state struct {
	queue *teststate.PoisonQueue
}

func (s *state) Check(t *rapid.T) {}

func (s *state) EnqueueCommand(t *rapid.T) {
	env := xrapid.Envelope().Draw(t, "command")

	s.queue.SendEnqueueRequest(
		t,
		EnqueueRequest{
			CommandEnvelope: env,
			FailedHandler:   xrapid.Identity().Draw(t, "failed handler"),
		},
		EnqueueResponse{
			CommandMessageID: env.MessageId,
			Ok:               true,
		},
	)
}

func (s *state) RenqueueExistingCommand(t *rapid.T) {
	env := s.queue.MessagesGen(t).Draw(t, "existing message")

	s.queue.SendEnqueueRequest(
		t,
		EnqueueRequest{
			CommandEnvelope: env,
			FailedHandler:   xrapid.Identity().Draw(t, "failed handler"),
		},
		EnqueueResponse{
			CommandMessageID: env.MessageId,
			Ok:               true,
		},
	)
}

func (s *state) InduceFailureBeforeNextKeyspaceSet(t *rapid.T) {
	s.queue.Keyspaces.ScheduleFailure(kvtest.BeforeSet)
}

func (s *state) InduceFailureAfterNextKeyspaceSet(t *rapid.T) {
	s.queue.Keyspaces.ScheduleFailure(kvtest.AfterSet)
}
