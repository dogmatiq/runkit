package poisonqueue_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/enginekit/x/xrapid"
	"github.com/dogmatiq/persistencekit/kv"
	. "github.com/dogmatiq/runkit/internal/subsystem/poisonqueue"
	"github.com/dogmatiq/runkit/internal/subsystem/poisonqueue/internal/teststate"
	"github.com/dogmatiq/runkit/internal/x/xtesting/kvtest"
	"pgregory.net/rapid"
)

func TestSupervisor(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		requests := make(chan EnqueueRequest)

		// Create a context under which we execute the supervisors.
		//
		// Note that it's NOT based on [testing.T.Context], so that we can
		// execute the supervisor's graceful shutdown logic when the test ends.
		ctx, cancel := context.WithCancelCause(context.Background())

		subsystem := &teststate.Subsystem{
			Context:         ctx,
			EnqueueRequests: requests,
		}

		var keyspaces kv.BinaryStore = &subsystem.Keyspaces
		telem := telemetry.NewTestProvider(t)

		if testing.Verbose() {
			keyspaces = kv.WithTelemetry(
				keyspaces,
				telem.TracerProvider,
				telem.MeterProvider,
				telem.LoggerProvider,
			)
		}

		// shutdown is a channel used to signal all supervisors to shut down
		// gracefully.
		shutdown := make(chan struct{})

		// Run multiple supervisors in the background for the duration of the
		// tests.
		//
		// Each supervisor represents a separate running instance of the
		// subsystem, as would normally be run on separate machines/containers
		// in a production system.
		var supervisors sync.WaitGroup

		for idx := range 3 {
			supervisors.Go(func() {
				sup := &Supervisor{
					ID:              uuidpb.Generate(),
					Keyspaces:       keyspaces,
					Shutdown:        shutdown,
					EnqueueRequests: requests,
					Telemetry:       telem,
				}

				if err := sup.Run(ctx); err != nil {
					t.Errorf("supervisor %d failed: %s", idx, err)
					cancel(fmt.Errorf("supervisor %d failed: %w", idx, err))
				}
			})
		}

		// When the test ends signal all supervisors to shut down gracefully
		// and wait for them to stop.
		t.Cleanup(func() {
			close(shutdown)
			supervisors.Wait()
			cancel(errors.New("test completed"))
		})

		t.Repeat(
			rapid.StateMachineActions(&state{subsystem}),
		)
	})
}

type state struct {
	subsystem *teststate.Subsystem
}

func (s *state) Check(t *rapid.T) {
}

func (s *state) EnqueueCommand(t *rapid.T) {
	env := xrapid.Envelope().Draw(t, "command")

	s.subsystem.SendEnqueueRequest(
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
	env := s.subsystem.MessagesGen(t).Draw(t, "existing message")

	s.subsystem.SendEnqueueRequest(
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
	s.subsystem.Keyspaces.ScheduleFailure(kvtest.BeforeSet)
}

func (s *state) InduceFailureAfterNextKeyspaceSet(t *rapid.T) {
	s.subsystem.Keyspaces.ScheduleFailure(kvtest.AfterSet)
}
