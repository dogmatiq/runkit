package eventstream_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/persistencekit/journal"
	. "github.com/dogmatiq/runkit/internal/subsystem/eventstream"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream/internal/teststate"
	"github.com/dogmatiq/runkit/internal/x/xrapid"
	"github.com/dogmatiq/runkit/internal/x/xtesting/journaltest"
	"pgregory.net/rapid"
)

func TestSupervisor(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		requests := make(chan AppendRequest)

		// Create a context under which we execute the supervisors.
		//
		// Note that it's NOT based on [testing.T.Context], so that we can
		// execute the supervisor's graceful shutdown logic when the test ends.
		ctx, cancel := context.WithCancelCause(context.Background())

		subsystem := &teststate.Subsystem{
			Context:        ctx,
			AppendRequests: requests,
		}

		var (
			journals journal.BinaryStore = &subsystem.Journals
		)

		telem := telemetry.NewTestProvider(t)

		if testing.Verbose() {
			journals = journal.WithTelemetry(
				journals,
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
		// Each supervisor represents a separate running instance of the event
		// stream subsystem, as would normally be run on separate
		// machines/containers in a production system.
		var supervisors sync.WaitGroup

		for idx := range 3 {
			supervisors.Go(func() {
				sup := &Supervisor{
					ID:             uuidpb.Generate(),
					Journals:       journals,
					BufferSize:     2, // small buffer size to increase chance of contention
					Shutdown:       shutdown,
					AppendRequests: requests,
					Telemetry:      telem,
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
	s.guardAgainstDuplicateEvents(t)
}

func (s *state) guardAgainstDuplicateEvents(t *rapid.T) {
	for part := range s.subsystem.Partitions.Values() {
		if part.NextOffset == 0 {
			t.Fatalf("invariant violated: stream partition %q exists with no events", part)
		}

		var seen uuidpb.Set

		for _, env := range part.Events {
			if seen.Has(env.MessageId) {
				t.Fatalf(
					"invariant violated: duplicate event %s found on stream partition %q",
					env.MessageId,
					part,
				)
			}

			seen.Add(env.MessageId)
		}
	}
}

func (s *state) AppendEventsToNewStream(t *rapid.T) {
	req := AppendRequest{
		PartitionID: uuidpb.Generate(),
	}

	for range rapid.IntRange(1, 3).Draw(t, "number of events to append") {
		req.EventEnvelopes = append(
			req.EventEnvelopes,
			xrapid.Envelope().Draw(t, "event"),
		)
	}

	s.subsystem.SendAppendRequest(t, req, AppendResponse{
		BeginOffset: 0,
		EndOffset:   uint64(len(req.EventEnvelopes)),
	})
}

func (s *state) AppendMoreEventsToAnExistingStream(t *rapid.T) {
	part := s.subsystem.PartitionsGen(t).Draw(t, "stream partition")

	req := AppendRequest{
		PartitionID:          part.ID,
		LowestPossibleOffset: rapid.Uint64Range(0, part.NextOffset).Draw(t, "deduplication hint"),
	}

	for range rapid.IntRange(1, 3).Draw(t, "number of events to append") {
		req.EventEnvelopes = append(
			req.EventEnvelopes,
			xrapid.Envelope().Draw(t, "event"),
		)
	}

	s.subsystem.SendAppendRequest(t, req, AppendResponse{
		BeginOffset: part.NextOffset,
		EndOffset:   part.NextOffset + uint64(len(req.EventEnvelopes)),
	})
}

func (s *state) ReappendPriorEvents(t *rapid.T) {
	part := s.subsystem.PartitionsGen(t).Draw(t, "stream partition")
	prior := part.AppendRequestsGen(t).Draw(t, "prior request")
	offset, ok := part.FindOffset(prior.EventEnvelopes[0].MessageId)
	if !ok {
		t.Fatalf("unable to find offset of existing event in partition %q", part)
	}

	req := AppendRequest{
		PartitionID:          prior.PartitionID,
		EventEnvelopes:       prior.EventEnvelopes,
		LowestPossibleOffset: rapid.Uint64Range(0, offset).Draw(t, "deduplication hint"),
	}

	s.subsystem.SendAppendRequest(t, req, AppendResponse{
		BeginOffset: offset,
		EndOffset:   offset + uint64(len(prior.EventEnvelopes)),
	})
}

func (s *state) InduceFailureOnNextJournalOpen(t *rapid.T) {
	s.subsystem.Journals.ScheduleFailure(journaltest.BeforeOpen)
}

func (s *state) InduceFailureBeforeNextJournalAppend(t *rapid.T) {
	s.subsystem.Journals.ScheduleFailure(journaltest.BeforeAppend)
}

func (s *state) InduceFailureAfterNextJournalAppend(t *rapid.T) {
	s.subsystem.Journals.ScheduleFailure(journaltest.AfterAppend)
}
