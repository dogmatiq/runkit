package eventstream_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/enginekit/x/xrapid"
	"github.com/dogmatiq/persistencekit/journal"
	. "github.com/dogmatiq/runkit/internal/subsystem/eventstream"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream/internal/teststate"
	"github.com/dogmatiq/runkit/internal/x/xtesting/journaltest"
	"pgregory.net/rapid"
)

func TestService(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		appendRequests := make(chan AppendRequest)

		// Create a context under which we run the services.
		//
		// Note that it's NOT based on [testing.T.Context], so that we can stop
		// the services' gracefully when the test ends.
		ctx, cancel := context.WithCancelCause(context.Background())

		stream := &teststate.EventStream{
			Context:        ctx,
			AppendRequests: appendRequests,
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
				telem := telemetry.
					NewTestProvider(t).
					WithAttrs(telemetry.Int("service.id", idx))

				journals := journal.BinaryStore(&stream.Journals)

				if testing.Verbose() {
					journals = journal.WithTelemetry(
						journals,
						telem.TracerProvider,
						telem.MeterProvider,
						telem.LoggerProvider,
					)
				}

				svc := &Service{
					Journals:       journals,
					BufferSize:     2, // small buffer size to increase chance of contention
					Stop:           stop,
					AppendRequests: appendRequests,
					Telemetry:      telem,
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
			rapid.StateMachineActions(&state{stream}),
		)
	})
}

type state struct {
	stream *teststate.EventStream
}

func (s *state) Check(t *rapid.T) {
	s.guardAgainstDuplicateEvents(t)
}

func (s *state) guardAgainstDuplicateEvents(t *rapid.T) {
	for part := range s.stream.Partitions.Values() {
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

	s.stream.SendAppendRequest(t, req, AppendResponse{
		FirstEventMessageID: req.EventEnvelopes[0].MessageId,
		Ok:                  true,
		Offsets: OffsetRange{
			Begin: 0,
			End:   Offset(len(req.EventEnvelopes)),
		},
	})
}

func (s *state) AppendMoreEventsToAnExistingStream(t *rapid.T) {
	part := s.stream.PartitionsGen(t).Draw(t, "stream partition")

	req := AppendRequest{
		PartitionID:          part.ID,
		LowestPossibleOffset: xrapid.Uint64Range(0, part.NextOffset).Draw(t, "deduplication hint"),
	}

	for range rapid.IntRange(1, 3).Draw(t, "number of events to append") {
		req.EventEnvelopes = append(
			req.EventEnvelopes,
			xrapid.Envelope().Draw(t, "event"),
		)
	}

	s.stream.SendAppendRequest(t, req, AppendResponse{
		FirstEventMessageID: req.EventEnvelopes[0].MessageId,
		Ok:                  true,
		Offsets: OffsetRange{
			Begin: part.NextOffset,
			End:   part.NextOffset + Offset(len(req.EventEnvelopes)),
		},
	})
}

func (s *state) ReappendPriorEvents(t *rapid.T) {
	part := s.stream.PartitionsGen(t).Draw(t, "stream partition")
	prior := part.AppendRequestsGen(t).Draw(t, "prior request")
	offset, ok := part.FindOffset(prior.EventEnvelopes[0].MessageId)
	if !ok {
		t.Fatalf("unable to find offset of existing event in partition %q", part)
	}

	req := AppendRequest{
		PartitionID:          prior.PartitionID,
		EventEnvelopes:       prior.EventEnvelopes,
		LowestPossibleOffset: xrapid.Uint64Range(0, offset).Draw(t, "deduplication hint"),
	}

	s.stream.SendAppendRequest(t, req, AppendResponse{
		FirstEventMessageID: req.EventEnvelopes[0].MessageId,
		Ok:                  true,
		Offsets: OffsetRange{
			Begin: offset,
			End:   offset + Offset(len(req.EventEnvelopes)),
		},
	})
}

func (s *state) InduceFailureOnNextJournalOpen(t *rapid.T) {
	s.stream.Journals.ScheduleFailure(journaltest.BeforeOpen)
}

func (s *state) InduceFailureBeforeNextJournalAppend(t *rapid.T) {
	s.stream.Journals.ScheduleFailure(journaltest.BeforeAppend)
}

func (s *state) InduceFailureAfterNextJournalAppend(t *rapid.T) {
	s.stream.Journals.ScheduleFailure(journaltest.AfterAppend)
}
