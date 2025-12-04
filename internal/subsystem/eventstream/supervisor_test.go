package eventstream_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/dogmatiq/enginekit/collections/sets"
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
		requests := make(chan AppendEventsRequest)
		notifications := make(chan EventsAppendedNotification)

		// Create a context under which we execute the supervisors.
		//
		// Note that it's NOT based on [testing.T.Context], so that we can
		// execute the supervisor's graceful shutdown logic when the test ends.
		ctx, cancel := context.WithCancelCause(context.Background())

		subsystem := &teststate.Subsystem{
			Context:       ctx,
			Requests:      requests,
			Notifications: notifications,
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
				// TODO: add a supervisor ID of some kind
				telem := telemetry.NewTestProvider(t)

				sup := &Supervisor{
					Journals: journal.WithTelemetry(
						&subsystem.Journals,
						telem.TracerProvider,
						telem.MeterProvider,
						telem.LoggerProvider,
					),
					Shutdown:      shutdown,
					Requests:      requests,
					Notifications: notifications,
					Telemetry:     telem,
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
	for stream := range s.subsystem.Streams.Values() {
		if stream.NextOffset == 0 {
			t.Fatalf("[%s] invariant violated: stream has no events", stream)
		}

		var seen sets.Proto[*uuidpb.UUID]

		for _, env := range stream.Events {
			if seen.Has(env.MessageId) {
				t.Fatalf(
					"[%s] invariant violated: duplicate event %s found on stream",
					stream,
					env.MessageId,
				)
			}

			seen.Add(env.MessageId)
		}
	}
}

func (s *state) AppendEventsToNewStream(t *rapid.T) {
	req := AppendEventsRequest{
		StreamID: uuidpb.Generate(),
	}

	for range rapid.IntRange(1, 3).Draw(t, "number of events to append") {
		req.Events = append(
			req.Events,
			xrapid.Envelope().Draw(t, "event"),
		)
	}

	s.subsystem.SendAppendEventsRequest(t, req, AppendEventsResponse{
		BeginOffset: 0,
		EndOffset:   uint64(len(req.Events)),
	})

	s.subsystem.ExpectEventsAppendedNotification(t, EventsAppendedNotification{
		StreamID: req.StreamID,
		Offset:   0,
		Events:   req.Events,
	})
}

func (s *state) AppendMoreEventsToAnExistingStream(t *rapid.T) {
	stream := s.subsystem.StreamsGen(t).Draw(t, "stream")

	req := AppendEventsRequest{
		StreamID:          stream.ID,
		DeduplicationHint: rapid.Uint64Range(0, stream.NextOffset).Draw(t, "deduplication hint"),
	}

	for range rapid.IntRange(1, 3).Draw(t, "number of events to append") {
		req.Events = append(
			req.Events,
			xrapid.Envelope().Draw(t, "event"),
		)
	}

	s.subsystem.SendAppendEventsRequest(t, req, AppendEventsResponse{
		BeginOffset: stream.NextOffset,
		EndOffset:   stream.NextOffset + uint64(len(req.Events)),
	})

	s.subsystem.ExpectEventsAppendedNotification(t, EventsAppendedNotification{
		StreamID: stream.ID,
		Offset:   stream.NextOffset,
		Events:   req.Events,
	})
}

func (s *state) ReappendExistingEvents(t *rapid.T) {
	stream := s.subsystem.StreamsGen(t).Draw(t, "stream")
	n := stream.NotificationsGen(t).Draw(t, "prior notification")

	req := AppendEventsRequest{
		StreamID:          stream.ID,
		Events:            n.Events,
		DeduplicationHint: rapid.Uint64Range(0, n.Offset).Draw(t, "deduplication hint"),
	}

	s.subsystem.SendAppendEventsRequest(t, req, AppendEventsResponse{
		BeginOffset: n.Offset,
		EndOffset:   n.Offset + uint64(len(n.Events)),
	})

	s.subsystem.ExpectEventsAppendedNotification(t, n)
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
