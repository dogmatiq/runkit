package eventstream_test

import (
	"context"
	"testing"

	"github.com/dogmatiq/enginekit/collections/sets"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/journal"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/telemetry"
	"github.com/dogmatiq/runkit/internal/x/xrapid"
	"pgregory.net/rapid"
)

func TestSupervisor(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		tele := telemetry.NewTestProvider(t)

		state := &subsystemState{
			Shutdown:       make(chan struct{}),
			AppendEvents:   make(chan AppendEvents),
			EventsAppended: make(chan EventsAppended),
		}

		supervisor := &Supervisor{
			Journals: journal.WithTelemetry(
				&state.Journals,
				tele.TracerProvider,
				tele.MeterProvider,
				tele.LoggerProvider,
			),
			Shutdown:       state.Shutdown,
			AppendEvents:   state.AppendEvents,
			EventsAppended: state.EventsAppended,
			Telemetry:      tele,
		}

		// Setup a channel to signal when the test has ended.
		supervisorDone := make(chan error, 1)

		t.Cleanup(func() {
			close(state.Shutdown)

			if err := <-supervisorDone; err != nil {
				t.Fatalf("supervisor failed: %s", err)
			}
		})

		// Run the supervisor in the background for the duration of the test.
		// We don't use the test context here, because we want to be able to
		// control how it shuts down in the cleanup function above.
		go func() {
			supervisorDone <- supervisor.Run(context.Background())
		}()

		t.Repeat(rapid.StateMachineActions(state))
	})
}

func (s *subsystemState) Check(t *rapid.T) {
	for stream := range s.Streams.Values() {
		if stream.NextOffset() == 0 {
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

func (s *subsystemState) AppendEventsToNewStream(t *rapid.T) {
	req := AppendEvents{
		StreamID: uuidpb.Generate(),
	}

	for range rapid.IntRange(1, 3).Draw(t, "number of events") {
		req.Events = append(
			req.Events,
			xrapid.Envelope().Draw(t, "event envelope"),
		)
	}

	s.sendAppendEvents(t, req, AppendEventsReply{
		BeginOffset:  0,
		EndOffset:    uint64(len(req.Events)),
		Deduplicated: false,
	})

	s.awaitEventsAppended(t, EventsAppended{
		StreamID: req.StreamID,
		Offset:   0,
		Events:   req.Events,
	})
}

func (s *subsystemState) AppendMoreEventsToAnExistingStream(t *rapid.T) {
	stream := s.drawExistingStream(t)

	req := AppendEvents{
		StreamID: stream.ID,
	}

	for range rapid.IntRange(1, 3).Draw(t, "number of events") {
		req.Events = append(
			req.Events,
			xrapid.Envelope().Draw(t, "event envelope"),
		)
	}

	s.sendAppendEvents(t, req, AppendEventsReply{
		BeginOffset:  stream.NextOffset(),
		EndOffset:    stream.NextOffset() + uint64(len(req.Events)),
		Deduplicated: false,
	})

	s.awaitEventsAppended(t, EventsAppended{
		StreamID: req.StreamID,
		Offset:   stream.NextOffset(),
		Events:   req.Events,
	})
}

func (s *subsystemState) AppendZeroEvents(t *rapid.T) {
	req := AppendEvents{
		StreamID: uuidpb.Generate(),
	}

	s.sendAppendEvents(t, req, AppendEventsReply{})
	s.ensureNoEventsAppended(t, req.StreamID)
}

// func (s *subsystemState) AppendDuplicateEvent(t *rapid.T) {
// 	stream := s.drawExistingStream(t)
// 	offset := stream.DrawOffset(t)

// 	req := AppendEvents{
// 		StreamID: stream.ID,
// 		Events:   []*envelopepb.Envelope{stream.Events[offset]},

// 		// Draw a deduplication hint that is somewhere between 0 and the actual
// 		// offset of the duplicated event so that the event stream will always
// 		// find the original event.
// 		//
// 		// We don't bother testing what happens if the hint is greater than the
// 		// actual offset, as this is essentially a misuse of the internal API.
// 		DeduplicationHint: rapid.Uint64Range(0, offset).Draw(t, "deduplication hint"),
// 	}

// 	s.sendAppendEvents(t, req, AppendEventsReply{
// 		BeginOffset:  offset,
// 		EndOffset:    offset + 1,
// 		Deduplicated: true,
// 	})

// 	s.ensureNoEventsAppended(t, req.StreamID)
// }
