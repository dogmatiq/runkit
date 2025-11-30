package eventstream_test

import (
	"testing"

	"github.com/dogmatiq/dapper"
	"github.com/dogmatiq/enginekit/collections/maps"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/telemetry"
	"github.com/dogmatiq/runkit/internal/x/xrapid"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

func TestSupervisor(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		journals := &memoryjournal.BinaryStore{}
		shutdown := make(chan struct{})
		appendEvents := make(chan AppendEvents)
		eventsAppended := make(chan EventsAppended)

		supervisor := &Supervisor{
			Journals:       journals,
			Shutdown:       shutdown,
			AppendEvents:   appendEvents,
			EventsAppended: eventsAppended,
			Telemetry:      telemetry.NewTestProvider(t),
		}

		state := state{
			AppendEvents:   appendEvents,
			EventsAppended: eventsAppended,
		}

		// Setup a channel to signal when the test has ended.
		testDone := make(chan struct{})
		supervisorDone := make(chan struct{})
		t.Cleanup(func() {
			close(testDone)
			close(shutdown)
			<-supervisorDone
		})

		// Run the supervisor in the background for the duration of the test.
		go func() {
			defer close(supervisorDone)

			err := supervisor.Run(t.Context())

			select {
			case <-testDone:
				// Don't fail the test if it's already ended.
			default:
				if err != nil {
					t.Fatalf("supervisor failed: %s", err)
				} else {
					t.Fatal("supervisor exited before the test completed")
				}
			}
		}()

		t.Repeat(map[string]func(*rapid.T){
			"append new events to a new stream": func(t *rapid.T) {
				req := AppendEvents{
					StreamID: xrapid.
						UUID().
						Filter(
							func(u *uuidpb.UUID) bool {
								return !state.Streams.Has(u)
							},
						).
						Draw(t, "stream ID"),
				}

				for range rapid.IntRange(0, 3).Draw(t, "number of events") {
					req.Events = append(
						req.Events,
						xrapid.Envelope().Draw(t, "event envelope"),
					)
				}

				state.TestAppendEvents(t, req, false)
			},
		})
	})
}

type state struct {
	Streams        maps.Proto[*uuidpb.UUID, []*envelopepb.Envelope]
	AppendEvents   chan<- AppendEvents
	EventsAppended <-chan EventsAppended
}

func (s *state) TestAppendEvents(
	t *rapid.T,
	req AppendEvents,
	duplicates bool,
) {
	stream := s.Streams.Get(req.StreamID)
	wantBegin := uint64(len(stream))

	if duplicates {
		if len(req.Events) == 0 {
			panic("test misuse: cannot test duplicate events with an empty event set")
		}

		found := false
		for offset, env := range req.Events {
			if env.MessageId.Equal(req.Events[0].MessageId) {
				wantBegin = uint64(offset)
				found = true
			}
		}

		if !found {
			panic("test misuse: append request is marked as containing duplicates, but the events are not on the stream")
		}
	}

	wantEnd := wantBegin + uint64(len(req.Events))

	reply := make(chan AppendEventsReply, 1)
	req.Reply = reply

	select {
	case <-t.Context().Done():
		t.Fatalf("context canceled while waiting to AppendEvents request: %s", t.Context().Err())
	case s.AppendEvents <- req:
		t.Log("sent AppendEvents request")
	}

	select {
	case <-t.Context().Done():
		t.Fatalf("context canceled while waiting for AppendEventsReply: %s", t.Context().Err())
	case rep := <-reply:
		t.Log("received AppendEventsReply")
		s.validateAppendEventsReply(t, rep, duplicates, wantBegin, wantEnd)
	}

	if duplicates {
		return
	}

	select {
	case <-t.Context().Done():
		t.Fatalf("context canceled while waiting for EventsAppended notification: %s", t.Context().Err())
	case n := <-s.EventsAppended:
		t.Log("received EventsAppended notification")
		s.validateEventsAppendedNotification(t, req, n, wantBegin)

		s.Streams.Set(
			req.StreamID,
			append(
				stream,
				req.Events...,
			),
		)
	}
}

func (s *state) validateAppendEventsReply(
	t *rapid.T,
	rep AppendEventsReply,
	duplicates bool,
	wantBegin, wantEnd uint64,
) {
	if rep.BeginOffset != wantBegin || rep.EndOffset != wantEnd {
		t.Errorf(
			"unexpected AppendEventsReply offsets: got [%d, %d), want [%d, %d)",
			rep.BeginOffset,
			rep.EndOffset,
			wantBegin,
			wantEnd,
		)
	}

	if duplicates != rep.Deduplicated {
		t.Errorf("unexpected AppendEventsReply.Deduplicated value: got %t, want %t", rep.Deduplicated, duplicates)
	}
}

func (s *state) validateEventsAppendedNotification(
	t *rapid.T,
	req AppendEvents,
	n EventsAppended,
	wantOffset uint64,
) {

	if !n.StreamID.Equal(req.StreamID) {
		t.Errorf(
			"unexpected EventsAppended.StreamID value: got %s, want %s",
			n.StreamID,
			req.StreamID,
		)
	}

	if n.Offset != wantOffset {
		t.Errorf(
			"unexpected EventsAppended.Offset value: got %d, want %d",
			n.Offset,
			wantOffset,
		)
	}

	if len(n.Events) != len(req.Events) {
		t.Errorf(
			"unexpected EventsAppended.Events length: got %d, want %d",
			len(n.Events),
			len(req.Events),
		)
	} else {
		for i, got := range n.Events {
			want := req.Events[i]

			if !proto.Equal(got, want) {
				t.Errorf(
					"unexpected EventsAppended.Events[%d] value:\ngot %s\nwant %s",
					i,
					dapper.Format(got),
					dapper.Format(want),
				)
			}
		}
	}
}
