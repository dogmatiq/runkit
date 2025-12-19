package teststate

import (
	"context"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream"
	"github.com/dogmatiq/runkit/internal/x/xrapid"
	"github.com/dogmatiq/runkit/internal/x/xtesting/journaltest"
	"github.com/dogmatiq/runkit/internal/x/xtesting/settest"
	"pgregory.net/rapid"
)

// Subsystem represents the Subsystem of the eventstream system within the a
// test.
type Subsystem struct {
	// Context is the context in which the subsystem is running.
	Context context.Context

	// Journals is the in-memory journal store used to persist event streams.
	Journals journaltest.FailableBinaryStore

	// Sets is the in-memory set store used to persist the event stream
	// registry.
	Sets settest.FailableBinaryStore

	// Streams is the set of event streams known to the subsystem. This
	// represents the _expected_ state of the streams based on prior
	// observations.
	Streams uuidpb.Map[*Stream]

	// AppendEventsRequests is used to send [eventstream.AppendEventsRequest]
	// requests to a supervisor.
	AppendEventsRequests chan<- eventstream.AppendEventsRequest
}

// StreamsGen returns a generator that yields existing streams.
func (s *Subsystem) StreamsGen(t *rapid.T) *rapid.Generator[*Stream] {
	if s.Streams.Len() == 0 {
		t.Skip("stream is empty")
	}

	return xrapid.SampledFromSeq(s.Streams.Values())
}

// SendAppendEventsRequest sends an [eventstream.AppendEventsRequest] request to
// the supervisor.
func (s *Subsystem) SendAppendEventsRequest(t *rapid.T, req eventstream.AppendEventsRequest, want eventstream.AppendEventsResponse) {
	t.Helper()

	if req.Response != nil {
		panic("test misuse: do not set req.Response channel")
	}

	for {
		response := make(chan eventstream.AppendEventsResponse, 1)
		req.Response = response

		select {
		case <-s.Context.Done():
			t.Fatalf("[%s] context cancelled while sending AppendEventsRequest: %s", req.StreamID, context.Cause(s.Context))
		case s.AppendEventsRequests <- req:
			t.Logf("[%s] sent AppendEventsRequest request", req.StreamID)
		}

		select {
		case <-s.Context.Done():
			t.Fatalf("[%s] context cancelled while waiting for AppendEventsResponse: %s", req.StreamID, context.Cause(s.Context))
		case got, ok := <-response:
			if !ok {
				t.Logf("[%s] AppendEventsResponse was rejected, retrying", req.StreamID)
				continue
			}

			t.Logf("[%s] received AppendEventsResponse", req.StreamID)

			if got.BeginOffset != want.BeginOffset || got.EndOffset != want.EndOffset {
				t.Fatalf(
					"[%s] AppendEventsResponse has unexpected offset range: got [%d, %d), want [%d, %d)",
					req.StreamID,
					got.BeginOffset,
					got.EndOffset,
					want.BeginOffset,
					want.EndOffset,
				)
			}

			stream, ok := s.Streams.Get(req.StreamID)

			if !ok {
				stream = &Stream{
					ID: req.StreamID,
				}
				s.Streams.Set(req.StreamID, stream)
			}

			stream.append(t, req, got)

			return
		}
	}
}
