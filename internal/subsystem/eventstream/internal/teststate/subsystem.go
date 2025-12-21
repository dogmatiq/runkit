package teststate

import (
	"context"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream"
	"github.com/dogmatiq/runkit/internal/x/xrapid"
	"github.com/dogmatiq/runkit/internal/x/xtesting/journaltest"
	"pgregory.net/rapid"
)

// Subsystem represents the state of the eventstream subsystem within a test.
type Subsystem struct {
	// Context is the context in which the subsystem is running.
	Context context.Context

	// Journals is the in-memory journal store used to persist events.
	Journals journaltest.FailableBinaryStore

	// Partitions is the set of known stream partitions. This represents the
	// _expected_ state of the partitions based the events that have been
	// appended.
	Partitions uuidpb.Map[*Partition]

	// AppendEventsRequests is used to send [eventstream.AppendEventsRequest]
	// requests to a supervisor.
	AppendEventsRequests chan<- eventstream.AppendEventsRequest
}

// PartitionsGen returns a generator that yields existing partitions.
func (s *Subsystem) PartitionsGen(t *rapid.T) *rapid.Generator[*Partition] {
	if s.Partitions.Len() == 0 {
		t.Skip("stream is empty (has no partitions)")
	}

	return xrapid.SampledFromSeq(s.Partitions.Values())
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
			t.Fatalf("context cancelled while sending AppendEventsRequest for partition %s: %s", req.PartitionID, context.Cause(s.Context))
		case s.AppendEventsRequests <- req:
			t.Logf("sent AppendEventsRequest request for partition %s", req.PartitionID)
		}

		select {
		case <-s.Context.Done():
			t.Fatalf("context cancelled while waiting for AppendEventsResponse for partition %s: %s", req.PartitionID, context.Cause(s.Context))
		case got, ok := <-response:
			if !ok {
				t.Logf("AppendEventsResponse for partition %s was rejected, retrying", req.PartitionID)
				continue
			}

			t.Logf("received AppendEventsResponse for partition %s", req.PartitionID)

			if got.BeginOffset != want.BeginOffset || got.EndOffset != want.EndOffset {
				t.Fatalf(
					"AppendEventsResponse has unexpected offset range for partition %d: got [%d, %d), want [%d, %d)",
					req.PartitionID,
					got.BeginOffset,
					got.EndOffset,
					want.BeginOffset,
					want.EndOffset,
				)
			}

			part, ok := s.Partitions.Get(req.PartitionID)

			if !ok {
				part = &Partition{
					ID: req.PartitionID,
				}
				s.Partitions.Set(req.PartitionID, part)
			}

			part.append(t, req, got)

			return
		}
	}
}
