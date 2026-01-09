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

	// AppendRequests is used to send [eventstream.AppendRequest] requests to a
	// supervisor.
	AppendRequests chan<- eventstream.AppendRequest
}

// PartitionsGen returns a generator that yields existing partitions.
func (s *Subsystem) PartitionsGen(t *rapid.T) *rapid.Generator[*Partition] {
	if s.Partitions.Len() == 0 {
		t.Skip("stream is empty (has no partitions)")
	}

	return xrapid.SampledFromSeq(s.Partitions.Values())
}

// SendAppendRequest sends an [eventstream.AppendRequest] request to
// the supervisor.
func (s *Subsystem) SendAppendRequest(t *rapid.T, req eventstream.AppendRequest, want eventstream.AppendResponse) {
	t.Helper()

	if req.Response != nil {
		panic("test misuse: do not set req.Response channel")
	}
	response := make(chan eventstream.AppendResponse)
	req.Response = response

	for {
		select {
		case <-s.Context.Done():
			t.Fatalf(
				"context cancelled while sending AppendRequest to partition %s: %s",
				req.PartitionID,
				context.Cause(s.Context),
			)
		case s.AppendRequests <- req:
			t.Logf(
				"sent AppendRequest to partition %s",
				req.PartitionID,
			)
		}

		select {
		case <-s.Context.Done():
			t.Fatalf(
				"context cancelled while waiting for AppendResponse: %s",
				context.Cause(s.Context),
			)
		case got, ok := <-response:
			if !ok {
				t.Fatal("AppendResponse channel was closed")
			}

			if got.FirstEventMessageID == nil {
				t.Fatal("AppendResponse does not have a request ID")
			}

			if !got.FirstEventMessageID.Equal(req.EventEnvelopes[0].MessageId) {
				t.Fatalf("AppendResponse has unexpected request ID: got %s, want %s", got.FirstEventMessageID, req.EventEnvelopes[0].MessageId)
			}

			if !got.Ok {
				t.Log("AppendResponse indicates request was rejected, retrying")
				continue
			}

			t.Log("received AppendResponse")

			if got.BeginOffset != want.BeginOffset || got.EndOffset != want.EndOffset {
				t.Fatalf(
					"AppendResponse has unexpected offset range: got [%d, %d), want [%d, %d)",
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
