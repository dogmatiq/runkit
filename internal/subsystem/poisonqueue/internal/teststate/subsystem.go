package teststate

import (
	"context"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xrapid"
	"github.com/dogmatiq/runkit/internal/subsystem/poisonqueue"
	"github.com/dogmatiq/runkit/internal/x/xtesting/kvtest"
	"pgregory.net/rapid"
)

// Subsystem represents the state of the poison queue subsystem within a test.
type Subsystem struct {
	// Context is the context in which the subsystem is running.
	Context context.Context

	// Keyspaces is the in-memory key/value  store used to persist the queue.
	Keyspaces kvtest.FailableBinaryStore

	// Messages is the _expected_ state of the queue based the messages that
	// have been enqueued.
	Messages uuidpb.Map[*envelopepb.Envelope]

	// EnqueueRequests is used to send [poisonqueue.EnqueueRequest] requests to
	// [poisonqueue.Service] instances.
	EnqueueRequests chan<- poisonqueue.EnqueueRequest
}

// MessagesGen returns a generator that yields existing messages.
func (s *Subsystem) MessagesGen(t *rapid.T) *rapid.Generator[*envelopepb.Envelope] {
	if s.Messages.Len() == 0 {
		t.Skip("queue is empty")
	}

	return xrapid.SampledFromSeq(s.Messages.Values())
}

// SendEnqueueRequest sends an [poisonqueue.EnqueueRequest] request to a
// service.
func (s *Subsystem) SendEnqueueRequest(t *rapid.T, req poisonqueue.EnqueueRequest, want poisonqueue.EnqueueResponse) {
	t.Helper()

	if req.Response != nil {
		panic("test misuse: do not set req.Response channel")
	}
	response := make(chan poisonqueue.EnqueueResponse)
	req.Response = response

	for {
		select {
		case <-s.Context.Done():
			t.Fatalf(
				"context cancelled while sending EnqueueRequest: %s",
				context.Cause(s.Context),
			)
		case s.EnqueueRequests <- req:
			t.Logf("sent EnqueueRequest")
		}

		select {
		case <-s.Context.Done():
			t.Fatalf(
				"context cancelled while waiting for EnqueueResponse: %s",
				context.Cause(s.Context),
			)
		case got, ok := <-response:
			if !ok {
				t.Fatal("EnqueueResponse channel was closed")
			}

			t.Log("received EnqueueResponse")

			if got.CommandMessageID == nil {
				t.Fatal("EnqueueResponse does not have a command ID")
			}

			if !got.CommandMessageID.Equal(want.CommandMessageID) {
				t.Fatalf("EnqueueResponse has unexpected command ID: got %s, want %s", got.CommandMessageID, want.CommandMessageID)
			}

			if got.Ok != want.Ok {
				if want.Ok {
					t.Log("EnqueueResponse indicates request was rejected, retrying")
					continue
				}

				t.Fatalf("EnqueueResponse has unexpected Ok value: got %t, want %t", got.Ok, want.Ok)
			}

			s.Messages.Set(
				req.CommandEnvelope.MessageId,
				req.CommandEnvelope,
			)

			return
		}
	}
}
