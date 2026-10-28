package scanner

import (
	"sync/atomic"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
)

// Consumer represents a subscriber registered to receive events from a
// specific event stream.
type Consumer struct {
	// ID is the unique identifier of the consumer, used purely for logging
	// purposes.
	ID *uuidpb.UUID

	// StreamID is the ID of the event stream to which the consumer is
	// subscribed.
	StreamID *uuidpb.UUID

	// Checkpoint is the offset of the next event that the consumer expects to
	// receive.
	//
	// It is advanced by whichever scanner is currently delivering events to the
	// consumer, and may be read concurrently by others, such as for logging.
	Checkpoint atomic.Uint64

	// MessageTypes is the set of message types that the consumer is interested
	// in receiving. It must not be empty.
	MessageTypes *uuidpb.Set

	// Batches is the channel on which the consumer is sent batches of events
	// that are appended to the specified stream ID, if they are one of the
	// specified types.
	Batches chan<- Batch

	// Gone is a latch that is set when the consumer no longer expects to
	// receive events. A scanner serving the consumer stops when it is set.
	Gone xsync.Latch
}

// Batch is a set of events delivered to a consumer, along with the checkpoint
// from which the consumer should resume afterwards.
type Batch struct {
	// StreamID is the ID of the event stream from which the events were read.
	StreamID *uuidpb.UUID

	// Envelopes are the delivered events, in offset order.
	//
	// It may be empty, indicating only that the checkpoint has advanced past
	// events that are not of a type the consumer is interested in.
	//
	// Each envelope has the [envelopepb.EventStreamPosition] extension set.
	Envelopes []*envelopepb.Envelope

	// Checkpoint is the offset immediately after the range of the stream that
	// this batch covers.
	Checkpoint uint64
}
