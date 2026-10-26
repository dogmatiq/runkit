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

	// Envelopes is the channel on which the consumer is sent events that are
	// appended to the specified stream ID, if they are one of the specified
	// types.
	Envelopes chan<- *envelopepb.Envelope

	// Gone is a latch that is set when the consumer no longer expects to
	// receive events. It is used as a signal to abort writes to the envelopes
	// channel that would otherwise never unblock.
	Gone xsync.Latch
}
