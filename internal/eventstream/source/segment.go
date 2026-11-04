package source

import (
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
)

// Segment is a contiguous piece of one event stream, the half-open range
// [Begin, End), carrying the events in that range that match a set of message
// types.
type Segment struct {
	// StreamID is the ID of the event stream that this is a segment of.
	StreamID *uuidpb.UUID

	// Begin and End bound the segment, [Begin, End).
	Begin, End uint64

	// Envelopes are the events in the segment that match the message types for
	// which it was read, in offset order.
	//
	// It may be empty, indicating only that the range holds no events of those
	// types.
	//
	// Each envelope has the [envelopepb.EventStreamPosition] extension set.
	Envelopes []*envelopepb.Envelope
}
