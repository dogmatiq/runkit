package source

import (
	"uuid"

	"github.com/dogmatiq/enginekit/optional"
	"github.com/dogmatiq/protobuf/envelopepb"
)

// Segment is a contiguous piece of one event stream, the half-open range
// [Begin, End), carrying the events in that range that match a set of message
// types.
type Segment struct {
	// StreamID is the ID of the event stream that this is a segment of.
	StreamID uuid.UUID

	// Begin and End bound the segment, [Begin, End).
	Begin, End uint64

	// StreamLength is the number of events in the stream when the source
	// loaded the segment, which is also the offset at which the next event was
	// to be appended. It is never less than End. The stream may have grown by
	// the time that the segment is delivered.
	//
	// It is absent if the source does not know the length of the stream.
	StreamLength optional.Optional[uint64]

	// Envelopes are the events in the segment that match the message types for
	// which it was read, in offset order.
	//
	// It may be empty, indicating only that the range holds no events of those
	// types.
	//
	// Each envelope has the [envelopepb.EventStreamPosition] extension set.
	Envelopes []*envelopepb.Envelope
}
