package messagepump

import (
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// Delivery describes a unit of work scoped to the delivery of a single message
// to a single handler.
type Delivery struct {
	// MessageID is the unique identifier of the message being delivered.
	MessageID *uuidpb.UUID

	// MessageTypeID is the unique identifier of the message's type.
	MessageTypeID *uuidpb.UUID

	// EnvelopeBytes is the serialized form of the message's envelope.
	EnvelopeBytes []byte

	// Failures is the number of times that delivery has been attempted and
	// failed.
	Failures uint64

	// Stream describes the position of the message within an event stream. It
	// is set by stream-based pumps and is nil for queue-based pumps.
	Stream *DeliveryStream
}

// DeliveryStream describes the position of a message within an event stream,
// and the engine's recorded checkpoint offset for that stream at the moment the
// delivery was acquired.
type DeliveryStream struct {
	// ID is the unique identifier of the stream.
	ID *uuidpb.UUID

	// EventOffset is the offset of the message within the stream.
	EventOffset uint64

	// CheckpointOffset is the engine's recorded checkpoint offset for the
	// stream at the moment the delivery was acquired.
	CheckpointOffset uint64
}
