package messagepump

import (
	"github.com/dogmatiq/protobuf/uuidpb"
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
}
