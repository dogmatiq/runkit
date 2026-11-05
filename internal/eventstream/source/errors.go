package source

import (
	"fmt"

	"github.com/dogmatiq/protobuf/uuidpb"
)

// ConsumerTooSlowError is the error that [Source.Watch] returns when its caller
// can not keep up, so that a stream can no longer be delivered contiguously.
//
// It corresponds to the RESOURCE_EXHAUSTED status, with an attached
// [messaginggrpc.EventStreamConsumerTooSlow] value, that ends a WatchEvents
// call.
type ConsumerTooSlowError struct {
	// StreamID is the ID of the stream that could no longer be delivered
	// contiguously.
	StreamID *uuidpb.UUID
}

func (e *ConsumerTooSlowError) Error() string {
	return fmt.Sprintf(
		"event stream %s can no longer be delivered contiguously, as the consumer is too slow",
		e.StreamID,
	)
}
