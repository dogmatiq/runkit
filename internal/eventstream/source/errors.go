package source

import "errors"

var (
	// ErrConsumerTooSlow is the error that [Source.Watch] returns when its
	// caller can not keep up, so that a stream can no longer be delivered
	// contiguously.
	ErrConsumerTooSlow = errors.New("consumer is too slow")

	// ErrUnrecognizedStream is the error that [Source.Read] returns for a
	// stream that the source does not have.
	ErrUnrecognizedStream = errors.New("event stream is not recognized")

	// ErrOffsetOutOfRange is the error that [Source.Read] returns when the
	// given offset is beyond the end of the stream.
	ErrOffsetOutOfRange = errors.New("offset is beyond the end of the event stream")

	// ErrInvalidSegment is the error that [Remote] returns when the remote
	// engine sends a segment that is malformed, that holds events of types
	// that were not requested, or that does not begin where the previous
	// segment of its stream ended.
	ErrInvalidSegment = errors.New("invalid segment")
)
