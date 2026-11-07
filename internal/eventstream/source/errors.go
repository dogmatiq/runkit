package source

import "errors"

var (
	// ErrNotContiguous is the error that [Source.Watch] returns when it can no
	// longer deliver a stream contiguously, such as when its caller can not
	// keep up. The caller should watch again, and read any events it missed.
	ErrNotContiguous = errors.New("event stream can not be delivered contiguously")

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
