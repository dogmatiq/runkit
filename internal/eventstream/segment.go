package eventstream

import "github.com/dogmatiq/runkit/internal/eventstream/internal/source"

// Segment is a contiguous piece of one event stream, the half-open range
// [Begin, End), carrying the events in that range that match a set of message
// types.
type Segment = source.Segment
