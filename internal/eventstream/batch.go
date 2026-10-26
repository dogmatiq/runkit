package eventstream

import "github.com/dogmatiq/runkit/internal/eventstream/internal/scanner"

// Batch is a set of events delivered to a subscriber of a [Reader], along with
// the checkpoint from which the subscriber should resume afterwards.
//
// A batch may contain no events, indicating only that the checkpoint has
// advanced past events that are not of a type the subscriber is interested in.
type Batch = scanner.Batch
