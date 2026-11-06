package eventstream

import (
	"context"

	"github.com/dogmatiq/protobuf/uuidpb"
)

// Sink loads durable stream progress and processes delivered segments.
// Methods may be called concurrently for different streams. Within one stream,
// calls are sequential. Implementations classify their errors for the supervisor.
type Sink interface {
	// CheckpointOffset returns the offset from which this stream should resume.
	CheckpointOffset(context.Context, *uuidpb.UUID) (uint64, error)

	// ConsumeSegment returns durable progress, which may lag segment.End.
	// checkpoint is the progress returned by the previous successful call, or
	// loaded at the start of this attempt. A segment may partially commit before
	// failing; the consumer reloads durable progress before retrying.
	ConsumeSegment(ctx context.Context, checkpoint uint64, segment Segment) (uint64, error)
}
