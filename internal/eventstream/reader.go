package eventstream

import (
	"context"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/eventstream/internal/eventstreamjournal"
)

// NewReader creates a new [Reader] that reads historical events from a stream
// starting at the given offset.
func NewReader(
	ctx context.Context,
	journals journal.BinaryStore,
	streamID *uuidpb.UUID,
	offset uint64,
) (*Reader, error) {
	j, err := eventstreamjournal.Open(ctx, journals, streamID)
	if err != nil {
		return nil, err
	}

	r := &Reader{
		journal: j,
		offset:  offset,
	}

	if offset != 0 {
		bounds, err := j.Bounds(ctx)
		if err != nil {
			return nil, err
		}

		pos, rec, err := journal.Search(
			ctx,
			j,
			bounds,
			eventstreamjournal.SearchByOffset(offset),
		)
		if err != nil {
			return nil, err
		}

		r.events = rec.GetAppendEvents().Events[offset-rec.OffsetBefore:]
		r.pos = pos + 1
	}

	return r, nil
}

// Reader synchronously reads events from an event stream in order.
type Reader struct {
	journal eventstreamjournal.Journal
	pos     journal.Position
	events  []*envelopepb.Envelope
	offset  uint64
}

// Read returns the next event from the stream.
func (r *Reader) Read(ctx context.Context) (
	offset uint64,
	env *envelopepb.Envelope,
	ok bool,
	err error,
) {
	if len(r.events) == 0 {
		rec, err := r.journal.Get(ctx, r.pos)
		if err != nil {
			if journal.IsNotFound(err) {
				return 0, nil, false, nil
			}
			return 0, nil, false, err
		}

		r.pos++

		eventstreamjournal.MustSwitch_Record_Op(
			rec,
			func(op *eventstreamjournal.AppendEvents) {
				r.events = op.Events
			},
		)
	}

	if len(r.events) == 0 {
		return 0, nil, false, nil
	}

	env, r.events = r.events[0], r.events[1:]

	offset = r.offset
	r.offset++

	return offset, env, true, nil
}
