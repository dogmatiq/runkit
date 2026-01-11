package eventstream

import (
	"context"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream/internal/persistence"
)

// NewReader creates a new [Reader] that reads historical events from a stream
// partition starting at the given offset.
func NewReader(
	ctx context.Context,
	journals journal.BinaryStore,
	partitionID *uuidpb.UUID,
	offset uint64,
) (_ *Reader, err error) {
	j, err := openJournal(ctx, journals, partitionID)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			j.Close()
		}
	}()

	r := &Reader{
		journal: j,
		offset:  offset,
	}

	if offset != 0 {
		bounds, err := j.Bounds(ctx)
		if err != nil {
			return nil, err
		}

		pos, txn, err := journal.Search(
			ctx,
			j,
			bounds,
			searchForOffset(offset),
		)
		if err != nil {
			return nil, err
		}

		r.events = txn.GetAppendOperation().Events[offset-txn.MetaData.OffsetBefore:]
		r.pos = pos + 1
	}

	return r, nil
}

// Reader synchronously reads events from an event stream in order.
type Reader struct {
	journal journal.Journal[*persistence.Transaction]
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
		txn, err := r.journal.Get(ctx, r.pos)
		if err != nil {
			if journal.IsNotFound(err) {
				return 0, nil, false, nil
			}
			return 0, nil, false, err
		}

		r.pos++

		persistence.MustSwitch_Transaction_Op(
			txn,
			func(op *persistence.AppendOperation) {
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

// Close releases any resources held by the reader.
func (r *Reader) Close() error {
	return r.journal.Close()
}
