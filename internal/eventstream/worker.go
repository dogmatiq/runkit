package eventstream

import (
	"context"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/eventstream/internal/eventstreamjournal"
)

// A worker is a service that appends events to a specific stream.
type worker struct {
	ID          int
	StreamID    *uuidpb.UUID
	Journals    journal.BinaryStore
	IdleTimeout time.Duration

	Shutdown       <-chan struct{}
	Terminate      <-chan struct{}
	EventsAppended chan<- EventsAppended
	AppendEvents   chan AppendEvents

	journal eventstreamjournal.Journal
	pos     journal.Position
	offset  uint64
}

func (w *worker) Run(ctx context.Context) error {
	if err := w.init(ctx); err != nil {
		return err
	}

	// The worker was started to service a request, so do that first.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case req := <-w.AppendEvents:
		if err := w.handleAppendEvents(ctx, req); err != nil {
			return err
		}
	}

	// Only after that first request has been handled do we honor the graceful
	// shutdown and idle signals.
	for {
		ok, err := w.tick(ctx)
		if !ok || err != nil {
			return err
		}
	}
}

func (w *worker) init(ctx context.Context) error {
	var err error
	w.journal, err = eventstreamjournal.Open(ctx, w.Journals, w.StreamID)
	if err != nil {
		return err
	}

	pos, rec, ok, err := journal.LastRecord(
		ctx,
		w.journal,
	)
	if !ok || err != nil {
		return err
	}

	w.pos = pos + 1
	w.offset = rec.OffsetAfter

	return nil
}

func (w *worker) tick(ctx context.Context) (bool, error) {
	idle := time.NewTimer(w.IdleTimeout)
	defer idle.Stop()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-w.Shutdown:
		return false, nil
	case <-idle.C:
		return false, nil
	case req := <-w.AppendEvents:
		return true, w.handleAppendEvents(ctx, req)
	}
}

func (w *worker) handleAppendEvents(ctx context.Context, req AppendEvents) error {
	if len(req.Events) == 0 {
		panic("zero-length append request, should have been ignored by the supervisor")
	}

	begin := w.offset
	end := begin + uint64(len(req.Events))

	if err := w.journal.Append(
		ctx,
		w.pos,
		eventstreamjournal.
			NewRecordBuilder().
			WithOffsetBefore(begin).
			WithOffsetAfter(end).
			WithAppendEvents(
				&eventstreamjournal.AppendEvents{
					Events: req.Events,
				},
			).
			Build(),
	); err != nil {
		return err
	}

	w.pos++
	w.offset = end

	return w.publish(
		ctx,
		req,
		AppendEventsReply{
			BeginOffset:  begin,
			EndOffset:    end,
			Deduplicated: false,
		},
		EventsAppended{
			StreamID: w.StreamID,
			Offset:   begin,
			Events:   req.Events,
		},
	)
}

func (w *worker) publish(
	ctx context.Context,
	req AppendEvents,
	reply AppendEventsReply,
	appended EventsAppended,
) error {
	replyChan := req.Reply
	appendChan := w.EventsAppended

	// Send to each of the channels, delivering whichever is ready first.
	// Set that channel to nil so we don't redeliver.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case replyChan <- reply:
		replyChan = nil
	case appendChan <- appended:
		appendChan = nil
	}

	// Then send to the remaining channel.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case replyChan <- reply:
		return nil
	case appendChan <- appended:
		return nil
	}
}

// // mightBeDuplicates returns true if it's possible that the events in req have
// // already been appended to the stream.
// func (w *worker) mightBeDuplicates(req AppendEvents) bool {
// 	// The events can't be duplicates if the lowest possible offset that
// 	// they could have been appended is the current end of the stream.
// 	return req.DeduplicationHint < w.offset
// }

// // findAppendRecord searches the journal to find the record that contains the
// // append operation for the given events.
// //
// // TODO: This is a brute-force approach that searches the journal directly
// // (though efficiently). We could improve upon this approach by keeping some
// // in-memory state of recent event IDs (either explicitly, or via a bloom
// // filter, for example).
// func (w *worker) findAppendRecord(
// 	ctx context.Context,
// 	req AppendEvents,
// ) (*eventstreamjournal.Record, error) {
// 	return journal.ScanFromSearchResult(
// 		ctx,
// 		w.journal,
// 		journal.Interval{
// 			Begin: 0,
// 			End:   w.pos,
// 		},
// 		eventstreamjournal.SearchByOffset(uint64(req.DeduplicationHint)),
// 		func(
// 			_ context.Context,
// 			_ journal.Position,
// 			rec *eventstreamjournal.Record,
// 		) (*eventstreamjournal.Record, bool, error) {
// 			op := rec.GetAppendEvents()
// 			if op == nil {
// 				return nil, false, nil
// 			}

// 			identical, collision := hasCollision(op.Events, req.Events)
// 			if !collision {
// 				return nil, false, nil
// 			}

// 			if identical {
// 				return rec, true, nil
// 			}

// 			return nil, false, fmt.Errorf("duplicated events from non-identical request")
// 		},
// 	)
// }

// // hasCollision determines whether there is any overlap between the two slices
// // of envelope, and if there is, whether they are identical.
// func hasCollision(lhs, rhs []*envelopepb.Envelope) (identical, collision bool) {
// 	// If either set is empty, they can't collide.
// 	if len(lhs) == 0 || len(rhs) == 0 {
// 		return len(lhs) == len(rhs), false
// 	}

// 	// If the sets have different lengths, they can't be identical, so we jump
// 	// directly to a cartesian comparison (the slow path).
// 	if len(lhs) != len(rhs) {
// 		return false, hasCollisionCartesian(lhs, rhs)
// 	}

// 	for idxL, envL := range lhs {
// 		envR := rhs[idxL]

// 		if envL.MessageId.Equal(envR.MessageId) {
// 			// Keep going so long as we have the same message IDs at the same
// 			// indices.
// 			continue
// 		}

// 		// Otherwise, we know the sets aren't identical. If we've already found
// 		// one collusion, we can return immediately.
// 		if idxL > 0 {
// 			return false, true
// 		}

// 		// Otherwise, we fall back to the cartesian comparison on the remainder
// 		// of the slices.
// 		return false, hasCollisionCartesian(lhs[idxL:], rhs[idxL:])
// 	}

// 	return true, true
// }

// // hasCollisionCartesian returns true if there is any overlap between the
// // message IDs of the two slices of envelopes.
// func hasCollisionCartesian(lhs, rhs []*envelopepb.Envelope) bool {
// 	for _, envL := range lhs {
// 		for _, envR := range rhs {
// 			if envL.MessageId.Equal(envR.MessageId) {
// 				return true
// 			}
// 		}
// 	}

// 	return false
// }
