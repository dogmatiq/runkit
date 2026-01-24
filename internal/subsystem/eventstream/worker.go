package eventstream

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream/internal/persistence"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
)

// idleTimeout is the duration that a worker will remain idle before
// shutting down to conserve resources.
const idleTimeout = 3 * time.Minute

// A worker appends events to a specific partition of the event stream.
type worker struct {
	ID          uint
	PartitionID *uuidpb.UUID
	Journals    journal.BinaryStore
	Telemetry   *telemetry.Recorder

	Shutdown       <-chan struct{}
	AppendRequests chan AppendRequest

	transactions journal.Journal[*persistence.Transaction]
	nextPos      journal.Position
	nextOffset   Offset
}

func (w *worker) Run(ctx context.Context) error {
	startedAt := time.Now()

	w.Telemetry.Info(
		ctx,
		"event-stream.worker.started",
		"event stream worker started",
	)
	defer func() {
		w.Telemetry.Info(
			ctx,
			"event-stream.worker.stopped",
			"event stream worker stopped",
			telemetry.Duration("worker.uptime", time.Since(startedAt)),
		)
	}()

	var err error
	w.transactions, err = persistence.OpenTransactionJournal(ctx, w.Journals, w.PartitionID)
	if err != nil {
		return err
	}
	defer w.transactions.Close()

	if err := w.loadState(ctx, "event stream worker loaded partition state from the journal"); err != nil {
		return err
	}

	for {
		ok, err := w.tick(ctx)
		if !ok || err != nil {
			return err
		}
	}
}

// loadState (re)loads the partition state from the journal.
func (w *worker) loadState(ctx context.Context, message string) error {
	ctx, span := w.Telemetry.StartSpan(
		ctx,
		"event-stream.worker.load-state",
		telemetry.Int("partition.next_offset.before", w.nextOffset),
	)
	defer func() {
		span.SetAttributes(
			telemetry.Int("partition.next_offset.after", w.nextOffset),
		)
		span.End()
	}()

	pos, txn, ok, err := journal.LastRecord(ctx, w.transactions)
	if err != nil {
		return err
	}

	nextPosBefore := w.nextPos
	nextOffsetBefore := w.nextOffset

	if ok {
		w.nextPos = pos + 1
		w.nextOffset = Offset(txn.MetaData.OffsetAfter)
	} else {
		w.nextPos = 0
		w.nextOffset = 0
	}

	w.Telemetry.Info(
		ctx,
		"event-stream.worker.loaded-state",
		message,
	)

	if w.nextPos < nextPosBefore {
		return xerrors.Bug(
			"next journal position is %d, but worker was already at position %d",
			w.nextPos,
			nextPosBefore,
		)
	}

	if w.nextOffset < nextOffsetBefore {
		return xerrors.Bug(
			"next offset is %d, but worker was already at offset %d",
			w.nextOffset,
			nextOffsetBefore,
		)
	}

	return nil
}

// tick waits for the next work item to arrive and processes it.
func (w *worker) tick(ctx context.Context) (bool, error) {
	idle := time.NewTimer(idleTimeout)
	defer idle.Stop()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-w.Shutdown:
		w.Telemetry.Info(
			ctx,
			"event-stream.worker.shutdown",
			"event stream worker received a request to shut down",
		)
		return false, nil
	case <-idle.C:
		w.Telemetry.Info(
			ctx,
			"event-stream.worker.timed-out",
			"event stream worker timed-out due to inactivity",
			telemetry.Duration("worker.idle_timeout", idleTimeout),
		)
		return false, nil
	case req := <-w.AppendRequests:
		return true, w.handleAppendRequest(ctx, req)
	}
}

// handleAppendRequest processes the given [AppendRequest].
func (w *worker) handleAppendRequest(ctx context.Context, req AppendRequest) error {
	ctx, span := w.Telemetry.StartSpan(
		ctx,
		"event-stream.worker.append",
		telemetry.Int("partition.next_offset.before", w.nextOffset),
		telemetry.UUID("request.first_event_message_id", req.EventEnvelopes[0].MessageId),
		telemetry.Int("request.event_count", len(req.EventEnvelopes)),
		telemetry.Int("request.lowest_possible_offset", req.LowestPossibleOffset),
	)
	defer func() {
		span.SetAttributes(
			telemetry.Int("partition.next_offset.after", w.nextOffset),
		)
		span.End()
	}()

	w.Telemetry.Info(
		ctx,
		"event-stream.worker.append-request.received",
		"event stream worker received a request to append events",
	)

	do := func() (AppendResponse, error) {
		for {
			res, ok, err := w.deduplicate(ctx, req)
			if err != nil {
				return AppendResponse{}, err
			}

			if ok {
				w.Telemetry.Info(
					ctx,
					"event-stream.worker.append-request.deduplicated",
					"event stream worker skipped append request that has already been processed",
					telemetry.Int("response.begin_offset", res.BeginOffset),
					telemetry.Int("response.end_offset", res.EndOffset),
				)

				return res, nil
			}

			res, ok, err = w.commit(ctx, req)
			if err != nil {
				return AppendResponse{}, err
			}

			if ok {
				w.Telemetry.Info(
					ctx,
					"event-stream.worker.append-request.committed",
					"event stream worker appended new events to the partition",
					telemetry.Int("response.begin_offset", res.BeginOffset),
					telemetry.Int("response.end_offset", res.EndOffset),
				)

				return res, nil
			}

			if err := w.loadState(ctx, "event stream worker reloaded partition state due to a journal conflict"); err != nil {
				return AppendResponse{}, err
			}
		}
	}

	res, err := do()

	// In the case of an error, ensure we always send a correctly correlated,
	// but otherwise empty response.
	if err != nil {
		res = AppendResponse{FirstEventMessageID: req.EventEnvelopes[0].MessageId}
	}

	select {
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	case req.Response <- res:
		return err
	}
}

// commit writes a transaction that appends the events in the given
// [AppendRequest] to the stream partition.
func (w *worker) commit(ctx context.Context, req AppendRequest) (AppendResponse, bool, error) {
	begin := w.nextOffset
	end := begin + Offset(len(req.EventEnvelopes))

	if err := w.transactions.Append(
		ctx,
		w.nextPos,
		persistence.
			NewTransactionBuilder().
			WithMetaData(&persistence.Transaction_MetaData{
				OffsetBefore: uint64(begin),
				OffsetAfter:  uint64(end),
			}).
			WithAppendOperation(&persistence.AppendOperation{
				Events: req.EventEnvelopes,
			}).
			Build(),
	); err != nil {
		if journal.IsConflict(err) {
			err = nil
		}
		return AppendResponse{}, false, err
	}

	w.nextPos++
	w.nextOffset = end

	return AppendResponse{
		req.EventEnvelopes[0].MessageId,
		true,
		begin,
		end,
	}, true, nil
}

// deduplicate searches the journal to find an existing transaction that appends
// the events from the given [AppendRequest], if any.
//
// TODO: This is a brute-force approach that searches the journal directly
// (though relatively efficiently). We could improve upon this approach by
// keeping some in-memory state of recent request and/or event IDs (either
// explicitly, or via a bloom filter, for example).
func (w *worker) deduplicate(
	ctx context.Context,
	req AppendRequest,
) (AppendResponse, bool, error) {
	// Assuming the request is well-formed, if its "lowest possible offset" is
	// greater than the next offset, we assume our knowledge of the parititon is
	// stale and reload from the journal.
	if req.LowestPossibleOffset > w.nextOffset {
		if err := w.loadState(ctx, "event stream worker reloaded partition state because the request's lowest possible offset implies stale in-memory state"); err != nil {
			return AppendResponse{}, false, err
		}

		// If the deduplication hint is _still_ greater than the next offset,
		// then the request is malformed.
		if req.LowestPossibleOffset > w.nextOffset {
			return AppendResponse{}, false, xerrors.Bug("AppendRequest.LowestPossibleOffset is greater than the partition's next offset")
		}
	}

	// The events can't be duplicates if the only place they could be is at the
	// end of the partition.
	if req.LowestPossibleOffset == w.nextOffset {
		return AppendResponse{}, false, nil
	}

	// Find the prior [eventstreamjournal.Transaction] for this request.
	//
	// We first find the transaction that appends the event at the deduplication
	// hint offset, then scan forward from there to find the transaction that
	// was produced this request ID.
	txn, err := journal.ScanFromSearchResult(
		ctx,
		w.transactions,
		journal.Interval{
			Begin: 0,
			End:   w.nextPos,
		},
		searchForOffset(req.LowestPossibleOffset),
		func(
			_ context.Context,
			_ journal.Position,
			txn *persistence.Transaction,
		) (*persistence.Transaction, bool, error) {
			events := txn.GetAppendOperation().GetEvents()
			if len(events) == 0 {
				return nil, false, nil
			}

			if !events[0].MessageId.Equal(req.EventEnvelopes[0].MessageId) {
				return txn, false, nil
			}

			if len(events) != len(req.EventEnvelopes) {
				return nil, false, xerrors.Bug("AppendRequest contains different number of events to the transaction with the same first event ID (%s)", req.EventEnvelopes[0].MessageId)
			}

			for idx := range events {
				got := events[idx].MessageId
				want := req.EventEnvelopes[idx].MessageId

				if !got.Equal(want) {
					return nil, false, xerrors.Bug("AppendRequest contains different events to the transaction with the same first event ID (%s)", req.EventEnvelopes[0].MessageId)
				}
			}

			return txn, true, nil
		},
	)

	if err != nil {
		return AppendResponse{}, false, journal.IgnoreNotFound(err)
	}

	return AppendResponse{
		req.EventEnvelopes[0].MessageId,
		true,
		Offset(txn.MetaData.OffsetBefore),
		Offset(txn.MetaData.OffsetAfter),
	}, true, nil
}
