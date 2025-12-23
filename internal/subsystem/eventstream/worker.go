package eventstream

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream/internal/transaction"
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

	Shutdown             <-chan struct{}
	AppendEventsRequests chan AppendEventsRequest

	journal    journal.Journal[*transaction.Transaction]
	nextPos    journal.Position
	nextOffset uint64
}

func (w *worker) Run(ctx context.Context) error {
	startedAt := time.Now()

	w.Telemetry.Info(
		ctx,
		"eventstream.worker.started",
		"event stream worker started",
	)
	defer func() {
		w.Telemetry.Info(
			ctx,
			"eventstream.worker.stopped",
			"event stream worker stopped",
			telemetry.Duration("worker.uptime", time.Since(startedAt)),
		)
	}()

	var err error
	w.journal, err = openJournal(ctx, w.Journals, w.PartitionID)
	if err != nil {
		return err
	}
	defer w.journal.Close()

	if err := w.load(ctx, "event stream worker loaded partition state from the journal"); err != nil {
		return err
	}

	for {
		ok, err := w.tick(ctx)
		if !ok || err != nil {
			return err
		}
	}
}

// load (re)loads the partition state from the journal.
func (w *worker) load(ctx context.Context, message string) error {
	pos, txn, ok, err := journal.LastRecord(
		ctx,
		w.journal,
	)
	if err != nil {
		return err
	}

	nextPosBefore := w.nextPos
	nextOffsetBefore := w.nextOffset

	if ok {
		w.nextPos = pos + 1
		w.nextOffset = txn.MetaData.OffsetAfter
	} else {
		w.nextPos = 0
		w.nextOffset = 0
	}

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

	w.Telemetry.Info(
		ctx,
		"eventstream.worker.loaded-state",
		message,
		telemetry.Int("partition.next_offset", w.nextOffset),
		telemetry.Duration("worker.idle_timeout", idleTimeout),
	)

	return nil
}

func (w *worker) tick(ctx context.Context) (bool, error) {
	idle := time.NewTimer(idleTimeout)
	defer idle.Stop()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-w.Shutdown:
		w.Telemetry.Info(
			ctx,
			"eventstream.worker.shutdown",
			"event stream worker received a request to shut down",
		)
		return false, nil
	case <-idle.C:
		w.Telemetry.Info(
			ctx,
			"eventstream.worker.timed-out",
			"event stream worker timed-out due to inactivity",
			telemetry.Duration("worker.idle_timeout", idleTimeout),
		)
		return false, nil
	case req := <-w.AppendEventsRequests:
		return true, w.handleAppendEvents(ctx, req)
	}
}

func (w *worker) handleAppendEvents(ctx context.Context, req AppendEventsRequest) (err error) {
	w.Telemetry.Info(
		ctx,
		"eventstream.worker.append-request.received",
		"event stream worker received a request to append events",
		telemetry.Int("partition.next_offset", w.nextOffset),
		telemetry.Int("request.event_count", len(req.EventEnvelopes)),
		telemetry.Int("request.lowest_possible_offset", req.LowestPossibleOffset),
	)

	do := func() (AppendEventsResponse, error) {
		if err := validateAppendEventsRequest(req); err != nil {
			return AppendEventsResponse{}, err
		}

		for {
			res, ok, err := w.deduplicate(ctx, req)
			if err != nil {
				return AppendEventsResponse{}, err
			}

			if ok {
				w.Telemetry.Info(
					ctx,
					"eventstream.worker.append-request.deduplicated",
					"event stream worker skipped append request that has already been processed",
					telemetry.Int("partition.next_offset", w.nextOffset),
					telemetry.Int("request.event_count", len(req.EventEnvelopes)),
					telemetry.Int("response.begin_offset", res.BeginOffset),
					telemetry.Int("response.end_offset", res.EndOffset),
				)

				return res, nil
			}

			res, ok, err = w.commit(ctx, req)
			if err != nil {
				return AppendEventsResponse{}, err
			}

			if ok {
				w.Telemetry.Info(
					ctx,
					"eventstream.worker.append-request.committed",
					"event stream worker committed new events to the stream partition",
					telemetry.Int("partition.next_offset", w.nextOffset),
					telemetry.Int("request.event_count", len(req.EventEnvelopes)),
					telemetry.Int("response.begin_offset", res.BeginOffset),
					telemetry.Int("response.end_offset", res.EndOffset),
				)

				return res, nil
			}

			if err := w.load(ctx, "event stream worker reloaded partition state due to a journal conflict"); err != nil {
				return AppendEventsResponse{}, err
			}
		}
	}

	res, err := do()

	// In the case of an error, ensure we always send a correctly correlated,
	// but otherwise empty response.
	if err != nil {
		res = AppendEventsResponse{FirstEventMessageID: req.EventEnvelopes[0].MessageId}
	}

	select {
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	case req.Response <- res:
		return err
	}
}

// commit writes a transaction that appends the events in the given
// [AppendEventsRequest] to the stream partition.
func (w *worker) commit(ctx context.Context, req AppendEventsRequest) (AppendEventsResponse, bool, error) {
	begin := w.nextOffset
	end := begin + uint64(len(req.EventEnvelopes))

	if err := w.journal.Append(
		ctx,
		w.nextPos,
		transaction.
			NewTransactionBuilder().
			WithMetaData(&transaction.Transaction_MetaData{
				OffsetBefore: begin,
				OffsetAfter:  end,
			}).
			WithAppendEventsOperation(&transaction.AppendEventsOperation{
				Events: req.EventEnvelopes,
			}).
			Build(),
	); err != nil {
		if journal.IsConflict(err) {
			err = nil
		}
		return AppendEventsResponse{}, false, err
	}

	w.nextPos++
	w.nextOffset = end

	return AppendEventsResponse{
		req.EventEnvelopes[0].MessageId,
		true,
		begin,
		end,
	}, true, nil
}

// deduplicate searches the journal to find an existing transaction that appends
// the events from the given [AppendEventsRequest], if any.
//
// TODO: This is a brute-force approach that searches the journal directly
// (though relatively efficiently). We could improve upon this approach by
// keeping some in-memory state of recent request and/or event IDs (either
// explicitly, or via a bloom filter, for example).
func (w *worker) deduplicate(
	ctx context.Context,
	req AppendEventsRequest,
) (AppendEventsResponse, bool, error) {
	// Assuming the request is well-formed, if its "lowest possible offset" is
	// greater than the next offset, we assume our knowledge of the parititon is
	// stale and reload from the journal.
	if req.LowestPossibleOffset > w.nextOffset {
		if err := w.load(ctx, "event stream worker reloaded partition state, request's lowest possible offset suggested stale in-memory state"); err != nil {
			return AppendEventsResponse{}, false, err
		}

		// If the deduplication hint is _still_ greater than the next offset,
		// then the request is malformed.
		if req.LowestPossibleOffset > w.nextOffset {
			return AppendEventsResponse{}, false, xerrors.Bug("AppendEventsRequest.LowestPossibleOffset is beyond the partition's next offset")
		}
	}

	// The events can't be duplicates if the only place they could be is at the
	// end of the partition.
	if req.LowestPossibleOffset == w.nextOffset {
		return AppendEventsResponse{}, false, nil
	}

	// Find the prior [eventstreamjournal.Transaction] for this request.
	//
	// We first find the transaction that appends the event at the deduplication
	// hint offset, then scan forward from there to find the transaction that
	// was produced this request ID.
	txn, err := journal.ScanFromSearchResult(
		ctx,
		w.journal,
		journal.Interval{
			Begin: 0,
			End:   w.nextPos,
		},
		searchForOffset(req.LowestPossibleOffset),
		func(
			_ context.Context,
			_ journal.Position,
			txn *transaction.Transaction,
		) (*transaction.Transaction, bool, error) {
			events := txn.GetAppendEventsOperation().GetEvents()
			if len(events) == 0 {
				return nil, false, nil
			}

			if !events[0].MessageId.Equal(req.EventEnvelopes[0].MessageId) {
				return txn, false, nil
			}

			if len(events) != len(req.EventEnvelopes) {
				return nil, false, xerrors.Bug("AppendEventsRequest contains different number of events to the transaction with the same first event ID (%s)", req.EventEnvelopes[0].MessageId)
			}

			for idx := range events {
				got := events[idx].MessageId
				want := req.EventEnvelopes[idx].MessageId

				if !got.Equal(want) {
					return nil, false, xerrors.Bug("AppendEventsRequest contains different events to the transaction with the same first event ID (%s)", req.EventEnvelopes[0].MessageId)
				}
			}

			return txn, true, nil
		},
	)

	if err != nil {
		return AppendEventsResponse{}, false, journal.IgnoreNotFound(err)
	}

	return AppendEventsResponse{
		req.EventEnvelopes[0].MessageId,
		true,
		txn.MetaData.OffsetBefore,
		txn.MetaData.OffsetAfter,
	}, true, nil
}
