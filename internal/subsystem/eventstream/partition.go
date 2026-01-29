package eventstream

import (
	"context"
	"fmt"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream/internal/persistence"
)

// idleTimeout is the duration that a [partition] will remain idle before
// shutting down to conserve resources.
const idleTimeout = 3 * time.Minute

// partition manages the state of a single partition of the application's event
// stream.
type partition struct {
	// ID is the unique identifier of the partition.
	ID *uuidpb.UUID

	// Journals is the store used to persist event stream transactions.
	Journals journal.BinaryStore

	// Telemetry is used to record logs, metrics and traces.
	Telemetry *telemetry.Recorder

	// Shutdown is a signalling channel that, when closed, initiates a graceful
	// shutdown of the partition.
	Shutdown <-chan struct{}

	// AppendRequests is a messaging channel on which the partition receives
	// requests to append events to the event stream.
	//
	// It may be buffered, as per [Service.BufferSize].
	AppendRequests chan AppendRequest

	transactions   journal.Journal[*persistence.Transaction]
	transactionPos journal.Position
	offset         Offset
}

func (p *partition) Run(ctx context.Context) error {
	var err error
	p.transactions, err = persistence.OpenTransactionJournal(ctx, p.Journals, p.ID)
	if err != nil {
		return err
	}
	defer p.transactions.Close()

	if err := p.load(ctx, "loaded partition state"); err != nil {
		return err
	}

	for {
		ok, err := p.tick(ctx)
		if !ok || err != nil {
			return err
		}
	}
}

// load (re)loads the partition state from the journal.
func (p *partition) load(ctx context.Context, message string) error {
	ctx, span := p.Telemetry.StartSpan(
		ctx,
		"event-stream.load-partition",
		telemetry.Int("partition.offset.before", p.offset),
	)
	defer func() {
		span.SetAttributes(
			telemetry.Int("partition.offset.after", p.offset),
		)
		span.End()
	}()

	pos, txn, ok, err := journal.LastRecord(ctx, p.transactions)
	if err != nil {
		return err
	}

	transactionPosBefore := p.transactionPos
	offsetBefore := p.offset

	if ok {
		p.transactionPos = pos + 1
		p.offset = Offset(txn.MetaData.OffsetAfter)
	} else {
		p.transactionPos = 0
		p.offset = 0
	}

	p.Telemetry.Info(
		ctx,
		"event-stream.partition-loaded",
		message,
	)

	if p.transactionPos < transactionPosBefore {
		panic(fmt.Sprintf(
			"transaction journal position is %d, but partition was already at position %d",
			p.transactionPos,
			transactionPosBefore,
		))
	}

	if p.offset < offsetBefore {
		panic(fmt.Sprintf(
			"next event offset is %d, but partition was already at offset %d",
			p.offset,
			offsetBefore,
		))
	}

	return nil
}

// tick waits for the next request to arrive and processes it.
func (p *partition) tick(ctx context.Context) (bool, error) {
	timeout := time.NewTimer(idleTimeout)
	defer timeout.Stop()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-p.Shutdown:
		// We don't log this because the [service] has already done so.
		return false, nil
	case <-timeout.C:
		p.Telemetry.Info(
			ctx,
			"event-stream.partition-timed-out",
			"partition stopped due to inactivity",
			telemetry.Duration("partition.idle_timeout", idleTimeout),
		)
		return false, nil
	case req := <-p.AppendRequests:
		return true, p.handleAppendRequest(ctx, req)
	}
}

// handleAppendRequest processes the given [AppendRequest].
func (p *partition) handleAppendRequest(ctx context.Context, req AppendRequest) error {
	ctx, span := p.Telemetry.StartSpan(
		ctx,
		"event-stream.append",
		telemetry.Int("partition.offset.before", p.offset),
		telemetry.UUID("request.first_event_message_id", req.EventEnvelopes[0].MessageId),
		telemetry.Int("request.event_count", len(req.EventEnvelopes)),
		telemetry.Int("request.lowest_possible_offset", req.LowestPossibleOffset),
	)
	defer func() {
		span.SetAttributes(
			telemetry.Int("partition.offset.after", p.offset),
		)
		span.End()
	}()

	p.Telemetry.Info(
		ctx,
		"event-stream.append-request-started",
		"started processing append request",
	)

	do := func() (AppendResponse, error) {
		for {
			res, ok, err := p.deduplicate(ctx, req)
			if err != nil {
				return AppendResponse{}, err
			}

			if ok {
				p.Telemetry.Info(
					ctx,
					"event-stream.append-request-deduplicated",
					"skipped request for events that are already in the event stream",
					telemetry.Int("response.begin_offset", res.BeginOffset),
					telemetry.Int("response.end_offset", res.EndOffset),
				)

				return res, nil
			}

			res, ok, err = p.commit(ctx, req)
			if err != nil {
				return AppendResponse{}, err
			}

			if ok {
				p.Telemetry.Info(
					ctx,
					"event-stream.append-request-committed",
					"committed new events to the partition",
					telemetry.Int("response.begin_offset", res.BeginOffset),
					telemetry.Int("response.end_offset", res.EndOffset),
				)

				return res, nil
			}

			if err := p.load(ctx, "reloaded partition due to a journal conflict"); err != nil {
				return AppendResponse{}, err
			}
		}
	}

	res, err := do()

	// In the case of an error, ensure we always send a correctly correlated,
	// but otherwise empty response.
	if err != nil {
		p.Telemetry.Error(
			ctx,
			"event-stream.append-request-failed",
			"an error occurred while processing an append request",
			err,
		)

		res = AppendResponse{
			FirstEventMessageID: req.EventEnvelopes[0].MessageId,
		}
	}

	validateAppendResponse(res)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case req.Response <- res:
		return nil
	}
}

// commit writes a transaction that appends the events in the given
// [AppendRequest] to the stream partition.
func (p *partition) commit(ctx context.Context, req AppendRequest) (AppendResponse, bool, error) {
	begin := p.offset
	end := begin + Offset(len(req.EventEnvelopes))

	if err := p.transactions.Append(
		ctx,
		p.transactionPos,
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

	p.transactionPos++
	p.offset = end

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
func (p *partition) deduplicate(
	ctx context.Context,
	req AppendRequest,
) (AppendResponse, bool, error) {
	// Assuming the request is well-formed, if its "lowest possible offset" is
	// greater than the next offset, we assume our knowledge of the parititon is
	// stale and reload from the journal.
	if req.LowestPossibleOffset > p.offset {
		if err := p.load(ctx, "reloaded partition because the request's lowest possible offset implies stale in-memory state"); err != nil {
			return AppendResponse{}, false, err
		}

		// If the deduplication hint is _still_ greater than the next offset,
		// then the request is malformed.
		if req.LowestPossibleOffset > p.offset {
			panic("AppendRequest.LowestPossibleOffset is greater than the partition's next offset")
		}
	}

	// The events can't be duplicates if the only place they could be is at the
	// end of the partition.
	if req.LowestPossibleOffset == p.offset {
		return AppendResponse{}, false, nil
	}

	// Find the prior [eventstreamjournal.Transaction] for this request.
	//
	// We first find the transaction that appends the event at the deduplication
	// hint offset, then scan forward from there to find the transaction that
	// was produced this request ID.
	txn, err := journal.ScanFromSearchResult(
		ctx,
		p.transactions,
		journal.Interval{
			Begin: 0,
			End:   p.transactionPos,
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
				panic(fmt.Sprintf("AppendRequest contains different number of events to the transaction with the same first event ID (%s)", req.EventEnvelopes[0].MessageId))
			}

			for idx := range events {
				got := events[idx].MessageId
				want := req.EventEnvelopes[idx].MessageId

				if !got.Equal(want) {
					panic(fmt.Sprintf("AppendRequest contains different events to the transaction with the same first event ID (%s)", req.EventEnvelopes[0].MessageId))
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
