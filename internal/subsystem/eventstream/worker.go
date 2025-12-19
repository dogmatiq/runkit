package eventstream

import (
	"context"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/set"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream/internal/eventstreamjournal"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream/internal/eventstreamregistry"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
)

const (
	// maxIdleTimeout is the maximum duration that a worker will remain idle
	// before shutting down to conserve resources.
	maxIdleTimeout = 1 * time.Minute

	// startupCost is the unit-less cost of starting a new worker. A higher value
	// causes workers to choose longer idle timeouts.
	startupCost = 5
)

// A worker is a service that appends events to a specific stream.
type worker struct {
	ID        uint
	StreamID  *uuidpb.UUID
	Journals  journal.BinaryStore
	Sets      set.BinaryStore
	Telemetry *telemetry.Recorder

	Shutdown             <-chan struct{}
	AppendEventsRequests chan AppendEventsRequest

	journal     eventstreamjournal.Journal
	pos         journal.Position
	offset      uint64
	idleTimeout time.Duration
}

func (w *worker) Run(ctx context.Context) error {
	startedAt := time.Now()

	var err error
	w.journal, err = eventstreamjournal.Open(ctx, w.Journals, w.StreamID)
	if err != nil {
		return err
	}
	defer w.journal.Close()

	if err := w.load(ctx, "worker loaded stream state from the journal"); err != nil {
		return err
	}

	if w.pos == 0 {
		// If the stream is new, we add it to the registry.
		if err := w.register(ctx); err != nil {
			return err
		}
	}

	w.idleTimeout = min(
		maxIdleTimeout,
		time.Since(startedAt)*time.Duration(startupCost),
	)

	for {
		ok, err := w.tick(ctx)
		if !ok || err != nil {
			return err
		}
	}
}

// load reloads the worker's state from the journal.
func (w *worker) load(ctx context.Context, message string) error {
	pos, rec, ok, err := journal.LastRecord(
		ctx,
		w.journal,
	)
	if err != nil {
		return err
	}

	lastKnownPos := w.pos
	lastKnownOffset := w.offset

	if ok {
		w.pos = pos + 1
		w.offset = rec.MetaData.OffsetAfter
	} else {
		w.pos = 0
		w.offset = 0
	}

	if w.pos < lastKnownPos {
		return xerrors.Bug(
			"next journal position is %d, but worker was already at position %d",
			w.pos,
			lastKnownPos,
		)
	}

	if w.offset < lastKnownOffset {
		return xerrors.Bug(
			"stream offset is %d, but worker was already at offset %d",
			w.offset,
			lastKnownOffset,
		)
	}

	w.Telemetry.Info(
		ctx,
		"eventstream.worker.load",
		message,
		telemetry.Int("stream.offset", w.offset),
		telemetry.Duration("worker.idle_timeout", w.idleTimeout),
	)

	return nil
}

// register adds the stream to the registry.
func (w *worker) register(ctx context.Context) error {
	reg, err := eventstreamregistry.Open(ctx, w.Sets)
	if err != nil {
		return err
	}
	defer reg.Close()

	if err := reg.Add(ctx, w.StreamID); err != nil {
		return err
	}

	w.Telemetry.Info(
		ctx,
		"eventstream.worker.register",
		"worker added stream to the registry",
		telemetry.Int("stream.offset", w.offset),
		telemetry.Duration("worker.idle_timeout", w.idleTimeout),
	)

	return nil
}

func (w *worker) tick(ctx context.Context) (bool, error) {
	idle := time.NewTimer(w.idleTimeout)
	defer idle.Stop()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-w.Shutdown:
		w.Telemetry.Info(
			ctx,
			"eventstream.worker.shutdown",
			"worker received a request to shut down",
		)
		return false, nil
	case <-idle.C:
		w.Telemetry.Info(
			ctx,
			"eventstream.worker.idle-timeout",
			"worker is shutting down due to inactivity",
			telemetry.Duration("worker.idle_timeout", w.idleTimeout),
		)
		return false, nil
	case req := <-w.AppendEventsRequests:
		return true, w.handleAppendEvents(ctx, req)
	}
}

func (w *worker) handleAppendEvents(ctx context.Context, req AppendEventsRequest) error {
	// If we send a reply first the sender will receive it before the close.
	// Otherwise, they will see the closed channel and know that their request
	// was not processed.
	defer close(req.Response)

	w.Telemetry.Info(
		ctx,
		"eventstream.worker.append.recv",
		"worker received a request to append events",
		telemetry.Int("stream.offset", w.offset),
		telemetry.Int("request.event_count", len(req.Events)),
		telemetry.Int("request.deduplication_hint", req.DeduplicationHint),
	)

	if err := validateAppendEventsRequest(req); err != nil {
		return err
	}

	for {
		res, ok, err := w.commit(ctx, req)
		if err != nil {
			return err
		}

		if ok {
			select {
			default:
				return xerrors.Bug("AppendEventsRequest.Response channel is unbuffered")
			case req.Response <- res:
				// response sent
			}
		}

		if err := w.load(ctx, "worker reloaded stream state due to conflict"); err != nil {
			return err
		}
	}
}

func (w *worker) commit(ctx context.Context, req AppendEventsRequest) (AppendEventsResponse, bool, error) {
	res, ok, err := w.deduplicate(ctx, req)
	if err != nil {
		return AppendEventsResponse{}, false, err
	}

	if ok {
		w.Telemetry.Info(
			ctx,
			"eventstream.worker.append.skip",
			"worker skipped append request that has already been processed",
			telemetry.Int("stream.offset", w.offset),
			telemetry.Int("append.offset", res.BeginOffset),
			telemetry.Int("append.event_count", len(req.Events)),
		)

		return res, true, nil
	}

	begin := w.offset
	end := begin + uint64(len(req.Events))

	if err := w.journal.Append(
		ctx,
		w.pos,
		eventstreamjournal.
			NewRecordBuilder().
			WithMetaData(&eventstreamjournal.Record_MetaData{
				OffsetBefore: begin,
				OffsetAfter:  end,
			}).
			WithAppendEvents(&eventstreamjournal.AppendEvents{
				Events: req.Events,
			}).
			Build(),
	); err != nil {
		if journal.IsConflict(err) {
			return AppendEventsResponse{}, false, nil
		}

		return AppendEventsResponse{}, false, err
	}

	w.pos++
	w.offset = end

	w.Telemetry.Info(
		ctx,
		"eventstream.worker.append.commit",
		"worker committed new events to the stream",
		telemetry.Int("stream.offset", w.offset),
		telemetry.Int("append.offset", begin),
		telemetry.Int("append.event_count", len(req.Events)),
	)

	return AppendEventsResponse{begin, end}, true, nil
}

// deduplicate searches the journal to find the record that represents the
// given [AppendEventsRequest], if any.
//
// TODO: This is a brute-force approach that searches the journal directly
// (though efficiently). We could improve upon this approach by keeping some
// in-memory state of recent event IDs (either explicitly, or via a bloom
// filter, for example).
func (w *worker) deduplicate(
	ctx context.Context,
	req AppendEventsRequest,
) (AppendEventsResponse, bool, error) {
	// Assuming the request is well-formed, if its deduplication hint is greater
	// than the current offset, we assume our knowledge of the journal is stale
	// and reload the latest record.
	if req.DeduplicationHint > w.offset {
		if err := w.load(ctx, "worker reloaded stream state due to future deduplication hint"); err != nil {
			return AppendEventsResponse{}, false, err
		}

		// If the deduplication hint is still greater than the current offset,
		// then the request is malformed.
		if req.DeduplicationHint > w.offset {
			return AppendEventsResponse{}, false, xerrors.Bug("AppendEventsRequest.DeduplicationHint is beyond the end of the stream")
		}
	}

	// The events can't be duplicates if the only place they could be is at the
	// *current* end of the stream.
	if req.DeduplicationHint == w.offset {
		return AppendEventsResponse{}, false, nil
	}

	// Perform a binary search of the joirnal to find the
	// [eventstreamjournal.Record] that represents a prior attempt at the same
	// [AppendEventsRequest].
	rec, err := journal.ScanFromSearchResult(
		ctx,
		w.journal,
		journal.Interval{
			Begin: 0,
			End:   w.pos,
		},
		eventstreamjournal.SearchByOffset(req.DeduplicationHint),
		func(
			_ context.Context,
			_ journal.Position,
			rec *eventstreamjournal.Record,
		) (*eventstreamjournal.Record, bool, error) {
			op := rec.GetAppendEvents()
			if op == nil {
				return nil, false, nil
			}

			identical, collision := hasCollision(op.Events, req.Events)
			if !collision {
				return nil, false, nil
			}

			if identical {
				return rec, true, nil
			}

			return nil, false, xerrors.Bug("AppendEventsRequest contains duplicate events, but does not correlate to a prior request")
		},
	)

	if err != nil {
		if journal.IsNotFound(err) {
			err = nil
		}
		return AppendEventsResponse{}, false, err
	}

	return AppendEventsResponse{rec.MetaData.OffsetBefore, rec.MetaData.OffsetAfter}, true, err
}

// hasCollision determines whether there is any overlap between the two slices
// of envelopes, and if there is, whether they are identical.
func hasCollision(lhs, rhs []*envelopepb.Envelope) (identical, collision bool) {
	// If either set is empty, they can't collide.
	if len(lhs) == 0 || len(rhs) == 0 {
		return len(lhs) == len(rhs), false
	}

	// If the sets have different lengths, they can't be identical, so we jump
	// directly to a cartesian comparison (the slow path).
	if len(lhs) != len(rhs) {
		return false, hasCollisionCartesian(lhs, rhs)
	}

	for idx, envL := range lhs {
		envR := rhs[idx]

		if envL.MessageId.Equal(envR.MessageId) {
			// Keep going so long as we have the same message IDs at the same
			// indices.
			continue
		}

		// Otherwise, we know the sets aren't identical. If we've already found
		// one collision, we can return immediately.
		if idx > 0 {
			return false, true
		}

		// Otherwise, we fall back to the cartesian comparison on the remainder
		// of the slices.
		return false, hasCollisionCartesian(lhs[idx:], rhs[idx:])
	}

	return true, true
}

// hasCollisionCartesian returns true if there is any overlap between the
// message IDs of the two slices of envelopes.
func hasCollisionCartesian(lhs, rhs []*envelopepb.Envelope) bool {
	for _, envL := range lhs {
		for _, envR := range rhs {
			if envL.MessageId.Equal(envR.MessageId) {
				return true
			}
		}
	}

	return false
}
