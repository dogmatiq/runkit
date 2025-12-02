package eventstream

import (
	"context"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/subsystem/eventstream/internal/eventstreamjournal"
	"github.com/dogmatiq/runkit/internal/x/ewma"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
)

const (
	// maxIdleTimeout is the maximum duration that a worker will remain idle
	// before shutting down to conserve resources.
	maxIdleTimeout = 5 * time.Minute

	// idleSmoothing is the smoothing factor (aka alpha-value) used when
	// computing the moving average of the idle time (time between requests). A
	// value closer to 0 biases the average towards historical values.
	idleSmoothing = 0.25
)

// A worker is a service that appends events to a specific stream.
type worker struct {
	ID        int
	StreamID  *uuidpb.UUID
	Journals  journal.BinaryStore
	Telemetry *telemetry.Recorder

	Shutdown      <-chan struct{}
	Requests      chan AppendEventsRequest
	Notifications chan<- EventsAppendedNotification

	journal eventstreamjournal.Journal
	pos     journal.Position
	offset  uint64

	idle struct {
		startup time.Duration // time worker started
		prev    time.Time     // time of alst request
		avg     time.Duration // average time between requests
		timeout time.Duration // current idle timeout
	}
}

func (w *worker) Run(ctx context.Context) error {
	start := time.Now()

	if err := w.load(ctx); err != nil {
		return err
	}

	w.idle.startup = time.Since(start)
	w.computeIdleTimeout()

	w.Telemetry.Info(
		ctx,
		"eventstream.worker.init",
		"worker loaded stream state from the journal",
		telemetry.Duration("worker.idle_duration_avg", w.idle.avg),
		telemetry.Duration("worker.idle_timeout", w.idle.timeout),
	)

	// Only after that first request has been handled do we honor the graceful
	// shutdown and idle signals.
	for {
		ok, err := w.tick(ctx)
		if !ok || err != nil {
			return err
		}
	}
}

// load reloads the worker's state from the journal.
func (w *worker) load(ctx context.Context) error {
	var err error
	w.journal, err = eventstreamjournal.Open(ctx, w.Journals, w.StreamID)
	if err != nil {
		return err
	}

	pos, rec, ok, err := journal.LastRecord(
		ctx,
		w.journal,
	)
	if err != nil {
		return err
	}

	if !ok {
		if w.pos != 0 {
			return xerrors.Bug(
				"journal is empty, but worker has previously seen a record at position %d",
				w.pos-1,
			)
		}

		return nil
	}

	if w.pos != 0 && pos < w.pos-1 {
		return xerrors.Bug(
			"last journal record is at position %d, but worker has previously seen a record at position %d",
			pos,
			w.pos-1,
		)
	}

	w.pos = pos + 1
	w.offset = rec.MetaData.OffsetAfter
	w.idle.avg = time.Duration(rec.MetaData.AverageIdle)

	return nil
}

func (w *worker) tick(ctx context.Context) (bool, error) {
	idle := time.NewTimer(w.idle.timeout)
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
			"eventstream.worker.timeout",
			"worker is shutting down due to inactivity",
			telemetry.Duration("worker.idle_duration_avg", w.idle.avg),
			telemetry.Duration("worker.idle_timeout", w.idle.timeout),
		)
		return false, nil
	case req := <-w.Requests:
		return true, w.handleAppendEvents(ctx, req)
	}
}

func (w *worker) handleAppendEvents(ctx context.Context, req AppendEventsRequest) error {
	w.Telemetry.Info(
		ctx,
		"eventstream.worker.append.recv",
		"worker received a request to append events",
		telemetry.Int("stream.offset", w.offset),
		telemetry.Int("request.event_count", len(req.Events)),
		telemetry.Int("request.deduplication_hint", req.DeduplicationHint),
	)

	if err := validateAppendEventsResponse(req); err != nil {
		return err
	}

	w.computeIdleTimeout()

	if ok, err := w.deduplicate(ctx, req); ok || err != nil {
		return err
	}

	return w.commit(ctx, req)
}

func (w *worker) commit(ctx context.Context, req AppendEventsRequest) error {
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
				AverageIdle:  uint64(w.idle.avg),
			}).
			WithAppendEvents(&eventstreamjournal.AppendEvents{
				Events: req.Events,
			}).
			Build(),
	); err != nil {
		return err
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
		telemetry.Duration("worker.idle_duration_avg", w.idle.avg),
		telemetry.Duration("worker.idle_timeout", w.idle.timeout),
	)

	return w.publish(
		ctx,
		req,
		AppendEventsResponse{
			BeginOffset:  begin,
			EndOffset:    end,
			Deduplicated: false,
		},
		EventsAppendedNotification{
			StreamID: w.StreamID,
			Offset:   begin,
			Events:   req.Events,
		},
	)
}

// publish sends an [AppendEventsResponse] and [EventsAppendedNotification] to
// their respective channels without blocking on either.
func (w *worker) publish(
	ctx context.Context,
	req AppendEventsRequest,
	res AppendEventsResponse,
	n EventsAppendedNotification,
) error {
	response := req.Response
	notification := w.Notifications

	// Send to each of the channels, delivering whichever is ready first. Set
	// that channel to nil so we don't redeliver.
	//
	// This approach allows us to send to whichever channel is ready first
	// without the overhead of creating separate goroutines for each.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case response <- res:
		response = nil
	case notification <- n:
		notification = nil
	}

	// Then send to the remaining channel.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case response <- res:
		return nil
	case notification <- n:
		return nil
	}
}

// computeIdleTimeout updates the worker's idle timeout based on recent
// activity. It must be called when an [AppendEventsRequest] request is received.
func (w *worker) computeIdleTimeout() {
	// Update the time of the last request.
	prev := w.idle.prev
	now := time.Now()
	w.idle.prev = now

	if !prev.IsZero() {
		// If this is _not_ the first request we have a new idle time to
		// incorporate into the average.
		idle := now.Sub(prev)
		ewma.Update(&w.idle.avg, idle, idleSmoothing)
	}

	w.idle.timeout = min(
		w.idle.startup+w.idle.avg, // minimum of startup time and always longer than the average
		maxIdleTimeout,            // never more than the maximum
	)
}

// deduplicate checks whether the events in the given [AppendEventsRequest] have
// already been appended to the stream.
//
// If they have, it sends the appropriate response to the request's response
// channel and returns true.
func (w *worker) deduplicate(ctx context.Context, req AppendEventsRequest) (bool, error) {
	rec, ok, err := w.findAppendRecord(ctx, req)
	if !ok || err != nil {
		return false, err
	}

	w.Telemetry.Info(
		ctx,
		"eventstream.worker.append.skip",
		"worker skipped append request containing duplicate events",
		telemetry.Int("stream.offset", w.offset),
		telemetry.Int("append.offset", rec.MetaData.OffsetBefore),
		telemetry.Int("append.event_count", len(req.Events)),
		telemetry.Duration("worker.idle_duration_avg", w.idle.avg),
		telemetry.Duration("worker.idle_timeout", w.idle.timeout),
	)

	select {
	case <-ctx.Done():
		return true, ctx.Err()
	case req.Response <- AppendEventsResponse{
		BeginOffset:  rec.MetaData.OffsetBefore,
		EndOffset:    rec.MetaData.OffsetAfter,
		Deduplicated: true,
	}:
		return true, nil
	}
}

// findAppendRecord searches the journal to find the record that represents the
// given [AppendEventsRequest], if any.
//
// TODO: This is a brute-force approach that searches the journal directly
// (though efficiently). We could improve upon this approach by keeping some
// in-memory state of recent event IDs (either explicitly, or via a bloom
// filter, for example).
func (w *worker) findAppendRecord(
	ctx context.Context,
	req AppendEventsRequest,
) (*eventstreamjournal.Record, bool, error) {
	// Assuming the request is well-formed, if its deduplication hint is greater
	// than the current offset, we assume our knowledge of the journal is stale
	// and reload the latest record.
	if req.DeduplicationHint > w.offset {
		if err := w.load(ctx); err != nil {
			return nil, false, err
		}

		// If the deduplication hint is still greater than the current offset,
		// then the request is malformed.
		if req.DeduplicationHint > w.offset {
			return nil, false, xerrors.Bug("AppendEventsRequest.DeduplicationHint is beyond the end of the stream")
		}
	}

	// The events can't be duplicates if the only place they could be is at the
	// *current* end of the stream.
	if req.DeduplicationHint == w.offset {
		return nil, false, nil
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

	if journal.IsNotFound(err) {
		return nil, false, nil
	}

	return rec, true, err
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

	for idxL, envL := range lhs {
		envR := rhs[idxL]

		if envL.MessageId.Equal(envR.MessageId) {
			// Keep going so long as we have the same message IDs at the same
			// indices.
			continue
		}

		// Otherwise, we know the sets aren't identical. If we've already found
		// one collusion, we can return immediately.
		if idxL > 0 {
			return false, true
		}

		// Otherwise, we fall back to the cartesian comparison on the remainder
		// of the slices.
		return false, hasCollisionCartesian(lhs[idxL:], rhs[idxL:])
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
