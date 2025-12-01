package eventstream

import (
	"context"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/eventstream/internal/eventstreamjournal"
	"github.com/dogmatiq/runkit/internal/ewma"
	"github.com/dogmatiq/runkit/internal/telemetry"
)

const (
	// minIdleTimeout is the minimum duration that a worker will remain idle
	// before shutting down to conserve resources.
	minIdleTimeout = 1 * time.Second

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
		prev    time.Time     // time of alst request
		avg     time.Duration // average time between requests
		timeout time.Duration // current idle timeout
	}
}

func (w *worker) Run(ctx context.Context) error {
	if err := w.init(ctx); err != nil {
		return err
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
	if err != nil {
		return err
	}

	if ok {
		w.pos = pos + 1
		w.offset = rec.MetaData.OffsetAfter
		w.idle.avg = time.Duration(rec.MetaData.AverageIdle)
	}

	w.Telemetry.Info(
		ctx,
		"eventstream.worker.init",
		"worker initialized successfully",
		telemetry.Int("stream.offset", w.offset),
		telemetry.Duration("worker.average_idle", w.idle.avg),
	)

	return nil
}

func (w *worker) tick(ctx context.Context) (bool, error) {
	var timeout <-chan time.Time
	if w.idle.timeout != 0 {
		timer := time.NewTimer(w.idle.timeout)
		defer timer.Stop()
		timeout = timer.C
	}

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-w.Shutdown:
		w.Telemetry.Info(
			ctx,
			"eventstream.worker.shutdown.signal",
			"worker received shutdown signal",
		)
		return false, nil
	case <-timeout:
		w.Telemetry.Info(
			ctx,
			"eventstream.worker.shutdown.inactivity",
			"worker shutting down due to inactivity",
			telemetry.Duration("worker.average_idle", w.idle.avg),
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
		"worker received request to append events",
		telemetry.Int("stream.offset", w.offset),
		telemetry.Int("append.events", len(req.Events)),
	)

	// If the request is malformed, we just close the response channel and
	// return. It's up to the sender to recover itself.
	if len(req.Events) == 0 {
		close(req.Response)

		w.Telemetry.Info(
			ctx,
			"eventstream.worker.append.drop",
			"worker dropped empty append request",
		)

		return nil
	}

	w.computeIdleTimeout()

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
		"worker committed events to stream",
		telemetry.Int("stream.offset", w.offset),
		telemetry.Int("append.events", len(req.Events)),
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

// computeIdleTimeout updates the worker's idle timeout based on recent
// activity. It must be called when an [AppendEventsRequest] request is received.
func (w *worker) computeIdleTimeout() {
	// Update the time of the last request.
	prev := w.idle.prev
	now := time.Now()
	w.idle.prev = now

	// If this is _not_ the first request we have a new idle time to incorporate
	// into the average.
	if !prev.IsZero() {
		idle := now.Sub(prev)

		if w.idle.avg == 0 {
			w.idle.avg = idle
		} else {
			ewma.Update(&w.idle.avg, idle, idleSmoothing)
		}
	}

	w.idle.timeout = min(
		minIdleTimeout+w.idle.avg, // always more than the expected idle time
		maxIdleTimeout,            // but don't keep mostly-idle workers alive forever
	)
}

func (w *worker) publish(
	ctx context.Context,
	req AppendEventsRequest,
	res AppendEventsResponse,
	n EventsAppendedNotification,
) error {
	responseChan := req.Response
	notificationChan := w.Notifications

	// Send to each of the channels, delivering whichever is ready first.
	// Set that channel to nil so we don't redeliver.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case responseChan <- res:
		responseChan = nil
	case notificationChan <- n:
		notificationChan = nil
	}

	// Then send to the remaining channel.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case responseChan <- res:
	case notificationChan <- n:
	}

	return nil
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
