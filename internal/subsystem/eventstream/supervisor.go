package eventstream

import (
	"context"

	"github.com/dogmatiq/enginekit/collections/maps"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/set"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xtelemetry"
)

// Supervisor accepts and delegates [AppendEventsRequest] requests.
type Supervisor struct {
	// Journals is the journal store used to persist event streams.
	Journals journal.BinaryStore

	// Sets is the set store used to persist the registry of event streams.
	Sets set.BinaryStore

	// BufferSize is the number of pending [AppendEventsRequest] values that can
	// be buffered in memory, per event stream.
	BufferSize uint

	// Telemetry is used to record logs, metrics and traces.
	Telemetry *telemetry.Provider

	// Shutdown is a channel that is closed when the supervisor should shut down
	// gracefully.
	Shutdown <-chan struct{}

	// Requests is a channel on which the supervisor receives requests to
	// append events to streams.
	Requests <-chan AppendEventsRequest

	// Notifications is a channel on which the supervisor sends notifications
	// that events have been appended to streams.
	Notifications chan<- EventsAppendedNotification

	telemetry *telemetry.Recorder

	workerID      int
	workers       maps.Proto[*uuidpb.UUID, *worker]
	workerStopped chan workerStopped
}

// Run starts the supervisor's main event loop.
func (s *Supervisor) Run(ctx context.Context) error {
	s.telemetry = s.Telemetry.Recorder(xtelemetry.ModulePath)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	defer func() {
		select {
		case <-s.Shutdown:
			// If we're shutting down, let the workers stop gracefully.
		default:
			// Otherwise, terminate them!
			cancel()
		}

		// Wait for all workers to stop.
		//
		// The workers share the same context and shutdown channel as the
		// supervisor. Therefore, if the supervisor is stopping, so too will the
		// workers.
		for s.workers.Len() > 0 {
			s.handleWorkerStopped(ctx, <-s.workerStopped)
		}
	}()

	for {
		ok, err := s.tick(ctx)
		if !ok || err != nil {
			return err
		}
	}
}

func (s *Supervisor) tick(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-s.Shutdown:
		s.telemetry.Info(
			ctx,
			"eventstream.supervisor.shutdown",
			"supervisor received a request to shut down",
			telemetry.Int("supervisor.worker_count", s.workers.Len()),
		)
		return false, nil
	case x := <-s.workerStopped:
		return true, s.handleWorkerStopped(ctx, x)
	case req := <-s.Requests:
		return true, s.handleRequest(ctx, req)
	}
}

func (s *Supervisor) handleRequest(ctx context.Context, req AppendEventsRequest) error {
	if err := validateAppendEventsRequest(req); err != nil {
		return err
	}

	s.telemetry.Info(
		ctx,
		"eventstream.supervisor.append.recv",
		"supervisor received a request to append events",
		telemetry.UUID("stream.id", req.StreamID),
	)

	for {
		w, ok := s.workers.TryGet(req.StreamID)
		if !ok {
			w = s.startWorker(
				ctx,
				req.StreamID,
				make(chan AppendEventsRequest, s.BufferSize),
			)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case x := <-s.workerStopped:
			if err := s.handleWorkerStopped(ctx, x); err != nil {
				return err
			}
		case w.Requests <- req:
			s.telemetry.Info(
				ctx,
				"eventstream.supervisor.append.enqueue",
				"supervisor added an append request to the worker queue",
				telemetry.UUID("stream.id", req.StreamID),
				telemetry.Int("worker.id", w.ID),
				telemetry.Int("worker.queue_length", len(w.Requests)),
			)
			return nil
		}
	}
}

func (s *Supervisor) handleWorkerStopped(ctx context.Context, x workerStopped) error {
	s.workers.Remove(x.Worker.StreamID)

	if x.Error == nil {
		s.telemetry.Info(
			ctx,
			"eventstream.supervisor.worker.shutdown",
			"supervisor detected a graceful worker shutdown",
			telemetry.UUID("stream.id", x.Worker.StreamID),
			telemetry.Int("worker.id", x.Worker.ID),
			telemetry.Int("worker.queue_length", len(x.Worker.Requests)),
			telemetry.Int("supervisor.worker_count", s.workers.Len()),
		)
	} else if xerrors.IsFatal(x.Error) {
		s.telemetry.Error(
			ctx,
			"eventstream.supervisor.worker.error",
			"supervisor is shutting down due to a fatal-worker error",
			x.Error,
			telemetry.UUID("stream.id", x.Worker.StreamID),
			telemetry.Int("worker.id", x.Worker.ID),
			telemetry.Int("worker.queue_length", len(x.Worker.Requests)),
			telemetry.Int("supervisor.worker_count", s.workers.Len()),
		)
		return x.Error
	} else {
		s.telemetry.Error(
			ctx,
			"eventstream.supervisor.worker.error",
			"supervisor detected a non-fatal worker failure",
			x.Error,
			telemetry.UUID("stream.id", x.Worker.StreamID),
			telemetry.Int("worker.id", x.Worker.ID),
			telemetry.Int("worker.queue_length", len(x.Worker.Requests)),
			telemetry.Int("supervisor.worker_count", s.workers.Len()),
		)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.Shutdown:
		// We're shutting down, so we don't want to restart the worker.
	default:
		// We're not shutting down, and the worker had unhandled append
		// requests, so we immediately start a new worker to take over.
		if len(x.Worker.Requests) != 0 {
			s.startWorker(
				ctx,
				x.Worker.StreamID,
				x.Worker.Requests,
			)
		}
	}

	return nil
}

func (s *Supervisor) startWorker(
	ctx context.Context,
	streamID *uuidpb.UUID,
	requests chan AppendEventsRequest,
) *worker {
	s.workerID++

	w := &worker{
		ID:            s.workerID,
		StreamID:      streamID,
		Journals:      s.Journals,
		Sets:          s.Sets,
		Shutdown:      s.Shutdown,
		Notifications: s.Notifications,
		Requests:      requests,
		Telemetry: s.Telemetry.Recorder(
			xtelemetry.ModulePath,
			telemetry.UUID("stream.id", streamID),
			telemetry.Int("worker.id", s.workerID),
		),
	}

	if s.workerStopped == nil {
		s.workerStopped = make(chan workerStopped)
	}

	go func() {
		// We can send to this channel without blocking (forever), because the
		// supervisor will wait for all workers to stop before it stops
		// receiving from this channel.
		s.workerStopped <- workerStopped{
			w,
			w.Run(ctx),
		}
	}()

	s.workers.Set(streamID, w)

	s.telemetry.Info(
		ctx,
		"eventstream.supervisor.worker.start",
		"supervisor started a new worker",
		telemetry.UUID("stream.id", w.StreamID),
		telemetry.Int("worker.id", w.ID),
		telemetry.Int("worker.queue_length", len(w.Requests)),
		telemetry.Int("supervisor.worker_count", s.workers.Len()),
	)

	return w
}

type workerStopped struct {
	Worker *worker
	Error  error
}
