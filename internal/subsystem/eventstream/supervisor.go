package eventstream

import (
	"context"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xtelemetry"
)

// Supervisor accepts and delegates [AppendEventsRequest] requests.
type Supervisor struct {
	// ID is a unique identifier for this supervisor instance.
	// It is used for telemetry purposes only.
	ID *uuidpb.UUID

	// Journals is the journal store used to persist events.
	Journals journal.BinaryStore

	// BufferSize is the number of pending [AppendEventsRequest] values that can
	// be buffered in memory, per partition.
	BufferSize uint

	// Telemetry is used to record logs, metrics and traces.
	Telemetry *telemetry.Provider

	// Shutdown is a channel that is closed when the supervisor should shut down
	// gracefully.
	Shutdown <-chan struct{}

	// AppendEventsRequests is a channel on which the supervisor receives
	// requests to append events to the event stream.
	AppendEventsRequests <-chan AppendEventsRequest

	telemetry *telemetry.Recorder

	workerID      uint
	workers       uuidpb.Map[*worker]
	workerStopped chan workerStopped
}

// Run starts the supervisor's main event loop.
func (s *Supervisor) Run(ctx context.Context) error {
	s.telemetry = s.Telemetry.Recorder(
		xtelemetry.ModulePath,
		telemetry.UUID("supervisor.id", s.ID),
	)

	s.telemetry.Info(
		ctx,
		"eventstream.supervisor.started",
		"event stream supervisor started",
		telemetry.Int("supervisor.buffer_size", s.BufferSize),
	)
	defer func() {
		s.telemetry.Info(
			ctx,
			"eventstream.supervisor.stopped",
			"event stream supervisor stopped",
		)
	}()

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
			"event stream supervisor received a request to shut down",
			telemetry.Int("supervisor.worker_count", s.workers.Len()),
		)
		return false, nil
	case x := <-s.workerStopped:
		return true, s.handleWorkerStopped(ctx, x)
	case req := <-s.AppendEventsRequests:
		return true, s.handleRequest(ctx, req)
	}
}

func (s *Supervisor) handleRequest(ctx context.Context, req AppendEventsRequest) error {
	if err := validateAppendEventsRequest(req); err != nil {
		return err
	}

	s.telemetry.Info(
		ctx,
		"eventstream.supervisor.append-request.received",
		"event stream supervisor received a request to append events to a partition",
		telemetry.UUID("request.id", req.ID),
		telemetry.UUID("partition.id", req.PartitionID),
	)

	for {
		w, ok := s.workers.Get(req.PartitionID)
		if !ok {
			w = s.startWorker(
				ctx,
				req.PartitionID,
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
		case w.AppendEventsRequests <- req:
			return nil
		}
	}
}

func (s *Supervisor) handleWorkerStopped(ctx context.Context, x workerStopped) error {
	s.workers.Delete(x.Worker.PartitionID)

	if xerrors.IsFatal(x.Error) {
		s.telemetry.Error(
			ctx,
			"eventstream.supervisor.worker.failed",
			"event stream supervisor detected a fatal worker error, supervisor is shutting down",
			x.Error,
			telemetry.Int("supervisor.worker_count", s.workers.Len()),
			telemetry.UUID("partition.id", x.Worker.PartitionID),
			telemetry.Int("partition.pending_append_requests", len(x.Worker.AppendEventsRequests)),
			telemetry.Int("worker.id", x.Worker.ID),
		)

		return x.Error
	}

	if x.Error != nil {
		s.telemetry.Error(
			ctx,
			"eventstream.supervisor.worker.failed",
			"event stream supervisor detected a non-fatal worker error",
			x.Error,
			telemetry.Int("supervisor.worker_count", s.workers.Len()),
			telemetry.UUID("partition.id", x.Worker.PartitionID),
			telemetry.Int("partition.pending_append_requests", len(x.Worker.AppendEventsRequests)),
			telemetry.Int("worker.id", x.Worker.ID),
		)
	} else {
		s.telemetry.Info(
			ctx,
			"eventstream.supervisor.worker.shutdown",
			"event stream supervisor detected a graceful worker shutdown",
			telemetry.Int("supervisor.worker_count", s.workers.Len()),
			telemetry.UUID("partition.id", x.Worker.PartitionID),
			telemetry.Int("partition.pending_append_requests", len(x.Worker.AppendEventsRequests)),
			telemetry.Int("worker.id", x.Worker.ID),
		)
	}

	// If the partition had unhandled append requests then start another
	// worker for this partition.
	//
	// Note that we _don't_ start another worker just because the previous
	// one failed. If there are no pending requests, we rely on the sender
	// to resend requests that were outright rejected.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.Shutdown:
		// We're shutting down, so don't start a new worker.
	default:
		if len(x.Worker.AppendEventsRequests) != 0 {
			s.startWorker(
				ctx,
				x.Worker.PartitionID,
				x.Worker.AppendEventsRequests,
			)
		}
	}

	return nil
}

func (s *Supervisor) startWorker(
	ctx context.Context,
	partitionID *uuidpb.UUID,
	requests chan AppendEventsRequest,
) *worker {
	s.workerID++

	w := &worker{
		ID:                   s.workerID,
		PartitionID:          partitionID,
		Journals:             s.Journals,
		Shutdown:             s.Shutdown,
		AppendEventsRequests: requests,
		Telemetry: s.Telemetry.Recorder(
			xtelemetry.ModulePath,
			telemetry.UUID("supervisor.id", s.ID),
			telemetry.UUID("partition.id", partitionID),
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

	s.workers.Set(partitionID, w)

	s.telemetry.Info(
		ctx,
		"eventstream.supervisor.worker.started",
		"event stream supervisor started a new worker",
		telemetry.Int("supervisor.worker_count", s.workers.Len()),
		telemetry.UUID("partition.id", w.PartitionID),
		telemetry.Int("partition.pending_append_requests", len(w.AppendEventsRequests)),
		telemetry.Int("worker.id", w.ID),
	)

	return w
}

type workerStopped struct {
	Worker *worker
	Error  error
}
