package poisonqueue

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/runkit/internal/subsystem/poisonqueue/internal/persistence"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xtelemetry"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Supervisor manages the state of the application's global poison queue.
type Supervisor struct {
	// ID is a unique identifier for this supervisor instance.
	// It is used for telemetry purposes only.
	ID *uuidpb.UUID

	// Keyspaces is the keyspace store used to persist queued messages.
	Keyspaces kv.BinaryStore

	// Telemetry is used to record logs, metrics and traces.
	Telemetry *telemetry.Provider

	// Shutdown is a channel that is closed when the supervisor should shut down
	// gracefully.
	Shutdown <-chan struct{}

	// EnqueueRequests is a channel on which the supervisor receives requests to
	// add commands to the queue.
	EnqueueRequests <-chan EnqueueRequest

	telemetry *telemetry.Recorder
	messages  kv.Keyspace[*uuidpb.UUID, *persistence.QueueMessage]
}

// Run starts the supervisor's main event loop.
func (s *Supervisor) Run(ctx context.Context) error {
	startedAt := time.Now()

	s.telemetry = s.Telemetry.Recorder(
		xtelemetry.ModulePath,
		telemetry.UUID("supervisor.id", s.ID),
	)

	s.telemetry.Info(
		ctx,
		"poison-queue.supervisor.started",
		"poison queue supervisor started",
	)
	defer func() {
		s.telemetry.Info(
			ctx,
			"poison-queue.supervisor.stopped",
			"poison queue supervisor stopped",
			telemetry.Duration("supervisor.uptime", time.Since(startedAt)),
		)
	}()

	var err error
	s.messages, err = persistence.OpenMessageKeyspace(ctx, s.Keyspaces)
	if err != nil {
		return err
	}
	defer s.messages.Close()

	for {
		ok, err := s.tick(ctx)
		if !ok || err != nil {
			return err
		}
	}
}

// tick waits for the next work item to arrive and processes it.
func (s *Supervisor) tick(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-s.Shutdown:
		s.telemetry.Info(
			ctx,
			"poison-queue.supervisor.shutdown",
			"poison queue supervisor received a request to shut down",
		)
		return false, nil
	case req := <-s.EnqueueRequests:
		return true, s.handleEnqueueRequest(ctx, req)
	}
}

// handleEnqueueRequest processes the given [EnqueueRequest].
func (s *Supervisor) handleEnqueueRequest(ctx context.Context, req EnqueueRequest) error {
	if err := validateEnqueueRequest(req); err != nil {
		return err
	}

	ctx, span := s.telemetry.StartSpan(
		ctx,
		"poison-queue.supervisor.enqueue",
		telemetry.UUID("request.command_message_id", req.CommandEnvelope.MessageId),
	)
	defer span.End()

	s.telemetry.Info(
		ctx,
		"poison-queue.supervisor.enqueue-request.received",
		"poison queue supervisor received a request to enqueue a command",
	)

	do := func() (EnqueueResponse, error) {
		if err := s.messages.Set(
			ctx,
			req.CommandEnvelope.MessageId,
			&persistence.QueueMessage{
				CommandEnvelope: req.CommandEnvelope,
				FailedHandler:   req.FailedHandler,
				EnqueuedAt:      timestamppb.Now(),
			},
			0,
		); err != nil {
			if !kv.IsConflict(err) {
				return EnqueueResponse{}, err
			}

			s.telemetry.Info(
				ctx,
				"poison-queue.supervisor.append-request.deduplicated",
				"poison queue supervisor deduplicated an enqueue request",
			)
		} else {
			s.telemetry.Info(
				ctx,
				"poison-queue.supervisor.enqueue-request.committed",
				"poison queue supervisor enqueued a new command",
			)
		}

		return EnqueueResponse{
			CommandMessageID: req.CommandEnvelope.MessageId,
			Ok:               true,
		}, nil
	}

	res, err := do()

	if xerrors.IsFatal(err) {
		s.telemetry.Error(
			ctx,
			"poison-queue.supervisor.append-request.failed",
			"poison queue supervisor detected a fatal error, supervisor is shutting down",
			err,
			telemetry.Int("pending_requests", len(s.EnqueueRequests)),
		)

		return err
	}

	// In the case of an error, ensure we always send a correctly correlated,
	// but otherwise empty response.
	if err != nil {
		s.telemetry.Error(
			ctx,
			"poison-queue.supervisor.append-request.failed",
			"poison queue supervisor detected a non-fatal error",
			err,
			telemetry.Int("pending_requests", len(s.EnqueueRequests)),
		)

		res = EnqueueResponse{CommandMessageID: req.CommandEnvelope.MessageId}
	}

	select {
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	case req.Response <- res:
		return nil
	}
}
