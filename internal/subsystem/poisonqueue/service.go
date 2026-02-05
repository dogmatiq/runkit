package poisonqueue

import (
	"context"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/telemetry"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/runkit/internal/subsystem/poisonqueue/internal/persistence"
	"github.com/dogmatiq/runkit/internal/x/xtelemetry"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service manages the state of the application's global poison queue.
type Service struct {
	// Keyspaces is the keyspace store used to persist queued messages.
	Keyspaces kv.BinaryStore

	// Telemetry is used to record logs, metrics and traces.
	Telemetry *telemetry.Provider

	// Stop is a signalling channel that, when closed, initiates a graceful
	// shutdown of the service.
	Stop <-chan struct{}

	// EnqueueRequests is a messaging channel on which the service receives
	// requests to add commands to the queue.
	EnqueueRequests <-chan EnqueueRequest

	telemetry *telemetry.Recorder
	messages  kv.Keyspace[*uuidpb.UUID, *persistence.QueueMessage]
}

// Run starts the service. It blocks until it the service is stopped gracefully,
// the context is cancelled, or a fatal error occurs.
func (s *Service) Run(ctx context.Context) error {
	startedAt := time.Now()
	s.telemetry = s.Telemetry.Recorder(xtelemetry.ModulePath)

	s.telemetry.Info(
		ctx,
		"poison-queue.service-started",
		"service started",
	)
	defer func() {
		s.telemetry.Info(
			ctx,
			"poison-queue.service-stopped",
			"service stopped",
			telemetry.Duration("uptime", time.Since(startedAt)),
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

// tick waits for the next request to arrive and processes it.
func (s *Service) tick(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-s.Stop:
		s.telemetry.Info(
			ctx,
			"poison-queue.stop",
			"received a request to stop gracefully",
		)
		return false, nil
	case req := <-s.EnqueueRequests:
		return true, s.handleEnqueueRequest(ctx, req)
	}
}

// handleEnqueueRequest processes the given [EnqueueRequest].
func (s *Service) handleEnqueueRequest(ctx context.Context, req EnqueueRequest) error {
	validateEnqueueRequest(req)

	ctx, span := s.telemetry.StartSpan(
		ctx,
		"poison-queue.enqueue",
		telemetry.UUID("request.command_message_id", req.CommandEnvelope.MessageId),
	)
	defer span.End()

	err := s.doEnqueue(ctx, req)

	if err != nil {
		s.telemetry.Error(
			ctx,
			"poison-queue.enqueue-request-failed",
			"an error occurred while processing an enqueue request",
			err,
		)
	}

	res := EnqueueResponse{
		CommandMessageID: req.CommandEnvelope.MessageId,
		Ok:               err == nil,
	}

	validateEnqueueResponse(res)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case req.Response <- res:
		return nil
	}
}

func (s *Service) doEnqueue(ctx context.Context, req EnqueueRequest) error {
	s.telemetry.Info(
		ctx,
		"poison-queue.enqueue-request-started",
		"started processing enqueue request",
	)

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
			return err
		}

		s.telemetry.Info(
			ctx,
			"poison-queue.enqueue-request-deduplicated",
			"skipped request for command that is already in the poison queue",
		)
	} else {
		s.telemetry.Info(
			ctx,
			"poison-queue.enqueue-request-committed",
			"committed new command to the poison queue",
		)
	}

	return nil
}
