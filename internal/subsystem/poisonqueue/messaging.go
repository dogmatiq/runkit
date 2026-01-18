package poisonqueue

import (
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
)

// EnqueueRequest is a request to add a command to the application's poison
// queue.
type EnqueueRequest struct {
	// CommandEnvelope is the envelope containing the command to enqueue.
	CommandEnvelope *envelopepb.Envelope

	// FailedHandler is the identity of the handler that failed to process the
	// command.
	FailedHandler *identitypb.Identity

	// Response is the channel to which the corresponding
	// [EnqueueCommandResponse] is sent.
	Response chan<- EnqueueResponse
}

// EnqueueResponse is the result of an [EnqueueRequest].
type EnqueueResponse struct {
	// CommandMessageID is the unique identifier of the enqueued command.
	CommandMessageID *uuidpb.UUID

	// Ok is true if the command was enqueued successfully.
	Ok bool
}

// validateEnqueueRequest returns an error if the given request is malformed. Any
// error indicates a bug within the engine.
func validateEnqueueRequest(req EnqueueRequest) error {
	if err := req.CommandEnvelope.Validate(); err != nil {
		return xerrors.Bug("EnqueueRequest.CommandEnvelope is invalid: %w", err)
	}

	if err := req.FailedHandler.Validate(); err != nil {
		return xerrors.Bug("EnqueueRequest.FailedHandler is invalid: %w", err)
	}

	if req.Response == nil {
		return xerrors.Bug("EnqueueRequest.Response is nil")
	}

	return nil
}
