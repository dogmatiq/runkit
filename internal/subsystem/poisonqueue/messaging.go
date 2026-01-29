package poisonqueue

import (
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
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

func validateEnqueueRequest(req EnqueueRequest) {
	if err := req.CommandEnvelope.Validate(); err != nil {
		panic(fmt.Sprintf("EnqueueRequest.CommandEnvelope is invalid: %v", err))
	}

	if err := req.FailedHandler.Validate(); err != nil {
		panic(fmt.Sprintf("EnqueueRequest.FailedHandler is invalid: %v", err))
	}

	if req.Response == nil {
		panic("EnqueueRequest.Response is nil")
	}
}

func validateEnqueueResponse(res EnqueueResponse) {
	if err := res.CommandMessageID.Validate(); err != nil {
		panic(fmt.Sprintf("EnqueueResponse.CommandMessageID is invalid: %v", err))
	}
}
