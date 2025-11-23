package integration

import (
	"context"
	"fmt"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/telemetry"
)

type WorkerFactory struct {
	Identity *identitypb.Identity
	Handler  dogma.IntegrationMessageHandler
	Packer   *envelopepb.Packer
	Shutdown <-chan struct{}
}

func (f *WorkerFactory) new() *worker {
}

type workerRequest struct {
	Command *envelopepb.Envelope
}

type workerResponse struct {
	CommandID *uuidpb.UUID
	Events    []*envelopepb.Envelope
	Error     error
}

type worker struct {
	Handler   dogma.IntegrationMessageHandler
	Identity  *identitypb.Identity
	Packer    *envelopepb.Packer
	Telemetry *telemetry.Recorder

	Inbox    <-chan workerRequest
	Outbox   chan<- workerResponse
	Shutdown <-chan struct{}

	scope scope
}

func (w *worker) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.Shutdown:
			return nil
		case req := <-w.Requests:
			if err := w.do(ctx, req); err != nil {
				return err
			}
		}
	}
}

func (w *worker) do(
	ctx context.Context,
	req workerRequest,
) error {
	m, err := envelopepb.Unpack(req.Command)
	if err != nil {
		return err
	}

	c, ok := m.(dogma.Command)
	if !ok {
		return fmt.Errorf("message %s is not a command", req.Command.GetTypeId())
	}

	w.scope = scope{
		Context:   ctx,
		Identity:  w.Identity,
		Packer:    w.Packer,
		Command:   req.Command,
		Telemetry: w.Telemetry,
	}

	handlerErr := w.Handler.HandleCommand(ctx, &w.scope, c)

	res := workerResponse{
		req.Command.GetMessageId(),
		w.scope.Events,
		handlerErr,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case req.Reply <- res:
		return nil
	}
}
