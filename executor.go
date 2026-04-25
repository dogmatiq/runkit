package runkit

import (
	"context"
	"fmt"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/x/xsync"
)

// executor is a [dogma.CommandExecutor] that delegates to an inner executor
// once the engine is running. It blocks until the engine signals readiness by
// storing a value in its future.
type executor struct {
	future xsync.Future[dogma.CommandExecutor]
}

// ExecuteCommand implements [dogma.CommandExecutor].
//
// It blocks until the engine is running or ctx is canceled.
func (ex *executor) ExecuteCommand(
	ctx context.Context,
	cmd dogma.Command,
	opts ...dogma.ExecuteCommandOption,
) error {
	inner, err := ex.future.Wait(ctx)
	if err != nil {
		return err
	}
	return inner.ExecuteCommand(ctx, cmd, opts...)
}

// commandSink handles a command that has been packed into an envelope.
type commandSink interface {
	ExecuteCommand(ctx context.Context, env *envelopepb.Envelope) error
}

// commandExecutor is a [dogma.CommandExecutor] that packs commands into
// envelopes and dispatches them to the appropriate [commandSink] based on the
// command's type.
type commandExecutor struct {
	packer *envelopepb.Packer
	routes map[string]commandSink
}

// ExecuteCommand implements [dogma.CommandExecutor].
func (c *commandExecutor) ExecuteCommand(
	ctx context.Context,
	cmd dogma.Command,
	_ ...dogma.ExecuteCommandOption,
) error {
	env := c.packer.PackCommand(cmd)

	sink, ok := c.routes[env.GetBody().GetMessage().GetTypeId().AsString()]
	if !ok {
		panic(fmt.Sprintf("runkit: no handler registered for %T commands", cmd))
	}

	return sink.ExecuteCommand(ctx, env)
}
