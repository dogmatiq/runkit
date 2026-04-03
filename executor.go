package runkit

import (
	"context"

	"github.com/dogmatiq/dogma"
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
