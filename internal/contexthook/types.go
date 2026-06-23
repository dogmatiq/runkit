package contexthook

import (
	_ "github.com/dogmatiq/dogma" // imported for documentation linking

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// ExecuteCommand a hook type that is invoked when a command is executed
// directly via the engine's [dogma.CommandExecutor] implementation.
//
// It is called after the command is packed into an envelope, but before the
// command is added to the queue.
type ExecuteCommand struct {
	CommandEnvelope *envelopepb.Envelope
}
