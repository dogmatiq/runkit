package runkit

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// commandSink handles a command that has been packed into an envelope.
type commandSink interface {
	ExecuteCommand(
		ctx context.Context,
		env *envelopepb.Envelope,
		observers []dogma.EventObserverOption,
	) error
}
