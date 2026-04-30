package runkit

import (
	"context"
	"errors"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// executionContext holds the state associated with a single call to
// [Engine.ExecuteCommand].
type executionContext struct {
	Envelope  *envelopepb.Envelope
	Observers []dogma.EventObserverOption
}

// commandSink handles a command that has been packed into an envelope.
type commandSink interface {
	ExecuteCommand(ctx context.Context, ec *executionContext) error
}

// errNotImplemented is returned by stub command sinks that have not yet been
// fully implemented.
var errNotImplemented = errors.New("runkit: command execution is not yet implemented")

// aggregateCommandSink is a [commandSink] that accepts commands destined for an
// aggregate message handler.
type aggregateCommandSink struct{}

func (aggregateCommandSink) ExecuteCommand(context.Context, *executionContext) error {
	return errNotImplemented
}

// integrationCommandSink is a [commandSink] that accepts commands destined for
// an integration message handler.
type integrationCommandSink struct{}

func (integrationCommandSink) ExecuteCommand(context.Context, *executionContext) error {
	return errNotImplemented
}
