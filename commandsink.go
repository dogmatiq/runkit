package runkit

import (
	"context"
	"errors"
	"fmt"

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
type aggregateCommandSink struct {
	Handler dogma.AggregateMessageHandler[dogma.AggregateRoot]
}

func (s aggregateCommandSink) ExecuteCommand(_ context.Context, ec *executionContext) error {
	// Unpack produces a fresh, independent message value from the envelope's
	// binary payload (per Dogma ADR-32, the engine must not pass a shared
	// reference to application code).
	//
	// A panic here is safe because we packed this envelope moments ago in the
	// same process; a failure indicates a broken MarshalBinary/UnmarshalBinary
	// implementation.
	m, err := envelopepb.Unpack(ec.Envelope)
	if err != nil {
		panic(fmt.Sprintf("runkit: unable to unpack command: %s", err))
	}

	instanceID := s.Handler.RouteCommandToInstance(m.(dogma.Command))
	if instanceID == "" {
		panic(fmt.Sprintf("runkit: %T.RouteCommandToInstance() returned an empty instance ID", s.Handler))
	}

	_ = instanceID
	return errNotImplemented
}

// integrationCommandSink is a [commandSink] that accepts commands destined for
// an integration message handler.
type integrationCommandSink struct{}

func (integrationCommandSink) ExecuteCommand(context.Context, *executionContext) error {
	return errNotImplemented
}
