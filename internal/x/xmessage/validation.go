package xmessage

import (
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// ValidationScope implements [dogma.CommandValidationScope],
// [dogma.EventValidationScope] and [dogma.DeadlineValidationScope].
type ValidationScope struct {
	IsNewMessage bool
	Envelope     *envelopepb.Envelope
}

var (
	_ dogma.CommandValidationScope  = ValidationScope{}
	_ dogma.EventValidationScope    = ValidationScope{}
	_ dogma.DeadlineValidationScope = ValidationScope{}
)

// IsNew returns true when a new message is being created message, or false when
// the engine is re-validating a message that it has already accepted.
func (s ValidationScope) IsNew() bool {
	return s.IsNewMessage
}

// ExecutedAt returns the time at which the application submitted the
// command for execution.
func (s ValidationScope) ExecutedAt() time.Time {
	return s.Envelope.GetBody().GetCreatedAt().AsTime()
}

// RecordedAt returns the time at which the event occurred.
func (s ValidationScope) RecordedAt() time.Time {
	return s.Envelope.GetBody().GetCreatedAt().AsTime()
}

// ScheduledAt returns the time at which [dogma.ProcessScope].ScheduleDeadline
// was called.
func (s ValidationScope) ScheduledAt() time.Time {
	return s.Envelope.GetBody().GetCreatedAt().AsTime()
}

// ScheduledFor returns the time at which the deadline message is to be
// delivered.
func (s ValidationScope) ScheduledFor() time.Time {
	return s.Envelope.GetBody().GetScheduledFor().AsTime()
}
