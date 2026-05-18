package process

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// scope implements [dogma.ProcessEventScope] and [dogma.ProcessDeadlineScope].
//
// It tracks two flags read by persistEffects:
//
//   - mutated: any call to Mutate has occurred during this handler
//     invocation, so the worker must persist new state bytes.
//   - ended: End was called, so the worker must mark the row ended and
//     drop any remaining deadlines.
type scope struct {
	instanceID string
	root       dogma.ProcessRoot
	packer     *envelopepb.EffectPacker
	mutated    bool
	ended      bool
	time       time.Time // RecordedAt for events, ScheduledFor for deadlines
	logger     *slog.Logger
}

func (s *scope) InstanceID() string { return s.instanceID }

func (s *scope) Now() time.Time { return time.Now() }

func (s *scope) Log(format string, args ...any) {
	s.logger.Info(fmt.Sprintf(format, args...))
}

func (s *scope) RecordedAt() time.Time { return s.time }

func (s *scope) ScheduledFor() time.Time { return s.time }

func (s *scope) Mutate(fn func(dogma.ProcessRoot)) {
	if s.ended {
		panic("cannot mutate an ended process instance")
	}
	fn(s.root)
	s.mutated = true
}

func (s *scope) End() {
	s.ended = true
}

func (s *scope) ExecuteCommand(cmd dogma.Command) {
	if s.ended {
		panic("cannot execute command: process instance has ended")
	}
	s.packer.PackCommand(cmd)
}

func (s *scope) ScheduleDeadline(d dogma.Deadline, t time.Time) {
	if s.ended {
		panic("cannot schedule deadline: process instance has ended")
	}
	s.packer.PackDeadline(d, envelopepb.WithScheduledFor(t))
}
