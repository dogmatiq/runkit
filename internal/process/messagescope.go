package process

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// messageScope implements [dogma.ProcessEventScope] and
// [dogma.ProcessDeadlineScope].
type messageScope struct {
	instanceID     string
	root           dogma.ProcessRoot
	mutated        bool
	ended          bool
	commandPacker  *envelopepb.EffectPacker
	deadlinePacker *envelopepb.EffectPacker
	logger         *slog.Logger
}

func (s *messageScope) Now() time.Time {
	return time.Now()
}

func (s *messageScope) Log(format string, args ...any) {
	s.logger.Info(fmt.Sprintf(format, args...))
}

func (s *messageScope) InstanceID() string {
	return s.instanceID
}

func (s *messageScope) Mutate(fn func(dogma.ProcessRoot)) {
	if s.ended {
		panic("cannot mutate process instance after it has ended")
	}

	fn(s.root)
	s.mutated = true
}

func (s *messageScope) End() {
	s.ended = true
}

func (s *messageScope) ExecuteCommand(c dogma.Command) {
	if s.ended {
		panic("cannot execute command after process instance has ended")
	}

	s.commandPacker.PackCommand(c)
}

func (s *messageScope) ScheduleDeadline(d dogma.Deadline, t time.Time) {
	if s.ended {
		panic("cannot schedule deadline after process instance has ended")
	}

	s.deadlinePacker.PackDeadline(d, envelopepb.WithScheduledFor(t))
}
