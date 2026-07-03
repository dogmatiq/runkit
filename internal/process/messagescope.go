package process

import (
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
)

// messageScope implements [dogma.ProcessEventScope] and
// [dogma.ProcessDeadlineScope].
type messageScope struct {
	instanceID           string
	root                 dogma.ProcessRoot
	mutated              bool
	ended                bool
	commandPacker        *envelopepb.EffectPacker
	deadlinePacker       *envelopepb.EffectPacker
	logger               *slog.Logger
	outboundMessageTypes map[reflect.Type]struct{}
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

	if _, ok := s.outboundMessageTypes[reflect.TypeOf(c)]; !ok {
		panic(fmt.Sprintf("this handler is not configured to execute commands of type %T", c))
	}

	commandEnvelope := s.commandPacker.PackCommand(c)

	if err := c.Validate(
		xmessage.ValidationScope{
			IsNewMessage: true,
			Envelope:     commandEnvelope,
		},
	); err != nil {
		panic(fmt.Sprintf("command of type %T is invalid: %s", c, err))
	}

	s.logger.Info(
		c.MessageDescription(),
		xslog.Envelope("command", commandEnvelope),
	)
}

func (s *messageScope) ScheduleDeadline(d dogma.Deadline, t time.Time) {
	if s.ended {
		panic("cannot schedule deadline after process instance has ended")
	}

	if _, ok := s.outboundMessageTypes[reflect.TypeOf(d)]; !ok {
		panic(fmt.Sprintf("this handler is not configured to schedule deadlines of type %T", d))
	}

	deadlineEnvelope := s.deadlinePacker.PackDeadline(d, envelopepb.WithScheduledFor(t))

	if err := d.Validate(
		xmessage.ValidationScope{
			IsNewMessage: true,
			Envelope:     deadlineEnvelope,
		},
	); err != nil {
		panic(fmt.Sprintf("deadline of type %T is invalid: %s", d, err))
	}

	s.logger.Info(
		d.MessageDescription(),
		xslog.Envelope("deadline", deadlineEnvelope),
	)
}
