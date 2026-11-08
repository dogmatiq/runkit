package process

import (
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/envelope"
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
	commandPacker        *envelope.EffectPacker
	deadlinePacker       *envelope.EffectPacker
	engineLogger         *slog.Logger
	appLogger            *slog.Logger
	outboundMessageTypes map[reflect.Type]struct{}
}

func (s *messageScope) Now() time.Time {
	return time.Now()
}

func (s *messageScope) Log(format string, args ...any) {
	text := fmt.Sprintf(format, args...)

	s.engineLogger.Debug(
		"handler logged a message",
		slog.String("text", text),
	)

	s.appLogger.Info(text)
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

	s.engineLogger.Debug("handler mutated the process instance")
	s.appLogger.Info("process instance mutated")
}

func (s *messageScope) End() {
	s.ended = true

	s.engineLogger.Debug("handler ended the process instance")
	s.appLogger.Info("process instance ended")
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

	s.engineLogger.Debug(
		"handler executed a command",
		xslog.Envelope("produced_message", commandEnvelope),
	)

	s.appLogger.Info(
		c.MessageDescription(),
		xslog.Envelope("produced_message", commandEnvelope),
	)
}

func (s *messageScope) ScheduleDeadline(d dogma.Deadline, t time.Time) {
	if s.ended {
		panic("cannot schedule deadline after process instance has ended")
	}

	if _, ok := s.outboundMessageTypes[reflect.TypeOf(d)]; !ok {
		panic(fmt.Sprintf("this handler is not configured to schedule deadlines of type %T", d))
	}

	deadlineEnvelope := s.deadlinePacker.PackDeadline(d, envelope.WithScheduledFor(t))

	if err := d.Validate(
		xmessage.ValidationScope{
			IsNewMessage: true,
			Envelope:     deadlineEnvelope,
		},
	); err != nil {
		panic(fmt.Sprintf("deadline of type %T is invalid: %s", d, err))
	}

	s.engineLogger.Debug(
		"handler scheduled a deadline",
		xslog.Envelope("produced_message", deadlineEnvelope),
	)

	s.appLogger.Info(
		d.MessageDescription(),
		xslog.Envelope("produced_message", deadlineEnvelope),
	)
}
