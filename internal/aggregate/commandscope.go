package aggregate

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

// commandScope implements [dogma.AggregateCommandScope].
type commandScope struct {
	instanceID           string
	root                 dogma.AggregateRoot
	packer               *envelope.EffectPacker
	engineLogger         *slog.Logger
	appLogger            *slog.Logger
	outboundMessageTypes map[reflect.Type]struct{}
}

func (s *commandScope) Now() time.Time {
	return time.Now()
}

func (s *commandScope) Log(format string, args ...any) {
	text := fmt.Sprintf(format, args...)

	s.engineLogger.Debug(
		"handler logged a message",
		slog.String("text", text),
		instanceLogAttr(s.instanceID, s.root),
	)

	s.appLogger.Info(text, instanceLogAttr(s.instanceID, s.root))
}

func (s *commandScope) InstanceID() string {
	return s.instanceID
}

func (s *commandScope) RecordEvent(event dogma.Event) {
	if _, ok := s.outboundMessageTypes[reflect.TypeOf(event)]; !ok {
		panic(fmt.Sprintf("this handler is not configured to record events of type %T", event))
	}

	eventEnvelope := s.packer.PackEvent(event)

	if err := event.Validate(
		xmessage.ValidationScope{
			IsNewMessage: true,
			Envelope:     eventEnvelope,
		},
	); err != nil {
		panic(fmt.Sprintf("event of type %T is invalid: %s", event, err))
	}

	s.root.ApplyEvent(event)

	s.engineLogger.Debug(
		"handler recorded an event",
		xslog.Envelope("produced_message", eventEnvelope),
		instanceLogAttr(s.instanceID, s.root),
	)

	s.appLogger.Info(
		event.MessageDescription(),
		xslog.Envelope("produced_message", eventEnvelope),
		instanceLogAttr(s.instanceID, s.root),
	)
}
