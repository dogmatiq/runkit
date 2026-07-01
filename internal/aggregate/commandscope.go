package aggregate

import (
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
)

// commandScope implements [dogma.AggregateCommandScope].
type commandScope struct {
	instanceID           string
	root                 dogma.AggregateRoot
	packer               *envelopepb.EffectPacker
	logger               *slog.Logger
	outboundMessageTypes map[reflect.Type]struct{}
}

func (s *commandScope) Now() time.Time {
	return time.Now()
}

func (s *commandScope) Log(format string, args ...any) {
	s.logger.Info(fmt.Sprintf(format, args...))
}

func (s *commandScope) InstanceID() string {
	return s.instanceID
}

func (s *commandScope) RecordEvent(event dogma.Event) {
	if _, ok := s.outboundMessageTypes[reflect.TypeOf(event)]; !ok {
		panic(fmt.Sprintf("this handler is not configured to record events of type %T", event))
	}

	s.root.ApplyEvent(event)

	eventEnvelope := s.packer.PackEvent(event)

	s.logger.Info(
		event.MessageDescription(),
		xslog.Envelope("event", eventEnvelope),
	)
}
