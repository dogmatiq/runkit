package aggregate

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
)

// commandScope implements [dogma.AggregateCommandScope].
type commandScope struct {
	instanceID string
	root       dogma.AggregateRoot
	packer     *envelopepb.EffectPacker
	logger     *slog.Logger
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
	s.root.ApplyEvent(event)

	eventEnvelope := s.packer.PackEvent(event)

	s.logger.Info(
		event.MessageDescription(),
		xslog.Envelope("event", eventEnvelope),
	)
}
