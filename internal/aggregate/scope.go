package aggregate

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
)

// scope implements [dogma.AggregateCommandScope].
type scope struct {
	instanceID string
	root       dogma.AggregateRoot
	packer     *envelopepb.EffectPacker
	logger     *slog.Logger
}

func (s *scope) Now() time.Time {
	return time.Now()
}

func (s *scope) Log(format string, args ...any) {
	s.logger.Info(fmt.Sprintf(format, args...))
}

func (s *scope) InstanceID() string {
	return s.instanceID
}

func (s *scope) RecordEvent(event dogma.Event) {
	s.root.ApplyEvent(event)

	eventEnvelope := s.packer.PackEvent(event)

	s.logger.Info(
		"handler recorded an event",
		xslog.Envelope("event", eventEnvelope),
	)
}
