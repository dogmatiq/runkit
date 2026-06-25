package integration

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
)

// commandScope implements [dogma.IntegrationCommandScope].
type commandScope struct {
	packer *envelopepb.EffectPacker
	logger *slog.Logger
}

func (s *commandScope) Now() time.Time {
	return time.Now()
}

func (s *commandScope) Log(format string, args ...any) {
	s.logger.Info(fmt.Sprintf(format, args...))
}

func (s *commandScope) RecordEvent(event dogma.Event) {
	eventEnvelope := s.packer.PackEvent(event)

	s.logger.Info(
		event.MessageDescription(),
		xslog.Envelope("event", eventEnvelope),
	)
}
