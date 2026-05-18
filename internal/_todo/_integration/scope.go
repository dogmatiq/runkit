package integration

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// commandScope implements [dogma.IntegrationCommandScope]. Integration
// handlers have no instance state, so RecordEvent simply forwards to the
// effect packer with no instance id.
type commandScope struct {
	packer *envelopepb.EffectPacker
	logger *slog.Logger
}

func (s *commandScope) Now() time.Time { return time.Now() }

func (s *commandScope) Log(format string, args ...any) {
	s.logger.Info(fmt.Sprintf(format, args...))
}

func (s *commandScope) RecordEvent(e dogma.Event) {
	s.packer.PackEvent(e)
}
