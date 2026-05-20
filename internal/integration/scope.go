package integration

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// scope implements [dogma.IntegrationCommandScope].
type scope struct {
	Packer *envelopepb.EffectPacker
	Logger *slog.Logger
}

func (s *scope) Now() time.Time {
	return time.Now()
}

func (s *scope) Log(format string, args ...any) {
	s.Logger.Info(fmt.Sprintf(format, args...))
}

func (s *scope) RecordEvent(e dogma.Event) {
	s.Packer.PackEvent(e)
}
