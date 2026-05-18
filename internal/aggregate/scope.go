package aggregate

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// scope implements [dogma.AggregateCommandScope].
type scope struct {
	AggregateInstanceID string
	Root                dogma.AggregateRoot
	Packer              *envelopepb.EffectPacker
	Logger              *slog.Logger
}

func (s *scope) InstanceID() string {
	return s.AggregateInstanceID
}

func (s *scope) Now() time.Time {
	return time.Now()
}

func (s *scope) Log(format string, args ...any) {
	s.Logger.Info(
		"application log",
		slog.String(
			"message",
			fmt.Sprintf(format, args...),
		),
	)
}

func (s *scope) RecordEvent(e dogma.Event) {
	s.Packer.PackEvent(e)
	s.Root.ApplyEvent(e)
}
