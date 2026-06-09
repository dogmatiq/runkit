package aggregate

import (
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

type scope struct {
	instanceID string
	packer     *envelopepb.EffectPacker
}

func (s *scope) Now() time.Time {
	panic("not implemented")
}

func (s *scope) Log(string, ...any) {
	panic("not implemented")
}

func (s *scope) InstanceID() string {
	return s.instanceID
}

func (s *scope) RecordEvent(ev dogma.Event) {
	s.packer.PackEvent(ev)
}
