package aggregate

import (
	"time"

	"github.com/dogmatiq/dogma"
)

type scope struct {
	instanceID string
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

func (s *scope) RecordEvent(dogma.Event) {
	panic("not implemented")
}
