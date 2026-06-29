package process

import "time"

type eventScope struct {
	messageScope
	recordedAt time.Time
}

func (s *eventScope) RecordedAt() time.Time {
	return s.recordedAt
}
